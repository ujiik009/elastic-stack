require('dotenv').config();
const axios = require('axios');
const { MongoClient } = require("mongodb");
var moment = require('moment');
const uri = process.env.MONGODB_URL;
const urlWebHook = process.env.DISCORD_WEB_HOOK || "https://discord.com/api/webhooks/956083561646133280/cZQqbAWZDvPTUY3pXbFZK9dETX9oY14CPvC7H5pn2tyFP-9E2-EVsPwI0tfnJer_z9bC"
const cron = require('node-cron');
const client = new MongoClient(uri);
const _ = require('underscore')
var fs = require('fs');
var query = {
    "query": {
        "match_all": {}
    },
    "size": 1,
    "sort": [
        {
            "@timestamp": {
                "order": "desc"
            }
        }
    ]
}



async function run() {
    try {
        await client.connect();

        const database = client.db('nakamotoDatabase');
        const PlayerGamePlayData = database.collection('playergameplaydatas');
        const gameItem = await database.collection("gameitems").find({}).toArray()
        var response = await axios.post('http://localhost:9200/game_data_transcation/_doc/_search', query, {
            auth: {
                username: process.env.ELASTICSEARCH_USER,
                password: process.env.ELASTICSEARCH_PASSWORD
            }
        })


        var { hits: { hits } } = response.data
        var lastTimestamp = null
        if (hits.length > 0) {
            lastTimestamp = hits[0]._source["@timestamp"]
        }

        var keyword = {}

        if (lastTimestamp != null) {
            keyword = {
                createdAt: {
                    $gt: new Date(lastTimestamp)
                }
            }
        }


        var mongoQuery = [
            {
                $match: {
                    ...keyword
                }
            },
            {
                $lookup: {
                    from: "profiles",
                    localField: "player_id",
                    foreignField: "_id",
                    as: "profileDetails",
                },
            },
            { $unwind: "$profileDetails" },
            {
                $project: {
                    player_id: "$profileDetails._id",
                    username: "$profileDetails.username",
                    avatar: "$profileDetails.avatar",
                    country: "$profileDetails.country",
                    room_id: 1,
                    game_id: 1,
                    createdAt: 1,
                    score: "$current_score",
                    used_items: 1
                },
            },
            {
                $lookup: {
                    from: "games",
                    localField: "game_id",
                    foreignField: "_id",
                    as: "games",
                },
            },
            {
                $unwind: "$games",
            },
            {
                $project: {
                    _id: 1,
                    createdAt: 1,
                    game_id: 1,
                    room_id: 1,
                    player_id: 1,
                    username: 1,
                    // avatar: 1,
                    score: 1,
                    game: "$games.name",
                    game_type: "$games.game_type",
                    used_items: 1,
                    country:1 ,
                }
            },
            {
             $lookup: {
                 from: 'gamerooms',
                 localField: "room_id",
                 foreignField: "_id",
                 as: 'gamerooms'
             },
         },
         {
             $unwind: "$gamerooms",
         },
         {
             $project: {
                 _id: 1,
                 createdAt: 1,
                 game_id: 1,
                 player_id: 1,
                 username: 1,
                 // avatar: 1,
                 score: 1,
                 game: 1,
                 game_type: 1,
                 used_items: 1,
                 country:1 ,
                 gamerooms :"$gamerooms"
             },
         },
        ]

        const gamePlayData = await PlayerGamePlayData.aggregate(mongoQuery);
        var data_record = []
        await gamePlayData.forEach((item, index) => {

            if (item._id !== undefined) delete item._id

            item.createdAt = moment(item.createdAt).format("YYYY-MM-DD HH:mm:ss")
            item.day_of_week = moment(item.createdAt).format("dddd")
            var [game_item] = item.used_items
            var item_name = "unknow"
            var item_qty = 0
            var item_price_usd = 0
            var country = item.country
            if (game_item != undefined) {
                var find_item = gameItem.find(x => String(x._id) == String(game_item.item_id))
                if (find_item != undefined) {
                    item_name = find_item.name+' '+find_item.item_size
                    item_qty = game_item.qty
                    item_price_usd = find_item.price
                }
            }
            if (country == undefined) {
                country = "unknow"
            }
            item.item_name = item_name
            item.item_qty = item_qty
            item.item_price_usd = item_price_usd
            item["@timestamp"] = new Date(item.createdAt)
            item.cost_price = item_qty*item_price_usd
            item.country = country
            if(item.gamerooms.history_user_play != undefined) {
                var player = item.gamerooms.history_user_play.find(x => String(x.player_id) == String(item.player_id))

                if (player){
                    item.game_start = moment(player.timestamp).format("YYYY-MM-DD HH:mm:ss")
                }else {
                    item.game_start = item.createdAt
                }
                
            } else {
                item.game_start = item.createdAt
            }
            item.game_end = item.createdAt

            var game_start = moment(item.game_start)
            var game_end = moment(item.game_end)
            item.time_play = Number(game_end.diff(game_start,"minute"))
            if (lastTimestamp != null) {

                if (item["@timestamp"] > new Date(lastTimestamp)) {
                    data_record.push(item)
                }
            }

        })
        console.log(new Date(), "[Get Data from Mongo]", `Number of record : ${data_record.length}`);
        if (data_record.length > 0) {
            var data_per_page = _.chunk(data_record, 100)

            var array_promises = data_per_page.map(page_item => {
                var data_new_line = []

                page_item.map((item) => {
                    data_new_line.push({ "index": { "_index": "game_data_transcation" } })
                    data_new_line.push(item)
                })
                return data_new_line
                    .map(JSON.stringify)
                    .join("\n") + "\n"
            }).map((data_body) => {

                return axios.post('http://localhost:9200/_bulk', data_body, {
                    auth: {
                        username: process.env.ELASTICSEARCH_USER,
                        password: process.env.ELASTICSEARCH_PASSWORD
                    },
                    headers: { "Content-Type": "application/x-ndjson" }
                })
            })
            console.log(new Date(), "[Import to Elasticsearch]", `Starting import`);
            Promise.all(array_promises)
                .then((data_output) => {
                    var response_data = data_output.map((x) => {
                        return x.data
                    })
                    var error = data_output.some((result) => {
                        return result.data.errors == true
                    })
                    if (error == true) {
                        fs.writeFileSync(`response.json`, JSON.stringify(response_data))
                        sendMessage("Import to Elasticsearch", "Error cannot import ", false)
                    } else {
                        sendMessage("Import to Elasticsearch", `Import Successfully [${data_record.length} Record]`, true)
                    }
                    console.log(new Date(), "[Import to Elasticsearch]", `Starting import`, "[SUCCESS]");
                })
                .catch((error) => {
                    console.log(error);
                    console.log(new Date(), "[Import to Elasticsearch]", `Import fail`, "[FAIL]");
                })
                .finally(() => {

                    console.log(new Date(), "[Import to Elasticsearch]", `Process finish`);
                    return true
                })
        } else {
            console.log(new Date(), "[Get Data from Mongo]", `Up to Date`);
            return true
        }
    } catch (error) {
        throw error
    }
    finally {
        // Ensures that the client will close when you finish/error
        await client.close();
    }
}

function sendMessage(topic = "TEST", message, status = true, username = "NAKA-BOT") {
    var formatMessage = `
\`\`\`
${moment().format("YYYY-MM-DD HH:mm:ss")} : ${(status == true) ? "✅" : "❌"} [${topic}]  ${message} 
\`\`\``
    axios.post(urlWebHook, JSON.stringify({
        username: username,
        content: formatMessage
    }), {
        headers: {
            "Content-Type": "application/json"
        }
    })
}


cron.schedule('*/5 * * * *', function () {
    run().catch(console.dir);
});

