require('dotenv').config();
const axios = require('axios');
const { MongoClient } = require("mongodb");
var moment = require('moment');
const uri = process.env.MONGODB_URL;
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

        console.log(lastTimestamp);

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
                    game_id: 1,
                    createdAt: 1,
                    score: "$current_score"
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
                    // _id: 1,
                    createdAt: 1,
                    game_id: 1,
                    player_id: 1,
                    username: 1,
                    // avatar: 1,
                    score: 1,
                    game: "$games.name",
                    game_type: "$games.game_type"
                }
            }
        ]

        const gamePlayData = await PlayerGamePlayData.aggregate(mongoQuery);
        var data = []
        await gamePlayData.forEach((item, index) => {
       
            if(item._id !== undefined) delete item._id

            item.createdAt = moment(item.createdAt).format("YYYY-MM-DD HH:mm:ss")
            item.day_of_week = moment(item.createdAt).format("dddd")
            item["@timestamp"] = new Date(item.createdAt)
            if(lastTimestamp!=null){
                
                if(item["@timestamp"] > new Date(lastTimestamp)){
                    data.push(item)
                }
            }
            
        })
        console.log(new Date(), "[Get Data from Mongo]", `Number of record : ${data.length}`);
        if (data.length > 0) {
            var data_per_page = _.chunk(data, 100)

            var array_promises = data_per_page.map(page_item => {
                var data_new_line = []

                page_item.map((item) => {
                    data_new_line.push({ "index": { "_index": "game_data_transcation" } })
                    data_new_line.push(item)
                })
               return data_new_line
               .map(JSON.stringify)
               .join("\n")+"\n"
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
                .then((data) => {
                    data.map((res_item,index)=>{
                        fs.writeFileSync(`response_${index}.json`,JSON.stringify(res_item.data))
                       
                    })
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


run().catch(console.dir);