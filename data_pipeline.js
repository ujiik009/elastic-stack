const { MongoClient } = require("mongodb");
require('dotenv').config();
var fs = require("fs");
var csvwriter = require('csv-writer')
var moment = require('moment');
var createCsvWriter = csvwriter.createObjectCsvWriter
// Replace the uri string with your MongoDB deployment's connection string.



const uri = process.env.MONGODB_URL;
const client = new MongoClient(uri);
async function run() {
    try {
        var start = performance.now();
        await client.connect();
        const database = client.db('nakamotoDatabase');
        const PlayerGamePlayData = database.collection('playergameplaydatas');
        // Query for a movie that has the title 'Back to the Future'
        const gameItem = await database.collection("gameitems").find({}).toArray()


        const gamePlayData = await PlayerGamePlayData.aggregate([
            // {
            //     $match: {
            //         createdAt: {
            //             $gt: new Date("2022-03-31 12:00:00")
            //         }
            //     }
            // },
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
                    player_id: 1,
                    username: 1,
                    // avatar: 1,
                    score: 1,
                    game: "$games.name",
                    game_type: "$games.game_type",
                    used_items: 1
                }
            }
        ]);
        var data = []
        console.log(new Date(), "Start query", "[STARTING]");

        // "_id" : ObjectId("61b11238ea379415e8ba6516"),
        // "createdAt" : ISODate("2021-12-08T20:14:48.839Z"),
        // "game_id" : ObjectId("61976837dffe844091ab8e59"),
        // "player_id" : ObjectId("61b11064ea379415e8b9d0ea"),
        // "username" : "Ampers",
        // "avatar" : "assets/images/avatar/Rank5.png",
        // "score" : 150740,
        // "game" : "NAKA RUNNER"
        var header = [
            {
                id: "createdAt",
                title: "createdAt"
            },
            {
                id: "game_id",
                title: "game_id"
            },
            {
                id: "player_id",
                title: "player_id"
            },
            {
                id: "username",
                title: "username"
            },
            // {
            //     id: "avatar",
            //     title: "avatar"
            // },
            {
                id: "score",
                title: "score"
            },
            {
                id: "game",
                title: "game"
            },
            {
                id: "game_type",
                title: "game_type"
            },
            {
                id: "day_of_week",
                title: "day_of_week"
            },
            {
                id: "item_name",
                title: "item_name"
            },
            {
                id: "item_qty",
                title: "item_qty"
            },
            {
                id: "item_price_usd",
                title: "item_price_usd"
            }
        ]
        await gamePlayData.forEach((item, index) => {
            item.createdAt = moment(item.createdAt).format("YYYY-MM-DD HH:mm:ss")
            item.day_of_week = moment(item.createdAt).format("dddd")
            // game item
            var [game_item] = item.used_items
            var item_name = "unknow"
            var item_qty = 0
            var item_price_usd = 0
            if (game_item != undefined) {
                var find_item = gameItem.find(x => String(x._id) == String(game_item.item_id))
                if (find_item != undefined) {
                    item_name = find_item.name
                    item_qty = game_item.qty
                    item_price_usd = find_item.price
                }
            }
          
            item.item_name = item_name
            item.item_qty = item_qty
            item.item_price_usd = item_price_usd
            data.push(item)
        });


        const csvWriter = createCsvWriter({

            // Output csv file name is geek_data
            path: 'data_import.csv',
            header: header
        });

        console.log(new Date(), "Prepare Data", "[DONE]");

        // index game_data_transcation
        csvWriter
            .writeRecords(data)
            .then(() => {
                var end = performance.now()
                console.log(new Date(), 'Data uploaded into csv successfully', "[DONE]")
                console.log("Time Process", ((end - start) / 1000) / 60);
            });

    } finally {
        // Ensures that the client will close when you finish/error
        await client.close();
    }
}
run().catch(console.dir);