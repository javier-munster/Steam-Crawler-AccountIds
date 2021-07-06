'use strict';
const steamCountries = require('./steamCountries');
const redisController = require('./redisController');

const AWS = require('aws-sdk');
AWS.config.update({ region: 'us-east-1' });
const docClient = new AWS.DynamoDB.DocumentClient({ apiVersion: '2012-08-10' });

async function batchWriteUsers(users) {
    // Keep track of table item number, used for randomized querying
    // let count = await redisController.getItemCount(users.length);

    // DynamoDB batch write only supports 25 inserts at a time so we must split users before insertion
    const ddbBatchWrites = [];

    for (let i = 0; i < Math.ceil(users.length / 25); i++) {
        ddbBatchWrites.push({
            RequestItems: {
                steamUsers: users.slice(i * 25, (i * 25) + 25).map((user) => {
                    let Item = {
                        steamId: user.steamid.toString(),
                        // counter: count++
                    };

                    if (!!user.personastate) {
                        Item.personaState = Number(user.personastate);
                    }

                    if (!!user.communityvisibilitystate) {
                        Item.communityVisibilityState = Number(user.communityvisibilitystate);
                    }

                    if (!!user.profilestate) {
                        Item.profileState = Number(user.profilestate);
                    }

                    if (!!user.lastlogoff) {
                        Item.lastLogoff = Number(user.lastLogoff);
                    }

                    if (!!user.primaryclanid) {
                        Item.primaryClanId = user.primaryclanid.toString();
                    }

                    if (!!user.loccountrycode) {
                        Item = {
                            ...Item,
                            ...steamCountries.getLocation(user.loccountrycode, user.locstatecode, user.loccityid)
                        };
                    }

                    if (!!user.timecreated) {
                        Item.timeCreated = Number(user.timecreated);
                    }

                    return {
                        PutRequest: {
                            Item
                        }
                    }
                })
            }
        });
    }

    console.log(JSON.stringify(ddbBatchWrites));
    await Promise.all(ddbBatchWrites.map((params) => {
        // return Promise.resolve();
        return new Promise((resolve, reject) => {
            return docClient.batchWrite(params, (err, data) => {
                if (err) {
                    console.error("Error batch writing:", err);
                    return reject(err);
                }

                console.log("batchWrite data:", data);
                return resolve(data);
            });
        });
    })).catch((err) => {
        // Rollback accountIds so that they can be reprocessed
        console.error("ddbBatchWrites error:", err);
        return redisController.decrementAccountId(users.length);
    });
}

module.exports = {
    batchWriteUsers
};