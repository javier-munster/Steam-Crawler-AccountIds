'use strict';
const axios = require('axios');
const AWS = require('aws-sdk');

const steamCountries = require('./utils/steamCountries');

const STEAM_API_KEYS = process.env.STEAM_API_KEYS.split(','); // This will be moved to AWS' SSM Parameter Store in a future update
const END_ACCOUNT_ID = Number(process.env.END_ACCOUNT_ID) || 1224000000; // Stops crawling at this accountId
const BATCH_SIZE = 100; // Number of steamIds to include in each request. Set by Steam API
const STEAM_REQUEST_TIMEOUT = Number(process.env.STEAM_REQUEST_TIMEOUT) || 10000;
const MAX_REQUESTS_PER_KEY = Number(process.env.MAX_REQUESTS_PER_DAY) || 2;
const LAUNCH_ON_START = process.env.LAUNCH_ON_START == "true" ? true : false; // Allows for re-deployment without relaunching
let STEAM_API_KEY;

const DocClient = new AWS.DynamoDB.DocumentClient({
    region: "us-east-1",
    apiVersion: "2012-08-10"
});

let requestCount = 0; // Keeps track of the number of requests made, used to evaluate when invocation limit is reached

/**
 * Requests for 100 steamIds from Steam's ISteamUser GetPlayerSummaries API
 * @param  {number} startAccountId
 * @param  {number} batchSize
 * @returns {Promise}
 */
async function getSteamIds(startAccountId, batchSize) {
    /** Generate steamIds from accountIds */
    let steamIds = [];

    for (let i = 0; i < batchSize; i++) {
        steamIds.push(generateSteamId(startAccountId + i));
    }

    console.log(`Processing steamIds from ${steamIds[0]} to ${steamIds[steamIds.length - 1]}`);

    /** Steam API Request */
    const steamResponse = await racePromise(axios.get("https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002", {
        params: {
            key: STEAM_API_KEY,
            steamids: steamIds.join(',')
        }
    }), STEAM_REQUEST_TIMEOUT, "getSteamIds");

    if (steamResponse.status !== 200) {
        console.log("Steam request failed with status:", steamResponse.status);
        return Promise.reject(`Response failed with status: ${steamResponse.status}`);
    }

    const users = steamResponse.data.response.players;
    console.log("Steam API Response:", users.length);

    let localAccountId = await getParameter("localAccountId");

    /** Map Steam response into DynamoDB request */
    // DynamoDB batch write supports a max of 25 operations, so they must be batched
    const ddbBatchWrites = [];

    for (let i = 0; i < Math.ceil(users.length / 25); i++) {
        ddbBatchWrites.push({
            RequestItems: {
                steamUsers: users.slice(i * 25, (i * 25) + 25).map((user) => {
                    let Item = {
                        steamId: user.steamid.toString(),
                        localAccountId: localAccountId++
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

                    Item.lastModified = Math.floor(Date.now() / 1000);

                    return {
                        PutRequest: {
                            Item
                        }
                    }
                })
            }
        });
    }

    /** Store SteamIds into DynamoDB */
    const ret = await Promise.all(ddbBatchWrites.map((params) => {
        return new Promise((resolve, reject) => {
            return DocClient.batchWrite(params, (err, data) => {
                if (err) {
                    console.error("Error batch writing:", err);
                    return reject(err);
                }

                if (data && Object.keys(data.UnprocessedItems).length !== 0) {
                    console.log("batchWrite data:", data);
                }

                return resolve(data);
            });
        })
    }));

    await setParameter("localAccountId", localAccountId);
    return ret;
}

/**
 * Generates a SteamID from an AccountID
 * @param  {number} accountId - 32 but unsigned integer accountId
 * @returns {string} steamid - 64 bit unsigned integer converted to string
 */
function generateSteamId(accountId) {
    /*
     * steamId Format:      64 bit unsigned integer converted to string
     * steamID Composition: [ 0x01100001, accountId ]
     * Explanation:         [ 8 bit universe, 4 bit account type, 20 bit instance, 32 bit accountId ]
     *
     * Source: https://developer.valvesoftware.com/wiki/SteamID#Format
     */

    let buf = Buffer.from([0x01, 0x10, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00]);
    buf.writeUInt32BE(accountId, 4);
    return buf.readBigUInt64BE(0).toString();
}

/**
 * Races a promise with a timeout. Several operations can hang on failure and this prevents the entire Lambda function from hanging
 * @param  {Promise} promise - Promise to race
 * @param  {number} timeout - Milliseconds to wait before timing out
 * @param  {string} name="racePromise" - Used for logging the operation that failed
 * @returns {promise}
 */
function racePromise(promise, timeout, name = "racePromise") {
    let opTimeout;

    return Promise.race([
        promise.finally(() => {
            if (!!opTimeout) {
                clearTimeout(opTimeout)
            }
        }),
        new Promise((resolve, reject) => {
            opTimeout = setTimeout(() => {
                console.warn(`${name} timeout!`);
                return reject(`${name} timeout!`);
            }, timeout);
        })
    ])
}

/**
 * Retrieves the crawler state from DynamoDB
 * @param  {string} parameterName
 * @returns {number}
 */
function getParameter(parameterName) {
    return racePromise(new Promise((resolve, reject) => {
        return DocClient.get({
            TableName: "parameters",
            Key: {
                parameterName
            },
            ProjectionExpression: "parameterValue"
        }, (err, data) => {
            if (err) {
                console.error("getParameter", parameterName, err);
                return reject(err);
            }

            console.log("getParameter success", parameterName, data.Item.parameterValue);
            return resolve(data.Item.parameterValue);
        });
    }), 5000, `getParameter ${parameterName}`);
}

/**
 * Saves the state of the Lambda function for the next invokation
 * @param  {string} parameterName
 * @param  {number} parameterValue
 * @returns {number}
 */
function setParameter(parameterName, parameterValue) {
    return racePromise(new Promise((resolve, reject) => {
        return DocClient.put({
            TableName: "parameters",
            Item: {
                parameterName,
                parameterValue
            }
        }, (err, data) => {
            if (!!err) {
                console.error("setParameter error:", err);
                return reject(err);
            }

            console.log("setParameter success", parameterName, data);
            return resolve(data);
        });
    }), 5000, `setParameter ${parameterName}`);
}

/**
 * Recursively requests steamIds from Steam API until limit is reached, then saves the current state in the DB
 * @param  {number} currentAccountId
 * @returns {Promise}
 */
async function crawlSteamIds(currentAccountId) {
    await getSteamIds(currentAccountId, BATCH_SIZE);
    await setParameter("currentAccountId", currentAccountId + BATCH_SIZE);

    if (++requestCount < MAX_REQUESTS_PER_KEY) {
        return crawlSteamIds(currentAccountId + BATCH_SIZE);
    }
}

/**
 * Swaps between our Steam API keys
 */
function swapApiKey() {
    STEAM_API_KEY = STEAM_API_KEY === STEAM_API_KEYS[0] ? STEAM_API_KEYS[1] : STEAM_API_KEYS[0];
}

/**
 *
 * @returns {Number} Steam API Key index currently in use
 */
function identifyApiKey() {
    return STEAM_API_KEYS.indexOf(STEAM_API_KEY);
}

/**
 * Resets the crawler for a new start, swaps API keys
 */
function reset() {
    requestCount = 0;
    swapApiKey();
}

async function startCrawler() {
    const startAccountId = await getParameter("currentAccountId");
    console.log(`Launching SteamIdCrawler API Key ${identifyApiKey()}: ${startAccountId}`);

    if (startAccountId >= END_ACCOUNT_ID) {
        console.warn("Crawler has reached AccountId limit!");
        return Promise.reject("done");
    }

    // Run crawler
    await crawlSteamIds(startAccountId);

    console.log("Finished at currentAccountId:", startAccountId + (requestCount * BATCH_SIZE));
}


function launcher() {
    reset();
    console.log(`[${Date.now()}] Launching SteamIdCrawler with API Key ${identifyApiKey()}`);
    return startCrawler().catch((err) => {
        console.error(`SteamIdCrawler API Key ${identifyApiKey()} Error:`, err);
    }).finally(() => {
        console.log(`[${Date.now()}] Finished SteamIdCrawler Daily Execution for API Key ${identifyApiKey()}, swapping keys`);
    }).then(() => {
        reset();
        const delay = Math.floor(Math.random() /* * 60 */ * 60 * 1000);
        console.log(`Delaying for ${delay} ms`)
        return new Promise((resolve, reject) => {
            return setTimeout(() => {
                console.log("Delay done");
                return resolve();
            }, delay);
        });
    }).then(startCrawler).catch((err) => {
        console.error(`SteamIdCrawler API Key ${identifyApiKey()} Error:`, err);
    }).finally(() => {
        console.log(`[${Date.now()}] Finished SteamIdCrawler Daily Execution for API Key ${identifyApiKey()}`);
    });
}
console.log("here0", STEAM_API_KEYS);
if (LAUNCH_ON_START) {
    launcher();
    setInterval(() => {
        return launcher();
    }, 24 * 60 * 60 * 1000);
}