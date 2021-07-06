'use strict';
const axios = require('axios');
const steamUsers = require('./utils/steamUsers');
const redisController = require('./utils/redisController');

const STEAM_API_KEY = process.env.STEAM_API_KEY;
const END_ACCOUNT_ID = process.env.END_ACCOUNT_ID || 1224000000;
const BATCH_SIZE = 100;
const INTER_REQUEST_DELAY = Number(process.env.INTER_REQUEST_DELAY) || 5000; // 864
const REQUESTS_PER_INVOCATION = process.env.REQUESTS_PER_INVOCATION || 20;
const STEAM_REQUEST_TIMEOUT = process.env.STEAM_REQUEST_TIMEOUT || 10000;

let requestCount = 0;

function makeRequest(startAccountId, batchSize) {
    console.log(`Processing steamids from ${generateSteamId(startAccountId)} to ${generateSteamId(startAccountId + batchSize - 1)}`);
    let steamids = [];

    for (let i = 0; i < batchSize; i++) {
        steamids.push(generateSteamId(startAccountId + i));
    }

    let requestTimeout;

    return Promise.race([axios.get("https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002", {
        params: {
            key: STEAM_API_KEY,
            steamids: steamids.join(',')
        }
    }), new Promise((resolve, reject) => {
        // Axios will not timeout requests if there is no internet access, so it must be timed out manually
        requestTimeout = setTimeout(() => {
            console.warn("Steam request timeout!");
            return reject("Steam request timeout");
        }, STEAM_REQUEST_TIMEOUT);
    })]).then((res) => {
        if (!!requestTimeout) {
            clearTimeout(requestTimeout);
        }

        if (res.status !== 200) {
            console.log("Steam request failed with status:", res.status);
            redisController.disconnect();
            return Promise.reject(`Response failed with status: ${res.status}`);
        }
        // console.log("response", res.data.response.players);
        console.log("response", res.status, res.data.response.players.length);

        return steamUsers.batchWriteUsers(res.data.response.players);
    });
}

function wait(time) {
    console.log("waiting...");
    return new Promise((resolve, reject) => {
        return setTimeout(() => {
            return resolve();
        }, time)
    });
}

function processSequentially(steamid) {
    return makeRequest(steamid, BATCH_SIZE).then(() => {
        requestCount++;
        if (requestCount < REQUESTS_PER_INVOCATION) {
            return wait(INTER_REQUEST_DELAY).then(() => {
                return processSequentially(steamid + BATCH_SIZE);
            });
        } else {
            console.info("Finished daily requests:", requestCount);
            return redisController.disconnect();
        }
    });
}

/*
 * Generates a SteamID from an AccountID
 * @param accountId number
 * @returns steamid string
 */
function generateSteamId(accountId) {
    /*
     * SteamID Format:
     *   8 bit universe, 4 bit account type, 20 bit instance, 32 bit account id
     *   First 32 bits: 0x01100001
     *
     * Source: https://developer.valvesoftware.com/wiki/SteamID#Format
     */

    let buf = Buffer.from([0x01, 0x10, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00]);
    buf.writeUInt32BE(accountId, 4);
    return buf.readBigUInt64BE(0).toString();
}

exports.handler = async(event, context) => {
    const startAccountId = await redisController.getCurrentAccountId(BATCH_SIZE);

    // Check if crawler has finished
    if (startAccountId >= END_ACCOUNT_ID) {
        console.warn("Crawler has reached AccountId limit!");
        return Promise.resolve("done");
    }

    return processSequentially(startAccountId);
};