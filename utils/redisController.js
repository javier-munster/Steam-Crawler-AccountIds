'use redis';
const redis = require('redis');
const { promisify } = require('util');

const REDIS_HOST = process.env.REDIS_HOST || "127.0.0.1";
const REDIS_PORT = process.env.REDIS_PORT || 6379;

const client = redis.createClient({
    host: REDIS_HOST,
    port: REDIS_PORT
});

let redisTimeout = setTimeout(() => {
    // The Redis client will hang the process if it does not connect, so it must be timed out manually
    console.error("Redis connection timeout");
    throw new Promise("Redis never connected");
}, 3000);

client.on("connect", () => {
    console.log("redis connected");
    clearTimeout(redisTimeout);
});

client.on("end", () => {
    console.log("redis disconnected");
});

client.on("error", () => {
    console.error("redis error");
    throw new Error("redis error");
});

const incrbyAsync = promisify(client.incrby).bind(client);
const decrbyAsync = promisify(client.decrby).bind(client);
const getAsync = promisify(client.get).bind(client);

function getItemCount(value) {
    return incrbyAsync("steamIdCrawlerItemCount", value).then(counter => Number(counter || 0) - value);
}

function getCurrentAccountId() {
    return getAsync("steamIdCrawlerAccountId").then(accountId => Number(accountId || 0));
}

function incrementAccountId(amonut) {
    return incrbyAsync("steamIdCrawlerAccountId", amonut).then(accountId => Number(accountId || 0) - amonut);
}

function decrementAccountId(amonut) {
    return decrbyAsync("steamIdCrawlerAccountId", amonut);
}

function disconnect() {
    return client.end(true);
}

module.exports = {
    getItemCount,
    getCurrentAccountId,
    decrementAccountId,
    incrementAccountId,
    disconnect
};