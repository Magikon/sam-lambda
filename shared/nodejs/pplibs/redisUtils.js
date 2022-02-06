var redis = require("redis");

const { getRedshiftClient } = require("./dbUtils")
const redisPort = "6379"
const redisIP = "10.34.8.111"

loadCache = async (unique_key, client) => {
    let cacheValue = await new Promise((resolve, reject) => {
        client.get(unique_key, (err, result) => {
            if (err) reject(err)
            resolve(result)
        })
    });
    return cacheValue
}

setKeyPair = async (client, unique_key, value) => {
    //sets the new <key, value> pair in redis, no stringyfy necessary, this process takes care of that
    await new Promise((resolve, reject) => {
        client.set(unique_key, JSON.stringify(value), ((err, result) => {
            if (err) reject(err)

            resolve(result)
        }))
    });
}
setExpireDate = async (client, unique_key, hours = 24) => {
    // Sets the expiration date to 24h after the refresh of the cache
    await new Promise((resolve, reject) => {
        var date = parseInt((new Date) / 1000 + (hours * 3600))
        client.expireat(unique_key, date, ((err, result) => {
            if (err) reject(err)

            resolve(result)
        }));
    });
}
retrieveCache = async (unique_key, callback, expireAfterHours = 24) => {

    const client = redis.createClient(redisPort, redisIP)
    const key = unique_key + `_${process.env.ENV}`

    let data = await this.loadCache(key, client)

    if (data == null || typeof data === 'undefined') {
        console.log(`:::::: ${key} cache was flushed.. retrieving new data`)
        data = await callback()

        if (data) {
            await setKeyPair(client, key, data)
            await setExpireDate(client, key, expireAfterHours)
            client.quit()
            return data
        } else {
            var err = new Error(`Callback function provided for Redis Cache returned null data!`);
            console.log(err.stack)
            throw err
        }

    }

    client.quit()
    return JSON.parse(data)
}

exports.loadCache = loadCache
exports.retrieveCache = retrieveCache
exports.setKeyPair = setKeyPair
exports.setExpireDate = setExpireDate