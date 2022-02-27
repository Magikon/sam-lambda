const AWS = require('aws-sdk');
const Pool = require('pg-pool');

AWS.config.update({ region: 'us-west-1' });

const ssm = new AWS.SSM({ apiVersion: '2014-11-06' });

let dbPassword;
let pool;

function getDBPassword(env, refresh = false) {
    return new Promise((resolve, reject) => {
        if (!dbPassword || refresh) {
            var params = {
                Name: `${env}-database-password`,
                WithDecryption: true
            };

            ssm.getParameter(params, function (err, data) {
                if (err) {
                    reject(err);
                } else {
                    dbPassword = data.Parameter.Value;
                    resolve(dbPassword);
                }
            });
        } else {
            resolve(dbPassword);
        }
    });
}

function getDBPool(env, refresh = false) {
    return new Promise((resolve, reject) => {
        if (!pool) {
            const {
                ENV: env,
                DB_ENDPOINT: host,
                DB_USER: user,
                DB_NAME: database
            } = process.env;

            getDBPassword(env, refresh)
                .then(password => {
                    pool = new Pool({
                        host,
                        database,
                        user,
                        password,
                        port: 5432,
                        max: 1,
                        min: 0,
                        idleTimeoutMillis: 300000,
                        connectionTimeoutMillis: 1000
                    });
                    resolve(pool);
                })
                .catch(err => reject(err));
        } else {
            resolve(pool);
        }
    });
}

exports.getDBPassword = getDBPassword;
exports.getDBPool = getDBPool;
