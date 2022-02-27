'use strict';
var AWS = require('aws-sdk');
AWS.config.update({ region: 'us-west-1' });

let ssm = new AWS.SSM({ apiVersion: '2014-11-06' });
var dynamodb = new AWS.DynamoDB.DocumentClient({ region: 'us-west-1' });

const data = require("./encrypted.json");

const path = require('path');
const fs = require('fs');
let _ = require('lodash');
var CryptoJS = require("crypto-js");

const args = require('minimist')(process.argv.slice(2));



function getEncryptionPassword(env) {
    var awsParams = {
        Name: `/${env}/encryption-password`, /* required */
        WithDecryption: true,
    };

    return new Promise((resolve, reject) => {
        ssm.getParameter(awsParams, function (err, data) {
            if (err) {
                console.log("::::: ERROR!!! Unable to download and decrypt Encryption Password")
                console.log(err, err.stack);
                reject(err);
            } else {
                console.log("::::: Successfully Downloaded Encryption Password")
                const encryptionPassword = data.Parameter.Value
                resolve(encryptionPassword)
            }
        });
    })
}

function getConfigurationById(id, refresh = false) {
    return new Promise((resolve, reject) => {
        console.log("::::: Retrieving Account Configuration");
        var params = {
            TableName: 'customer-configuration'
        };
        dynamodb.scan(params, function (err, data) {
            if (err) {
                console.log(err, err.stack); // an error occurred
                reject(err);
            } else {
                let accountsConfigurations = _.keyBy(data['Items'], a => a.id)

                if (id in accountsConfigurations) {
                    resolve(accountsConfigurations[id])
                } else {
                    reject({ message: `::::: Configuration not found for account id: ${id}` })
                }
            };
        });
    });
}


(async function decrypt() {

    if (!args || !args.e || !args.i) {
        console.log("::::: [ERROR] Missing arguments. Usage: node ./decrypt.js -e <env> -i <account_id>")
        return
    }

    const env = args.e;
    const accountId = args.i;

    if (env !== "staging" && env !== "production") {
        console.log("::::: [ERROR] Unkown Environment. Usage: node ./decrypt.js -e <env> -i <account_id>")
        return
    }

    if (typeof accountId !== "number") {
        console.log("::::: [ERROR] Account ID must be a number. Usage: node ./decrypt.js -e <env> -i <account_id>")
        return
    }

    try {

        let code = await getEncryptionPassword(env);
        let config = await getConfigurationById(accountId)

        function flatten(data) {
            var result = {};
            function recurse(cur, prop) {
                if (Object(cur) !== cur) {
                    result[prop] = cur;
                } else {
                    var isEmpty = true;
                    for (var p in cur) {
                        isEmpty = false;
                        recurse(cur[p], prop ? prop + "." + p : p);
                    }
                    if (isEmpty && prop)
                        result[prop] = {};
                }
            }
            recurse(data, "");
            return result;
        }

        function unflatten(data) {
            if (Object(data) !== data || Array.isArray(data))
                return data;
            var regex = /\.?([^.\[\]]+)|\[(\d+)\]/g,
                resultholder = {};
            for (var p in data) {
                var cur = resultholder,
                    prop = "",
                    m;
                while (m = regex.exec(p)) {
                    cur = cur[prop] || (cur[prop] = (m[2] ? [] : {}));
                    prop = m[2] || m[1];
                }
                cur[prop] = data[p];
            }
            return resultholder[""] || resultholder;
        };

        var obj = data;
        var tempObj = flatten(obj)

        console.log("::::: Decrypting data")

        config['pii_fields'].forEach(field => {

            for (let key in tempObj) {
                var PII = tempObj[key];

                let split = key.split(".");
                let lastKey = split[split.length - 1];
                let finalKey = String(lastKey).replace(/[^a-zA-Z0-9]/gi, "").toLowerCase()

                if (field === finalKey) {
                    tempObj[key] = CryptoJS.AES.decrypt(PII, code).toString(CryptoJS.enc.Utf8);;
                }
            }
        })

        obj = unflatten(tempObj)

        const filePath = path.join(__dirname, 'decrypted.json');

        fs.writeFileSync(filePath, JSON.stringify(obj));
        console.log("::::: Data successfuly decrypted! ", obj)
    } catch (e) {
        console.log("::::: ERROR!", e)
    }

})();