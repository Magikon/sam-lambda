const AWS = require('aws-sdk');
AWS.config.update({ region: 'us-west-1' });

var CryptoJS = require("crypto-js");

var accountsConfigurations;
var accountConfig;
var encryptionPassword;
var globalLeadId;

const ssm = new AWS.SSM({ apiVersion: '2014-11-06' });
var dynamodb = new AWS.DynamoDB.DocumentClient({ region: 'us-west-1' });
let _ = require('lodash');

exports.getConfigurationById = (id, refresh = false, ) => {
    return new Promise((resolve, reject) => {

        // if account configuration does not exist or a refresh as been invoked. 
        // Retrieve the latest configurations for all accounts from dynamoDB
        // and return the account configuration specified by id,
        // else, return the pre existing cached configuration.

        if (accountsConfigurations && !refresh) {
            console.log("::::: Returning Pre-existing Account Configuration.");
            if (id in accountsConfigurations) {
                accountConfig = accountsConfigurations[id]
                resolve(accountsConfigurations[id])
            } else {
                reject({ message: `::::: Configuration not found for account id: ${id}` })
            }
        } else {
            console.log("::::: Retrieving Account Configuration.");
            var params = {
                TableName: 'customer-configuration'
            };
            dynamodb.scan(params, function (err, data) {
                if (err) {
                    console.log(err, err.stack); // an error occurred
                    reject(err);
                } else {
                    accountsConfigurations = _.keyBy(data['Items'], a => a.id)

                    if (!refresh) {
                        if (id in accountsConfigurations) {
                            accountConfig = accountsConfigurations[id]
                            resolve(accountsConfigurations[id])
                        } else {
                            reject({ message: `::::: Configuration not found for account id: ${id}` })
                        }
                    } else {
                        resolve(true)
                    }
                };
            });
        }
    });
}

exports.getConfigurations = () => {
    return new Promise((resolve, reject) => {

        console.log("::::: Retrieving Account Configurations");
        var params = {
            TableName: 'customer-configuration'
        };
        dynamodb.scan(params, function (err, data) {
            if (err) {
                console.log(err, err.stack); // an error occurred
                reject(err);
            } else {
                accountsConfigurations = _.keyBy(data['Items'], a => a.id)

                resolve(accountsConfigurations)
            };
        });

    });
}


exports.getEncryptionPassword = () => {
    var awsParams = {
        Name: `/${process.env.ENV}/encryption-password`,
        WithDecryption: true,
    };

    return new Promise((resolve, reject) => {
        if (encryptionPassword) {
            resolve(encryptionPassword)
        } else {
            console.log("::::: Retrieving Encryption Password");
            ssm.getParameter(awsParams, function (err, data) {
                if (err) {
                    console.log("::::: ERROR!!! Unable to download and decrypt Encryption Password")
                    console.log(err, err.stack);
                    reject(err);
                } else {
                    console.log("::::: Successfully Downloaded Encryption Password")
                    encryptionPassword = data.Parameter.Value
                    resolve(encryptionPassword)
                }
            });
        }
    })
}

exports.secureLog = async (log, object = null, configuration = null, accountLeadId = null) => {
    try {
        function flatten(data) {
            var result = {};
            function recurse(cur, prop) {
                if (Object(cur) !== cur) {
                    result[prop] = cur;
                } else if (Array.isArray(cur)) {
                    for (var i = 0, l = cur.length; i < l; i++)
                        recurse(cur[i], prop + "[" + i + "]");
                    if (l == 0)
                        result[prop] = [];
                } else {
                    var isEmpty = true;
                    for (var p in cur) {
                        try {
                            // Try to parse JSON
                            var test = cur[p]
                            test = JSON.parse(test)
                            if (typeof test === 'object' ) cur[p] = test
                        } catch (e) {
                            // If not a JSON.. Do nothing
                        }
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

        //This will be our defining object to be encrypted
        var obj;

        //1. Retrieve account configuration via arg or global variable
        accountConfig = configuration ? configuration : accountConfig
        const config = accountConfig;

        //2. Set account name from account config (This value will be appended to every log if available)
        var accountName = config ? config['name'] : null

        //3. Set lead id via arg or global variable (This value will be appended to every log if available)    
        if (accountLeadId) globalLeadId = accountLeadId
        var leadId = globalLeadId ? globalLeadId : null

        //4. If data is a dict, assign to obj variable as a new dict ( This will create a new reference and prevent mutations to the original object)
        // . Else, assign raw value.
        if (object && typeof object === "object") {
            obj = Object.assign({}, object)
        } else if (object) {
            obj = object
        }

        //5. If only one argument is passed and the value is a dict
        // . Assign it to obj as a new dict ( This is in case the developer passes the object to be encrypted as the only argument )
        if (typeof log === "object" && !(log instanceof Error) && !obj) {
            obj = Object.assign({}, log)
            log = ""
        }

        if (obj) {
            if (typeof obj === "object" && config && config['pii_fields']) {
                //6. Retrieve encyprtion code and IV
                var code = await this.getEncryptionPassword()

                //7. Flatten object in order to find PII fields
                var tempObj = flatten(obj)

                //8. For Each PII field stated in the account configuration
                // . Search for it in the flattened object and encrypt the values
                config['pii_fields'].forEach(field => {

                    for (let key in tempObj) {
                        var PII = tempObj[key];

                        let split = key.split(".");
                        let lastKey = split[split.length - 1];
                        let finalKey = String(lastKey).replace(/[^a-zA-Z0-9]/gi, "").toLowerCase()

                        if (field === finalKey) {
                            tempObj[key] = CryptoJS.AES.encrypt(PII, code).toString()
                        }
                    }
                })

                //9. Finally, unflatten the object to return it to its original state
                obj = unflatten(tempObj)
            }

            console.log(`[${accountName}${leadId ? ", " + leadId : ''}] ${log}`, obj)

        } else {
            console.log(`[${accountName}${leadId ? ", " + leadId : ''}] ${log}`)
        }
    } catch (e) {
        console.log("::::: SECURE LOG ERROR", e)
    }
}


