const AWS = require('aws-sdk');
const _ = require('lodash');

const ssm = new AWS.SSM({apiVersion: '2014-11-06'});
let velocifyPasswords;

function generateAccountPasswords(env, refresh = false) {
    var awsParams = {
        Path: `/${env}/velocifyPasswords`,
        WithDecryption: true,
    };
    return new Promise((resolve, reject) => {
        if (velocifyPasswords && !refresh) {
            console.log("::::: Velocify Passwords Already Generated");
            console.log(velocifyPasswords);
            resolve(velocifyPasswords);
        } else {
            console.log("::::: Generating Velocify Passwords");
            getVelocifyPasswords(awsParams).then(params => {
                let passwords = [];
                _.forIn(params, param => {
                    const key = param.Name.replace(`/${env}/velocifyPasswords/`, "");
                    passwords[key] = param.Value;
                });
                console.log("::::: Velocify Passwords Successfully Generated");
                velocifyPasswords = passwords;
                resolve(velocifyPasswords);
            }).catch(error => {
                console.log("ERROR", error);
                reject(error);
            });
        }
    });
}

function getVelocifyPasswords(params) {
    return new Promise((resolve, reject) => {
        console.log("::::: Downloading and Decrypting Account Passwords");
        ssm.getParametersByPath(params, function (err, data) {
            if (err) {
                console.log("::::: ERROR!!! Unable to download and decrypt Accounts Passwords");
                console.log(err, err.stack);
                reject(err);
            } else {
                console.log("::::: Successfully Downloaded and Decrypted Accounts Passwords");
                const passwords = data.Parameters;
                if (data.NextToken) {
                    params.NextToken = data.NextToken;
                    getVelocifyPasswords(params).then(nextPasswords => {
                        resolve([...nextPasswords, ...passwords]);
                    });
                } else {
                    resolve(passwords);
                }
            }
        });
    });
}

module.exports = generateAccountPasswords;
