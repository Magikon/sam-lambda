const { secureLog } = require('pplibs/logUtils')
const { getRedshiftClient } = require('./dbUtils.js')

// original insert_redshift header
var Redshift = require('node-redshift');
var yaml = require('js-yaml');
var fs = require('fs');
var AWS = require('aws-sdk');


// lib variables
var models_path = 'pplibs/models/';
const aws_region = 'us-west-1';
const SSM = require('aws-sdk/clients/ssm');

AWS.config.update({ region: aws_region });

// Create a Secrets Manager client
var client = new AWS.SecretsManager({
    region: aws_region
});

// In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
// See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
// We rethrow the exception by default.
exports.withRedshiftPassword = (param_store, secret_name) => {
    return new Promise(function (resolve, reject) {
        client.getSecretValue({ SecretId: secret_name }, function (err, data) {
            if (err) {
                reject(err)
            }
            else {
                // Decrypts secret using the associated KMS CMK.
                // Depending on whether the secret is a string or binary, one of these fields will be populated.
                if ('SecretString' in data) {
                    secret = data.SecretString;
                } else {
                    let buff = new Buffer(data.SecretBinary, 'base64');
                    secret = buff.toString('ascii');
                }
            }
            secret_json = JSON.parse(secret);
            aws_secret_access_key = secret_json['AWS_SECRET_ACCESS_KEY'];
            aws_access_key_id = secret_json['AWS_ACCESS_KEY_ID'];

            const ssm = new SSM({ region: 'us-west-1', aws_secret_access_key: aws_secret_access_key, aws_access_key_id: aws_access_key_id })

            var params = {
                Name: param_store,
                WithDecryption: true
            };

            ssm.getParameter(params, function (err, data) {
                if (err) reject(err); // an error occurred
                else resolve(data['Parameter']['Value']);           // successful response
            });

            //resolve(aws_access_key_id);
        })
    });
}


exports.insert_redshift = (param, param_store, aws_credentials) => this.withRedshiftPassword(param_store, aws_credentials).then(function successHandler(redshift_passw) {
    getRedshiftClient().then(redshiftClient => {
        table_name = param['table_name'];
        var bad_data = redshiftClient.import(`./models/bad_data.js`);
        query_command = `insert into ${table_name} values ()`;

        // insert into recommendations
        if (table_name == 'recommendations') {
            var recommendations = redshiftClient.import(`${models_path}${table_name}.js`);
            records = param['record'].split('\n');
            records.forEach(function (record) {
                record_json = JSON.parse(record)
                // force test err code
                // record_json['account_lead_id'] = null;
                recommendations.create(record_json, function (err, result) {
                    if (err) {
                        bad_record = {
                            program_name: 'log_lead_meta',
                            table_name: 'recommendations',
                            table_record: JSON.stringify(record_json),
                            error_message: err.message,
                        };
                        bad_data.create(bad_record, function (err, result) {
                            if (err) {
                                secure_log(err.message);
                            } else {
                                secure_log("::: Bad Data Logged :::");
                            }
                        }
                        );
                        secure_log(err.message);
                    } else {
                        secure_log("::: new recommendation created ! :::");
                    }
                });
            }
            );

        }

        // insert into summaries
        if (table_name == 'summaries') {
            secure_log("::: inserting summaries :::");
            var summaries = redshiftClient.import(`${models_path}${table_name}.js`);
            records = [];
            records.push(param['record']);
            records.forEach(function (record) {
                record_json = record;
                // force test err code
                // record_json['account_lead_id'] = null;
                summaries.create(record_json, function (err, result) {
                    if (err) {
                        bad_record = {
                            program_name: 'log_lead_meta',
                            table_name: 'summaries',
                            table_record: JSON.stringify(record_json),
                            error_message: err.message,
                        };
                        bad_data.create(bad_record, function (err, result) {
                            if (err) {
                                secure_log(err.message);
                            } else {
                                secure_log("::: Bad Data Logged :::");
                            }
                        }
                        );
                        secure_log(err.message);
                    } else {
                        secure_log("::: new summary created ! :::");
                    }
                });
            }
            );
        }
    });
});
