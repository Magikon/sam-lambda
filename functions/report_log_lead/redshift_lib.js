const { secureLog } = require('pplibs/logUtils')
const { getRedshiftClient } = require('pplibs/dbUtils')

var AWS = require('aws-sdk');


// lib variables
var models_path = './models/';
const aws_region = 'us-west-1';

AWS.config.update({ region: aws_region });

// Create a Secrets Manager client

exports.insert_redshift = (param) => {
    return new Promise((resolve, reject) => {
        try {
            getRedshiftClient().then(redshiftClient => {
                redshiftClient.connect(async function (err) {
                    table_name = param['table_name'];
                    var bad_data = redshiftClient.import(`${models_path}bad_data.js`);

                    // insert into recommendations
                    if (table_name == 'recommendations') {
                        var recommendations = redshiftClient.import(`${models_path}${table_name}.js`);
                        records = param['record'].split('\n');
                        
                        let resolved = await Promise.all(records.map(async (record) => {
                            return await new Promise((resolve, reject) => {
                                try {
                                    record_json = JSON.parse(record)
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
                                                    secureLog(err.message);
                                                    reject(err)
                                                } else {
                                                    secureLog("::: Bad Data Logged :::");
                                                    resolve(result)
                                                }
                                            }
                                            );
                                            secureLog(err.message);
                                        } else {
                                            resolve(result)
                                        }
                                    });
                                } catch(e) {
                                    reject(e)
                                }
                            });
                        })
                        );
                        
                        secureLog(`::: ${resolved.length} new recommendations created! :::`);
                        resolve(resolved);

                    }

                    // insert into summaries
                    if (table_name == 'summaries') {
                        var summaries = redshiftClient.import(`${models_path}${table_name}.js`);
                        record_json = param['record']
                        summaries.create(record_json, (err, result) => {
                            if (err) {
                                bad_record = {
                                    program_name: 'log_lead_meta',
                                    table_name: 'summaries',
                                    table_record: JSON.stringify(record_json),
                                    error_message: err.message,
                                };
                                bad_data.create(bad_record, function (err, result) {
                                    if (err) {
                                        secureLog(err.message);
                                    } else {
                                        secureLog("::: Bad Data Logged :::");
                                        resolve(result)
                                    }
                                }
                                );
                                secureLog(err.message);
                            } else {
                                secureLog("::: new summary created ! :::");
                                resolve(result)
                            }
                        });
                    }
                });
            });
        } catch (e) {
            reject(e)
        }
    });
}
