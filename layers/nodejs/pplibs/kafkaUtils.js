
const AWS = require('aws-sdk');
AWS.config.update({ region: 'us-west-1' });

const ssm = new AWS.SSM({ apiVersion: '2014-11-06' });
var dynamodb = new AWS.DynamoDB.DocumentClient({ region: 'us-west-1' });
var kafka = require('kafka-node')
const KafkaAvro = require('kafka-node-avro');

let kafkaPassword;
let kafkaConfig;

exports.getKafkaConfig = (env, refresh = false) => {
    return new Promise((resolve, reject) => {
        if (!kafkaConfig || refresh === true) {
            console.log("::::: Retrieving Kafka Configuration");
            var params = {
                TableName: 'system-configuration',
                Key: {
                    name: 'kafka'
                }
            };
            dynamodb.get(params, function (err, data) {
                if (err) {
                    console.log(err, err.stack); // an error occurred
                    reject(err);
                } else {
                    resolve(data['Item'][env])
                };
            });
        } else {
            resolve(kafkaConfig)
        }

    });
}

exports.getKafkaPassword = (env, refresh = false) => {
    return new Promise((resolve, reject) => {
        if (!kafkaPassword || refresh) {
            console.log("::::: Retrieving Kafka Password");
            var params = {
                Name: `/${env}/kafka-password`,
                WithDecryption: true
            };

            ssm.getParameter(params, function (err, data) {
                if (err) {
                    reject(err);
                } else {
                    kafkaPassword = data.Parameter.Value;
                    resolve(kafkaPassword);
                }
            });
        } else {
            resolve(kafkaPassword);
        }
    });
}

exports.parseAvroData = (records, schema) => {
    let parsedRecords = []
    records = Array.isArray(records) ? records : [records]
    records.forEach( record => {
        let parsedRecord = {}
        schema.forEach(col => {
            if (record.hasOwnProperty(col.name)) {
                if (['long', 'int'].includes(col.type[0])) {
                    let testVal = record[col.name]

                    // Test for Date string and parse as integer
                    if (typeof record[col.name] === 'string') {
                        let re = /[^\d.\s]/g.test(testVal)
                        if (re) {
                            testVal = Date.parse(testVal)
                            // set to microsecond
                            if(!isNaN(testVal)) testVal = testVal * 1000
                        }   
                    }
                    // Test for integer
                    test = parseInt(testVal, 10)

                    parsedRecord[col.name] = isNaN(test) ? null : test 
                } else if (['float', 'double'].includes(col.type[0])) {
                    let test = parseFloat(record[col.name])
                    parsedRecord[col.name] = isNaN(test) ? null : test
                } else {
                    parsedRecord[col.name] = record[col.name] !== null ? record[col.name].toString() : null
                }
            } else {
                let Val = null
                if (['created_at', 'updated_at'].includes(col.name)) Val = Date.parse(new Date()) * 1000
                parsedRecord[col.name] = Val
            }
        })

        parsedRecords.push(parsedRecord)
    })

    return parsedRecords
} 

exports.getKafkaAvroClient = (refresh = false) => {
    return new Promise((resolve, reject) => {
        const {
            ENV: env
        } = process.env;

        let params = {
            kafkaHost: '',
            sasl: {
                mechanism: 'plain',
                username: '',
                password: ''
            }
        }
        this.getKafkaConfig(env, refresh)
            .then(config => {
                params['schemaHost'] = config['schema_host']
                params['kafkaHost'] = config['host']
                params['sasl']['username'] = config['user']

                return this.getKafkaPassword(env, refresh)
            }).then(password => {
                params['sasl']['password'] = password

                const Settings = {
                    "kafka": {
                        "kafkaHost": params.kafkaHost,
                        "sasl": params.sasl
                    },
                    "schema": {
                        "registry": params.schemaHost
                    }
                }
                KafkaAvro.init(Settings).then(kafka => {
                    resolve(kafka)
                }, error => {
                    reject(error);
                });
            })
    })
}

exports.getKafkaClient = (refresh = false) => {
    return new Promise((resolve, reject) => {
        const {
            ENV: env
        } = process.env;

        let params = {
            kafkaHost: '',
            sasl: {
                mechanism: 'plain',
                username: '',
                password: ''
            }
        }
        this.getKafkaConfig(env, refresh)
            .then(config => {
                params['kafkaHost'] = config['host']
                params['sasl']['username'] = config['user']

                return this.getKafkaPassword(env, refresh)
            }).then(password => {
                params['sasl']['password'] = password

                var client = new kafka.KafkaClient(params)
                resolve(client)
            })
    })
}