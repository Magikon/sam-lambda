'use strict';
var AWS = require('aws-sdk');
AWS.config.update({ region: 'us-west-1' });

let sns = new AWS.SNS();
let lambda = new AWS.Lambda({ region: 'us-west-1' });
let s3 = new AWS.S3({ region: 'us-west-1' });
let csv = require('csvtojson');
let JSONToCSV = require('json2csv').parse;
var kafka = require('kafka-node'),
    Producer = kafka.Producer;

const _ = require('lodash')
const moment = require('moment')
// Utilities
const { getRedshiftClient, insertToDB, cast } = require('pplibs/dbUtils')
const { secureLog, getConfigurationById } = require('pplibs/logUtils')
const { getKafkaClient } =  require('pplibs/kafkaUtils')
const { ProPairError, reportToRollbar } = require('pplibs/customErrors')

//Global QA FLAG
let isQA;
let fileNum;

function fetchAccount(name) {
    secureLog(`::::: Fetching Account: ${name} From DB`)
    return new Promise((resolve, reject) => {
        getRedshiftClient().then(client => {
            client.connect((err) => {
                if (err) { 
                    let directError = new ProPairError(err.message ? err.message : "DBERROR", "ProcessDirectMail")
                    reject(directError); }
                let query = `SELECT * FROM accounts WHERE name='${name}';`
                client.query(query)
                    .then(res => {
                        client.close()
                        if (res.rows.length > 0) {
                            resolve(res.rows[0]);
                        } else {
                            let directError = new ProPairError(`Unable find Account: ${name} in DB`, "ProcessDirectMail")
                            reject(directError);
                        }
                    }).catch(error => {
                        secureLog("ERROR", error);
                        let directError = new ProPairError(error.message ? error.message : "DBERROR", "ProcessDirectMail")
                        reject(directError);
                    });
            })
        });
    });
}

function fetchGlobalAttr(id) {
    secureLog(`::::: Fetching Global Attribute Lookup`)
    return new Promise((resolve, reject) => {
        getRedshiftClient().then(client => {
            client.connect((err) => {
                if (err) {
                    console.log(err);
                    let directMailError = new ProPair(err.message ? err.message : "DB connection error")
                    reject(directMailError); }
                let query = `
                    SELECT * FROM global_attribute_lookup 
                    WHERE table_name='direct_mail' 
                    AND account_id=${id};
                `
                client.query(query)
                    .then(res => {
                        client.close()
                        if (res.rows.length > 0) {
                            secureLog("::::: Succesfully Fetched  Global Attribute Lookup")
                            resolve(res.rows);
                        } else {
                            let directMailError = new ProPair(`Unable find direct_mail Global Attr for Account ID: ${id} in DB`)
                            reject(directMailError);
                        }
                    }).catch(error => {
                        secureLog("ERROR", error);
                        let directMailError = new ProPair("Failed to fetch Global Attribute Lookup")
                        reject(directMailError);
                    });
            })
        });
    });
}

async function downloadFile(params) {
    secureLog("::::: Fetching data")

    // get csv file and create stream
    const stream = s3.getObject(params).createReadStream();

    // convert csv file (stream) to JSON format data
    const json = await csv().fromStream(stream);

    secureLog(`::::: Retrieved ${json.length} leads`)
    return json
}

async function uploadFile(data, bucket, paths, date) {
    secureLog("::::: Uploading s3 object")

    const newFileName = `${date}_Propair_CustomRank${fileNum ? `_${fileNum}` : ''}.csv`
    // get csv file and create stream
    var newCsv = JSONToCSV(data, { fields: Object.keys(data[0]) })
    var params = {
        Bucket: bucket,
        Key: `${paths.slice(0, 2).join('/')}/download/${newFileName}`,
        Body: newCsv
    }
    await new Promise((resolve, reject) => {
        s3.upload(params, (err, data) => {
            if (err) {
                reject(err);
            } else {
                secureLog(`::::: Successfully uploaded file to ${params.Key}`)
                resolve(true)
            }
        })
    })

}

function appendToFile(data, file) {
    secureLog("::::: Appending custom rank values to file")
    var errorCount = 0;

    console.time('Append Time')
    var finalArray = _.map(file.slice(0), function (f) {
        var t = _.find(data, d => f['Ref ID'].toString() === d.ref_id.toString())

        if (t) {
            f['custom_rank_bin'] = t.custom_rank_bin
        } else {
            f['custom_rank_bin'] = 0
            errorCount++;
        }

        return f
    });
    console.timeEnd('Append Time')

    secureLog(`Custom Rank calculated for ${file.slice(0).length - errorCount} out of ${file.slice(0).length} leads`)
    return finalArray
}

function map(data, attrMap, account, date, emptyValues = true) {
    secureLog("::::: Mapping data")
    let leads = []

    data.forEach(lead => {
        if (typeof lead['Ref ID'] === "undefined" || !lead["Ref ID"] || lead["Ref ID"] === '') return
        var leadMap = {}

        attrMap.forEach(map => {
            if (map['customer_field_name'] in lead) {
                leadMap[map['propair_field']] = cast(lead[map['customer_field_name']], map['propair_field'], map['datatype']);
            } else if (map['customer_field_name'] ? map['customer_field_name'].includes(':') : false) {
                let components = map['customer_field_name'].split(':')
                if (components[0] in lead) {
                    const values = lead[components[0]].split(map['customer_split'])
                    if (values.length > parseInt(components[1], 10)) {
                        leadMap[map['propair_field']] = cast(values[parseInt(components[1], 10)], map['propair_field'], map['datatype']);
                    }
                }
            } else if (map['customer_original_field_name'] in lead) {
                leadMap[map['propair_field']] = cast(lead[map['customer_original_field_name']], map['propair_field'], map['datatype']);;
            } else if (emptyValues) {
                leadMap[map['propair_field']] = ""
            }

        })
        leadMap['account_id'] = account.id

        //TODO: Remove this once mail date is fixed
        if (!isQA) leadMap['mail_date'] = moment(date, 'YYYYMMDD').format('YYYY-MM-DD') + " 00:00:00"

        leads.push(leadMap);
    })

    return leads;
}

function chunkArrayInGroups(arr, size) {
    var myArray = [];
    for (var i = 0; i < arr.length; i += size) {
        myArray.push(arr.slice(i, i + size));
    }
    return myArray;
}

function recommend(
    customer,
    map,
    config,
    index,
    fileName
) {
    return new Promise((resolve, reject) => {
        try {
            const payload = JSON.stringify({
                customer,
                map,
                account_config: config,
                run_direct_mail: true,
                index,
                file_name: fileName
            });
            lambda.invoke({
                FunctionName: process.env.FUNC_DIRECT_MAIL,
                Payload: payload
            }, function (error, data) {
                if (error) {
                    console.log(error)
                    reject({ code: 500, message: "server error" });
                } else {

                    try {
                        let response = JSON.parse(data.Payload)
                        if (typeof response.leads === "undefined") {
                            let error = { name: "DirectMailException", message: "DirectMail returned no leads", type: "error" }
                            reject(error);
                        } else {
                            let leads = JSON.parse(response.leads)
                            secureLog(`::::: Successfully retrieved custom rank for ${leads.length} leads`)
                            resolve(leads);
                        }
                    } catch (err) {
                        secureLog(err)
                        secureLog("Python rec error")
                        let error = { name: "DirectMailException", message: "DirectMailException", type: "error", trace: { rec: err } }
                        reject(error);
                    }
                }
            });
        } catch (err) {
            secureLog(err)
            let error = { name: "LambdaInvokeException", message: "LambdaInvoke Exception", type: "error", trace: { lead: lead, error: err } }
            reject(error);
        }
    });
}


function storeData(data, account, index) {
    return new Promise((resolve, reject) => {
        getRedshiftClient().then(redshift => {
            redshift.connect(async (err) => {
                if (err) { reject(err) }
                try {
                    let unique = ['ref_id', 'account_id']
                    let defaults = { 'account_id': account.id }
    
                    await insertToDB(data, 'direct_mail', redshift, unique, index, defaults)
                    redshift.close()
                    resolve()
                } catch(e) {
                    reject(e)
                }
            });
        })
    })
}


function sendToKafka(payload, producer) {
    return new Promise((resolve, reject) => {
        producer.send(payload, (err, data) => {
            if (err) reject(err)
            resolve(data)
        })
    })
}

exports.handler = async (event, context, callback) => {
    console.log("::::: Initializing ProcessDirectMail")
    let account;
    let accountName;
    try {
        const contents = event['Records'][0];
        var data;
        if (contents.hasOwnProperty('s3')) {
            data = event['Records'][0]['s3'];
            console.log(":::: Detected S3 Event")
        } else if (contents.hasOwnProperty('Sns')) {
            data = JSON.parse(event['Records'][0]['Sns']['Message']);
            console.log(":::: Detected SNS Message")
        } else {
            throw 'Invalid Event'
        }

        const paths = data.object.key.split('/')

        if (paths[paths.length - 2] === 'upload') {
            const fileName = paths[paths.length - 1]
            const fileDate = moment(fileName.split('_')[0], 'YYYYMMDD').isValid() ? fileName.split('_')[0] : null

            accountName = paths[paths.length - 3]
            console.log(`::::: Detected File Upload. Account: ${accountName}, File: ${fileName}`)

            if (fileDate) {
                fileNum = fileName.split('_').slice(1).map(f => parseInt(f.replace(/.csv/gi, ''), 10)).find(f => !isNaN(f))
                isQA = fileName.split('_')[fileName.split('_').length - 1].replace(/.csv/gi, '') === 'QA' ? true : false
                account = await fetchAccount(accountName);
                const accountConfig = await getConfigurationById(account.id);
                const dataChunkSize = accountConfig['direct_mail_data_chunk_size'] ? accountConfig['direct_mail_data_chunk_size'] : 10000
                const customRankChunkSize = accountConfig['direct_mail_chunk_size'] ? accountConfig['direct_mail_chunk_size'] : 1000

                secureLog("::::: Initializing Secure Logging", null, accountConfig)

                const attrMap = await fetchGlobalAttr(account.id);

                var fileParams = {
                    Bucket: data.bucket.name,
                    Key: data.object.key,
                }

                let fileData = await downloadFile(fileParams);
                fileData = _.filter(fileData, file => typeof file['Ref ID'] !== "undefined" && file["Ref ID"] && file["Ref ID"] !== '')

                if (fileName.toLowerCase().includes('mailsent')) {
                    secureLog(`::::: This is a Mail Sent File`)
                    const normalizedData = await map(fileData, attrMap, account, fileDate, false);
                    
                    const dataGroups = chunkArrayInGroups(normalizedData, dataChunkSize);

                    const kafkaClient = await getKafkaClient();
                    const producer = new Producer(kafkaClient);

                    secureLog(`::::: Sending to stream ${dataGroups.length} times for ${normalizedData.length} leads in chunks of ${dataChunkSize}`)
                    await new Promise((resolve, reject) => {
                        producer.on('ready', async (err, data) => {
                            if (err) reject(err)
                            for (let index = 0; index < dataGroups.length; index++) {
                                let payloads = [{topic: 'direct_sent_mail', messages: dataGroups[index].map(x => JSON.stringify(x))}]
                                await sendToKafka(payloads, producer)
                            }
                            resolve(data)
                            producer.close()
                        })
                    });

                } else {

                    const normalizedData = await map(fileData, attrMap, account, fileDate);
                    var customRankPromises = []
    
    
                    const customRankGroups = chunkArrayInGroups(normalizedData, customRankChunkSize);
    
                    secureLog(`::::: Running custom rank ${customRankGroups.length} times for ${normalizedData.length} leads in chunks of ${customRankChunkSize}`)
                    customRankGroups.forEach((arr, i) => {
                        customRankPromises.push(recommend(
                            account.name,
                            arr,
                            accountConfig,
                            i,
                            fileName
                        ))
                    })
    
                    const reflect = p => p.then(v => ({ v, status: "fulfilled" }),
                        e => ({ e, status: "rejected" }));
    
                    const recResults = await Promise.all(customRankPromises.map(reflect))
                    var passed = recResults.filter(x => x.status === 'fulfilled');
                    var failed = recResults.filter(x => x.status === 'rejected');
    
                    if (failed.length > 0) {
                        for (let index = 0; index < failed.length; index++) {
                            let errorMsg = failed[index].e.message ? failed[index].e.message : "Rejected event"
                            let directMailError = new ProPairError(errorMsg)
                            directMailError.event = event
                            directMailError.account = accountName
                            await reportToRollbar(directMailError, process.env.TOPIC_ROLLBAR, "ProcessDirectMail")
                        }
                    } else {
                        const mergedResults = _.concat(..._.map(passed, res => res.v))
    
                        const appendedData = await appendToFile(mergedResults, fileData);
    
                        await uploadFile(appendedData, data.bucket.name, paths, fileDate)
                        
                        const kafkaClient = await getKafkaClient();
                        const producer = new Producer(kafkaClient);
    
                        const dataGroups = chunkArrayInGroups(mergedResults, dataChunkSize);

                        try {
                            secureLog("::::: Sending results to kafka")
                            secureLog(`::::: Sending to streams ${dataGroups.length} times for ${normalizedData.length} leads in chunks of ${dataChunkSize}`)
                            await new Promise((resolve, reject) => {
                                producer.on('ready', async (err, data) => {
                                    if (err) reject(err)
                                    for (let index = 0; index < dataGroups.length; index++) {
                                        let payloads = [
                                            {topic: 'direct_mail', messages: dataGroups[index].map(x => JSON.stringify(x))},
                                            {topic: 'summaries', messages: dataGroups[index].map(x => JSON.stringify(x))}
                                        ]
                                        
                                        try {
                                            await sendToKafka(payloads, producer)
                                        } catch(e) {
                                            reject(e)
                                        }
    
                                    }
                                    resolve(data)
                                    producer.close()
                                })
                            });
                        } catch(e) {
                          secureLog(":::: Error sending to streams")  
                          console.log(e)
                        }
                        
                        secureLog("::::: Uploading Summaries to S3")
                        let date = new Date();
                        let partitionString = date.getFullYear() + "-" + ("0" + (date.getMonth() + 1)).slice(-2) + "-" + ("0" + (date.getDate())).slice(-2);
                        let finalKey = "logs/summaries/dt=" + partitionString + "/" + accountName + "_" + fileName.split('.')[0] + ".log"

                        var params = {
                            Bucket: process.env.BUCKET,
                            Key: finalKey,
                            Body: mergedResults.map(x => JSON.stringify(x)).join("\n")
                        }
                        await new Promise((resolve, reject) => {
                            s3.putObject(params, function (err, data) {
                                if (err) reject({ err, stack: err.stack }); // an error occurred
                                else resolve(data);
                            });
                        });
                        secureLog("::::: S3 Summaries created")

                    }
                }

                secureLog("::::: All Done")
            } else {
                let directMailError = ("Unrecognized file date. File name must abide by the correct naming convention -- YYYYMMDD_Propair_Upload.csv")
                throw (directMailError)
            }

        }
        return
        
    } catch (e) {
        secureLog("::::: ERROR :::::")
        secureLog(e)
        await reportToRollbar(e, process.env.TOPIC_ROLLBAR, "ProcessDirectMail")
        return 
    }
}