'use strict';
var AWS = require('aws-sdk');
AWS.config.update({ region: 'us-west-1' });
let s3 = new AWS.S3({ region: 'us-west-1' });
let sns = new AWS.SNS();
const moment = require('moment');

var kafka = require('kafka-node'),
    Producer = kafka.Producer;


// Utilities

const { getKafkaClient, getKafkaAvroClient, parseAvroData } = require('pplibs/kafkaUtils')
const { secureLog, getConfigurationById } = require('pplibs/logUtils')

function getRecommendations(lead, account, recs, probs, isExpansion, isReassignment, dateString) {

    let lead_id = lead['id']
    let pp_id = typeof lead['propair_id'] == 'undefined' || lead['propair_id'] === "" ? null : lead['propair_id']

    let rows = [];
    if (!recs.includes("0000y") && !recs.includes("0001y")) {
        // Non error and non-random rec
        probs.forEach(function (prob, i) {
            if (typeof recs[i] != 'undefined' && recs[i].match(/\d/g) !== null) {
                let probability = prob == "nan" ? null : parseFloat(prob);
                let numProbs = probs.filter(function (n) { return n != "nan" }).map(x => parseFloat(x)).sort().reverse();
                let rank = 0;
                let algoSample = "1"

                if (recs[recs.length - 1].includes("r")) {
                    algoSample = "0"
                }

                for (rank = 0; rank < numProbs.length; rank++) {
                    if (probability && probability >= numProbs[rank])
                        break;
                }

                let agent_result = {
                    account_id: account.id,
                    account_lead_id: lead_id,
                    propair_id: pp_id,
                    agent_id: parseInt(recs[i].match(/\d/g).join("")),
                    account: account.name,
                    probability: probability,
                    rank: rank + 1,
                    recommendation: recs[i].includes("n") ? 0 : 1,
                    random: recs[i].includes("r") ? 1 : 0,
                    error: 0,
                    algo_sample: algoSample,
                    reassignment: isReassignment ? 1 : 0,
                    expansion: isExpansion ? 1 : 0,
                    created_at: dateString
                }
                rows.push(agent_result);
            }
        });
    } else {
        // Error rec
        secureLog("Logging 0000y & 0001y string!");
        secureLog(recs)
        const error_values = ['CRn', 'CRy', '0000y', '0001y']
        recs.forEach(function (rec, i) {
            if (!error_values.includes(rec)) {
                let agent_result = {
                    account_lead_id: parseInt(lead_id),
                    propair_id: pp_id,
                    agent_id: parseInt(rec.match(/\d/g).join("")),
                    account: account.name,
                    account_id: account.id,
                    probability: null,
                    rank: 1,
                    recommendation: 0,
                    random: 0,
                    error: 1,
                    algo_sample: "0",
                    reassignment: isReassignment ? 1 : 0,
                    expansion: isExpansion ? 1 : 0,
                    created_at: dateString
                }

                rows.push(agent_result);
            }
        });
    }

    secureLog(rows.sort(function (a, b) { return a.rank - b.rank }).map(x => JSON.stringify(x)).join("\n"));

    return rows
}


exports.handler = async (event, context, callback) => {

    try {
        context.callbackWaitsForEmptyEventLoop = false;

        let data = JSON.parse(event['Records'][0]['Sns']['Message'])
        const lead = data.lead
        const account = data.account

        let recommendations = [];
        let summary;

        let isReassignment = data.reassignment;
        let isExpansion = data.expansion;
        let code = null;

        let originalDateString = data.originalDateString
        let assignedOfficer = null
        let previousRec = null
        let date = new Date();
        let partitionString = date.getFullYear() + "-" + ("0" + (date.getMonth() + 1)).slice(-2) + "-" + ("0" + (date.getDate())).slice(-2);
        let dateString = typeof originalDateString == 'undefined' ? date.getFullYear() + "-" + ("0" + (date.getMonth() + 1)).slice(-2) + "-" + ("0" + (date.getDate())).slice(-2) + " " + ("0" + (date.getHours())).slice(-2) + ":" + ("0" + (date.getMinutes())).slice(-2) + ":" + ("0" + (date.getSeconds())).slice(-2) : originalDateString;

        let lead_id = lead['id']
        let pp_id = typeof lead['propair_id'] === "undefined" || lead['propair_id'] === "" ? null : lead['propair_id']
        const leadID = lead_id !== null && lead_id !== '' ? lead_id : pp_id

        let recs = null
        let probs = null

        let accountConfig = typeof data.accountConfig === "undefined" ? await getConfigurationById(account.id) : data.accountConfig

        secureLog("::::: Initializing Secure Logging", null, accountConfig, leadID)
        secureLog("::::: Log Insertion Service Starting")
        secureLog("::::: Logging results to Athena")
        secureLog(lead)

        let apFilter = false
        try {
            apFilter = accountConfig['active_prospect']['filtering']
        } catch {
            secureLog("::::: No AP Filter")
        }
        const isActiveProspect = data.active_prospect

        if (typeof lead['recommendation'] !== 'undefined' && lead['recommendation'] !== null) {
            secureLog("::::: Recommendation found, Uploading recommendation agent results..")

            if (isExpansion) {
                secureLog("This is an EXPANSION request")
                recs = lead['expandedRecommendation'].split(":").filter(function (n) { return n != "" });
                probs = lead['expandedProbabilities'].split(":").filter(function (n) { return n != "" });
            } else {
                recs = lead['recommendation'].split(":").filter(function (n) { return n != "" });
                probs = lead['probabilities'].split(":").filter(function (n) { return n != "" });
            }

            if (recs.includes("CRy")) {
                code = 1;
            } else if (recs.includes("CRn")) {
                code = 0;
            }

            recommendations = getRecommendations(lead, account, recs, probs, isExpansion, isReassignment, dateString)
            recommendations = recommendations.sort(function (a, b) { return a.rank - b.rank })

            let finalKey = "logs/recommendations/dt=" + partitionString + "/" + lead_id + "_" + Math.floor(Math.random() * 1000000) + ".log"


            secureLog("::::: Uploading Recommendations to S3")
            var params = {
                Bucket: process.env.BUCKET,
                Key: finalKey,
                Body: recommendations.map(x => JSON.stringify(x)).join("\n")
            }
            await new Promise((resolve, reject) => {
                s3.putObject(params, function (err, data) {
                    if (err) reject({ err, stack: err.stack }); // an error occurred
                    else resolve(data);
                });
            });
            secureLog("::::: S3 Recommendations created")
        }

        secureLog("::::: Uploading summaries..")


        secureLog('summary:::::')
        const isError = recs ? (recs.includes("0000y") || recs.includes("0001y")) : lead['custom_rank_bin'] === 0
        summary = {
            account: account.name,
            account_id: parseInt(account.id, 10),
            account_lead_id: parseInt(lead_id, 10),
            propair_id: pp_id,
            ref_id: 0,
            error: isError ? 1 : 0,
            rec_code: recs ? recs[recs.length - 1] : null,
            custom_rank_code: code,
            custom_rank_bin: lead['custom_rank_bin'],
            custom_rank_value: lead['custom_rank'],
            custom_rank_timeframe: isActiveProspect && apFilter ? -1 : 0,
            reassignment: isReassignment ? 1 : 0,
            expansion: isExpansion ? 1 : 0,
            available_agents: lead['availableAgents'] || null,
            available_agents_threshold: lead['availableAgentsThreshold'] || null,
            agents_added: lead['agentsAdded'] || null,
            created_at: dateString
        }

        //if (isReassignment && assignedOfficer) {
        //    summary["previous_assignment_agent_id"] = assignedOfficer.agent_id
        //    summary["previous_assignment_profile_id"] = assignedOfficer.profile_id
        //    summary["previous_assignment_match"] = !previousRec.includes(assignedOfficer.agent_id + "n");
        //}
        
        secureLog(summary)
        
        secureLog("::::: Uploading Summaries to S3")
        var params = {
            Bucket: process.env.BUCKET,
            Key: "logs/summaries/dt=" + partitionString + "/" + lead_id + "_" + Math.floor(Math.random() * 1000000) + ".log",
            Body: JSON.stringify(summary)
        }
        await new Promise((resolve, reject) => {
            s3.putObject(params, function (err, data) {
                if (err) reject({ err, stack: err.stack }); // an error occurred
                else resolve(data);
            });
        });
        secureLog("::::: S3 Summary created")
        
        const kafkaClient = await getKafkaClient();
        const producer = new Producer(kafkaClient);
        
        const payloads = []
        if (summary) payloads.push({ topic: 'summaries', messages: JSON.stringify(summary) })
        if (recommendations.length > 0) payloads.push({ topic: 'recommendations', messages: recommendations.map(x => JSON.stringify(x)) })
        
        if (payloads.length > 0) {
        
            secureLog("::::: Sending results to kafka")
            await new Promise((resolve, reject) => {
                producer.on('ready', (err, data) => {
                    if (err) reject(err)
                    producer.send(payloads, (err, data) => {
                        if (err) reject(err)
                        console.log(data)
                        producer.close()
                        resolve(data)
                    })
                })
            });
        }

        summary.ksql_id = parseInt(summary.account_id.toString() + summary.account_lead_id.toString(), 10)
        secureLog("::::: Getting KafkaAvro Client")
        const kafka = await getKafkaAvroClient()

        let schema = JSON.parse((await kafka.schemas.getByName('summaries_ksql')).definition).fields;
        let parsed = parseAvroData(summary, schema);

        secureLog("::::: Sending Summary to KSQL")
        await new Promise((resolve, reject) => {
            kafka.send({
                topic: 'summaries_ksql',
                messages: parsed
            }).then(success => {
                // Message was sent encoded with Avro Schema//
                resolve(success)
            }, error => {
                reject(error)
            })
        })

        secureLog("::::: All Done")

        callback(null, {
            statusCode: 200,
            headers: {},
            body: { status: 'ok' }
        })
        return

    } catch (err) {
        secureLog("::::: ERROR ::::::")
        secureLog(err)

        var errorParams = {
            Message: JSON.stringify(err),
            Subject: "Production Error in LogLead Lambda",
            TopicArn: process.env.TOPIC_ERROR
        };
        sns.publish(errorParams, function (err, data) {
            if (err) {
                secureLog("Error sending error email (duh!)")
                secureLog(err);
            }
        });
        callback(null, {
            statusCode: 500,
            headers: {},
            body: JSON.stringify(err)
        });
        return
    }
}
