'use strict';
var AWS = require('aws-sdk');
AWS.config.update({ region: 'us-west-1' });
let s3 = new AWS.S3({ region: 'us-west-1' });
let sns = new AWS.SNS();
const moment = require('moment');

var kafka = require('kafka-node'),
    Producer = kafka.Producer;


// Utilities

const { executeQueryOnRedshiftRawConn } = require('pplibs/dbUtils')
const { getKafkaClient } = require('pplibs/kafkaUtils')
const { secureLog, getConfigurationById } = require('pplibs/logUtils')


exports.handler = async (event, context, callback) => {

    try {

        let data = JSON.parse(event['Records'][0]['Sns']['Message'])

        const lead = data.lead
        const account = data.account

        const leadID = lead.lead_id !== null && lead.lead_id !== '' ? lead.lead_id : lead.propair_id

        const accountConfig = await getConfigurationById(account.id);

        secureLog("::::: Initializing Secure Logging", null, accountConfig, leadID)
        secureLog(lead)

        let date = new Date();
        let partitionString = date.getFullYear() + "-" + ("0" + (date.getMonth() + 1)).slice(-2) + "-" + ("0" + (date.getDate())).slice(-2);
        let dateString = date.getFullYear() + "-" + ("0" + (date.getMonth() + 1)).slice(-2) + "-" + ("0" + (date.getDate())).slice(-2) + " " + ("0" + (date.getHours())).slice(-2) + ":" + ("0" + (date.getMinutes())).slice(-2) + ":" + ("0" + (date.getSeconds())).slice(-2)
        let s3Key = "logs/leadsin/dt=" + partitionString + "/" + lead.request_id + ".log"

        
        if (data.isActiveProspect) {
            secureLog("::::: This is an Active Prospect Lead")
            lead['rec_type'] = "Preassignment"
            lead['rec_count'] = 0
        } else {
            let res = await executeQueryOnRedshiftRawConn(`SELECT * FROM leads_in WHERE lead_id=${lead.lead_id} AND account_id=${lead.account_id} ORDER BY rec_count DESC`)
            if (res.rows.length > 0) {
                let item = res.rows[0]
                secureLog(`::::: Lead already exists. Sending as Reassignment. Rec Count: ${(item.rec_count ? item.rec_count : 1) + 1}`)
                lead['rec_type'] = "Reassignment"
                lead['rec_count'] = (item.rec_count ? item.rec_count : 1) + 1
            } else {
                secureLog("::::: Lead does not exist. Sending as First Assignment.")
                
                lead['rec_type'] = lead.propair_id ? "FirstafterAP" : "First"
                lead['rec_count'] = 1
            }
        }

        let prospectResult = null;
        if (lead.recommendation) {
            let lastElement = lead.recommendation.trim().split(":")[lead.recommendation.trim().split(":").length - 1];
            if (lastElement.includes("r")) {
                prospectResult = "RANDOM"
            } else if (lastElement.includes("y")) {
                prospectResult = "ERROR"
            } else {
                prospectResult = "RECOMMENDATION"
            }
        }

        lead['rec_result'] = prospectResult

        //Log Lead Into S3
        var params = {
            Bucket: process.env.BUCKET,
            Key: s3Key,
            Body: JSON.stringify(lead)
        }
        s3.putObject(params, function (err, data) {
            if (err) {
                secureLog("::::: Error logging lead in to S3")
                secureLog(err, err.stack);
            }
        });


        const kafkaClient = await getKafkaClient();
        const producer = new Producer(kafkaClient);

        const payloads = []
        payloads.push({ topic: 'leads_in', messages: JSON.stringify(lead) })


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
        secureLog("::::: Successfully logged lead into database")

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
    }
}
