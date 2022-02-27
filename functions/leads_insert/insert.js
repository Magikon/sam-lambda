'use strict';
var AWS = require('aws-sdk');
const KafkaAvro = require('kafka-node-avro');
AWS.config.update({ region: 'us-west-1' });
let s3 = new AWS.S3({ region: 'us-west-1' });
var kafka = require('kafka-node'),
  Producer = kafka.Producer;

// Utilities
const { getKafkaClient, getKafkaConfig, getKafkaPassword, getKafkaAvroClient, parseAvroData  } = require('pplibs/kafkaUtils')
const { secureLog } = require('pplibs/logUtils')
const { ProPairError, reportToRollbar } = require('pplibs/customErrors')

exports.handler = async (event, context, callback) => {


  try {
    context.callbackWaitsForEmptyEventLoop = false;
    
    console.log(event)
    let data = JSON.parse(event['Records'][0]['Sns']['Message'])
    let lead = data['lead'];
    let customer = data.customer;

    lead['state'] = lead.originalRecommendation ? "EXPANDED" : "NEW"

    const leadID = lead.id !== null && lead.id !== '' ? lead.id : lead.propair_id

    const globalAccountConfig = data.accountConfig;

    secureLog("::::: Initializing Secure Logging", null, globalAccountConfig, leadID)
    secureLog("::::: Database Insertion Service Starting");
    secureLog("::::: EVENT", event);
    secureLog("::::: LEAD", lead);

    const item = { id: lead.id, customer: customer.name, payload: JSON.stringify(lead) };

    const date = new Date();
    const dateString = date.getFullYear() + "-" + ("0" + (date.getMonth() + 1)).slice(-2) + "-" + ("0" + (date.getDate())).slice(-2) + " " + ("0" + (date.getHours())).slice(-2) + ":" + ("0" + (date.getMinutes())).slice(-2) + ":" + ("0" + (date.getSeconds())).slice(-2);

    const pginsertlead = {
      customer_lead_id: item.id ? item.id : null,
      propair_id: lead.propair_id ? lead.propair_id : null,
      customer_id: customer.id,
      payload: item.payload,
      state: lead.state,
      created_at: dateString,
      updated_at: dateString
    }

    let partitionString = date.getFullYear() + "-" + ("0" + (date.getMonth() + 1)).slice(-2) + "-" + ("0" + (date.getDate())).slice(-2);

    let finalKey = "logs/leads_table/dt=" + partitionString + "/" + pginsertlead.customer_lead_id + "_" + Math.floor(Math.random() * 1000000) + ".log"

    var params = {
      Bucket: process.env.BUCKET,
      Key: finalKey,
      Body: JSON.stringify(pginsertlead)
    }

    await new Promise((resolve, reject) => {
      try {
        s3.putObject(params, function (err, data) {
          if (err) {
            let error = new ProPairError(err.message, "InsertRecord")
            error.stack = err.stack
            reject(error); // an error occurred
          }
          else resolve(data);
        });
      } catch (e) {
        let err = new ProPairError(e.message, "InsertRecord")
        err.stack = e.stack
        reject(err)
      }
    })
    secureLog("::::: Lead succesfully sent to S3");

    let kafkaClient = await getKafkaClient();
    let producer = new Producer(kafkaClient);

    let payload = [{ topic: 'leads', messages: JSON.stringify(pginsertlead) }]
    await new Promise((resolve, reject) => {
      try {
        producer.on('ready', (err, data) => {
          if (err) reject(err)
          producer.send(payload, (err, data) => {
            if (err) reject(err)
            resolve(data)
            producer.close()
          })
        })
      } catch (e) {
        reject(e)
      }
    })
    secureLog("::::: Lead Successfully sent to kafka stream");

    pginsertlead.ksql_id = parseInt(pginsertlead.customer_id.toString() + pginsertlead.customer_lead_id.toString(), 10)

    secureLog("::::: Getting KafkaAvro Client")
    const kafka = await getKafkaAvroClient()

    let schema = JSON.parse((await kafka.schemas.getByName('leads_ksql')).definition).fields;

    let parsed = parseAvroData(pginsertlead, schema);

    secureLog("::::: Sending Summary to KSQL")
    await new Promise((resolve, reject) => {
        kafka.send({
            topic: 'leads_ksql',
            messages: parsed
        }).then(success => {
            // Message was sent encoded with Avro Schema//
            secureLog(success)
            resolve(success)
        }, error => {
            reject(error)
        })
    })

    secureLog("::::: All Done");

    callback(null, {
      statusCode: 200,
      headers: {},
      body: { status: 'ok' }
    })
    return 

  } catch (err) {
    console.log("::::: ERROR :::::")
    console.log(err)

    let error = new ProPairError(err , "InsertRecordError")
    await reportToRollbar(error, process.env.TOPIC_ROLLBAR, "InsertRecord")

    callback(null, {
      statusCode: err.code || 500,
      headers: {},
      body: JSON.stringify({ status: err.message })
    });
    return
  }
}