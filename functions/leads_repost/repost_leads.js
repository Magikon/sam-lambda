'use strict';
var AWS = require('aws-sdk');
AWS.config.update({ region: 'us-west-1' });

let s3 = new AWS.S3({ region: 'us-west-1' });
let athena = new AWS.Athena({ region: 'us-west-2' });
let sns = new AWS.SNS();
let CSV = require('csv-string');
const { reportToRollbar, ProPairError } = require('pplibs/customErrors')

const { executeQueryOnRedshiftRawConn } = require('pplibs/dbUtils')
const moment = require('moment');
const axios = require("axios");

let _ = require('lodash');

function getSummaries(hoursFromNow, account) {
  console.log("::::: Fetching Summaries")

  return new Promise(async (resolve, reject) => {
    try {
      let res = await executeQueryOnRedshiftRawConn(`SELECT * FROM summaries WHERE created_at >= DATEADD('hour',  -${hoursFromNow}, getdate()) AND account_id=${account.id} AND account_lead_id IS NOT NULL;`)
      
      if (res.rows) {
        resolve(res.rows)
      } else {
        let error = ProPairError("No summaries found!", "Repost")
        reject(error)
      }

    } catch(e) {
      reject(e)
    }
      
  });
}

const reflect = p => p.then(v => ({ v, status: "fulfilled" }),
  e => ({ e, status: "rejected" }));

const delayIncrement = 500;
let delay = 0;

function executeLeadsApi(lead, account) {
  // Increment delay for each lead so they will trigger in intervals.
  delay += delayIncrement;

  const headers = {
    Authorization: `Bearer ${account.token}`
  };
  const body = {
    lead: {
      payload: lead
    },
    isQa: true,
    isRepost: true
  };


  return new Promise((resolve, reject) => {
    new Promise(resolve => setTimeout(resolve, delay)).then(() => {
      console.log(`::::: Processing lead: ${lead.id}`)
      var result = axios.post(process.env.LEADS_ENDPOINT, body, { headers })
      resolve(result);
    })
  });
}

async function processLeads(leads, account, originalLeads) {
  console.log("::::: Processing leads")
  var promises = []

  try {
    leads.forEach(lead => {
      promises.push(executeLeadsApi(lead, account));
    })

    const result = await Promise.all(promises.map(reflect))
    const failed = result.filter(item => item.status === 'rejected');
    const succeeded = result.filter(item => item.status === 'fulfilled');
    if (failed) {
      failed.forEach(failure => {
        if (typeof failure.e.response !== 'undefined') {
          console.log("ERROR", failure.e.response.data)
        } else {
          console.log("ERROR", failure.e.message)
        }
      })
    }
    console.log(`PASSED: ${succeeded ? succeeded.length : 0}, FAILED: ${failed ? failed.length : 0}`)
    if (succeeded) {
      let sample = {}

      succeeded.forEach(item => {
        let lead = item.v.data.recommendResult.lead
        let original = _.find(leads, o => o.id === lead.id)

        sample[lead.id] = { recommendResult: lead, initialPayload: original }

      })

      return sample
    } else {
      throw new ProPairError("All leads failed to post", "Repost")
    }
  } catch (e) {
    console.log("ERROR", e)
    throw e
  }
}


async function uploadToS3(sample, account) {

  console.log("::::: Syncing s3 object")


  // Check if existing sample for this day
  var getParams = {
    Bucket: process.env.BUCKET,
    Key: `sample-${moment().format("MM-DD-YY")}.json`,
    ResponseContentType: 'application/json'
  }

  var finalSample = Object.assign({}, sample)

  var existingSample;
  await new Promise((resolve, reject) => {
    s3.getObject(getParams, function (err, data) {
      if (err) {
        console.log("::::: Existing sample not found")
        //console.log(err, err.stack);
        resolve()
      } else {
        console.log("::::: Successfully retrieved sample", data);
        existingSample = JSON.parse(data.Body.toString());
        resolve()
      }
    });
  })

  if (existingSample) {
    console.log(`::::: Found existing sample for date: ${moment().format('MM-DD-YY')}. Merging...`)
    finalSample = _.merge(finalSample, existingSample)
  }

  var putParams = {
    Body: JSON.stringify(finalSample),
    Bucket: process.env.BUCKET,
    Key: `sample-${moment().format("MM-DD-YY")}.json`
  }
  await new Promise((resolve, reject) => {
    s3.putObject(putParams, function (err, data) {
      if (err) {
        console.log(err, err.stack); // an error occurred
        reject(err)
      } else {
        console.log(`::::: Sucessfully uploaded sample-${moment().format("MM-DD-YY")}`, data);
        resolve()
      }
    });
  });

}

function testResults(leads, summaries, threshold) {
  console.log(`::::: Testing custom rank for ${Object.keys(leads).length} leads`)
  console.log(`::::: Failure threshold is ${threshold * 100}%`)
  var result = {
    failed: [],
    passed: []
  }
  var missingCount = 0;
  Object.keys(leads).forEach(key => {
    const item = leads[key]['recommendResult'];
    const summary = _.find(summaries, s => String(s.lead_id) === String(item.id));
    if (summary) {
      if (Math.abs((summary.custom_rank - item.custom_rank) / summary.custom_rank) > 1e-05) {
        result['failed'].push(item)
      } else {
        result['passed'].push(item)
      }
    } else {
      console.log(`:::: WARNING! Could not find summary for lead id: ${item.id}`)
      missingCount++;
    }
  });
  var failureRate = Math.round((result.failed.length / (Object.keys(leads).length - missingCount)) * 100);
  if (failureRate > 20) {
    var e = { type: "error", name: "Repost Test Failure", message: `TEST FAILED! Failure rate: ${failureRate}%`, result, leads, summaries }
    let error = new ProPairError(`TEST FAILED! Failure rate: ${failureRate}%`, "Repost")
    error.resultRepost = result
    error.leads = leads
    error.summaries = summaries
    console.log(error.message)
    result.failureRate = failureRate
    throw error
  } else {
    console.log(`::::: TEST SUCCESSFULL! Failure rate: ${failureRate}%`)
    result.failureRate = failureRate
    return result
  }
}

exports.handler = async (event, context, callback) => {
  console.log("::::: Repost Service Starting");
  try {
    //console.log('::::: Event', event);
    
    let account = event.account
    const leads = event.leads
    const accountConfig = event.config
    const threshold = accountConfig['repost_failure_threshold'] ? accountConfig['repost_failure_threshold'] : 0.2
    const hoursFromNow = accountConfig['repost_hours_from_now'] ? accountConfig['repost_hours_from_now'] : 48
    
    console.log(`:::: Testing Custom Rank for Account ${account.name}`)
    const summaries = await getSummaries(hoursFromNow, account)
    const repostedSample = await processLeads(leads, account);
    const customRankResults = await testResults(repostedSample, summaries, threshold);
    //await uploadToS3(repostedSample, account);

    console.log("All done.")

    callback(null, {
      statusCode: 200,
      headers: {},
      body: JSON.stringify({ results: customRankResults })
    });

  } catch (e) {
    console.log(":::::::::: GOT AN ERROR TO REPORT :::::::::::::::");
    console.log(e);
    reportToRollbar(e, process.env.TOPIC_ROLLBAR, "Repost")

    callback(null, {
      statusCode: 500,
      headers: {},
      body: JSON.stringify(e)
    });
  }
};
