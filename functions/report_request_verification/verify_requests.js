'use strict';
var AWS = require('aws-sdk');
AWS.config.update({ region: 'us-west-1' });

let sns = new AWS.SNS();
let XML = require('pixl-xml');
let _ = require('lodash')

let REQUEST = require('request');

let leadErrors;
let leadsCounter;
let redshiftClient;

// Utilities
const { getRedshiftClient } = require('pplibs/dbUtils')
const { secureLog, getConfigurationById } = require('pplibs/logUtils')

function getLead(form, request, account) {
  return new Promise((resolve, reject) => {
    let velocifyUrl = process.env.VELOCIFY_URL + "GetLead"
    REQUEST.post({ url: velocifyUrl, form: form }, function (error, response, body) {
      if (error || response.statusCode !== 200) {
        if (typeof leadErrors[account.name] === 'undefined') leadErrors[account.name] = []
        let statusCode = typeof response != 'undefined' && typeof response.statusCode != 'undefined' ? response.statusCode : ''
        let errObj = { error, response, body, statusCode }
        leadErrors[account.name].push(errObj)
        reject(errObj)
      } else {
        let leadResult = XML.parse(body)['Lead']
        resolve({ lead: leadResult, request: request, account: account });
      }
      leadsCounter++;
      if (leadsCounter % 25 === 0) secureLog(`Fetched ${leadsCounter} leads from Velocify`)
    })
  });
}

function fetchVelocifyLeads() {
  return new Promise((resolve, reject) => {
    let accounts;

    redshiftClient.query("SELECT * FROM accounts;")
      .then(res => {
        secureLog("::::: Loaded list of accounts.");
        accounts = res.rows;
        let qry = `
            SELECT * FROM leads_in WHERE lead_id is not null and request_id NOT IN 
            ( SELECT request_id from leads_out WHERE created_at >= current_date-1 ) 
            AND created_at >= current_date-1 LIMIT 200;
          `
        return redshiftClient.query(qry);
      }).then(res => {
        console.log("::::: Redshift connection closed")
        secureLog("::::: Loaded list of leads");
        let entries = res.rows;
        let promises = [];
        leadsCounter = 0;

        if (entries.length > 0) {
          secureLog(`::::: Retrieved ${entries.length} leads`)
          for (let i = 0; i < entries.length; i++) {

            let request = { requestId: entries[i]['request_id'], leadId: entries[i]['lead_id'], accountId: entries[i]['account_id'], accountName: entries[i]['account_name'], recType: entries[i]['rec_type'], recCount: entries[i]['rec_count'], createdAt: entries[i]['created_at'], sreTimestamps: entries[i]['sre_timestamps'] }
            let account = accounts.filter(a => { return parseInt(a.id, 10) === parseInt(request.accountId, 10) })[0];

            if (!account) {
              secureLog(`The account with ID "${request.accountId}" doesn't exist in ${process.env.ENV}`);
              continue;
            }

            let form = {
              username: account.velocify_username,
              password: account.velocify_password,
              leadId: request.leadId
            }

            promises.push(getLead(form, request, account))
          }

          resolve(promises);
        } else {
          resolve(false);
        }
      }).catch(e => {
        reject(e)
      });
  })
}

function getVerifications(entries, accounts) {
  return new Promise((resolve, reject) => {

    let verifications = []

    try {

      for (let i = 0; i < entries.length; i++) {
        try {

          let leadResult = entries[i].v.lead;
          let request = entries[i].v.request;
          let account = entries[i].v.account;

          let success = null;
          let result = null;

          if (request.recType === 'First' || request.recType === 'Reassignment') {
            // Report new first request due for verification
            //reportRequest(process.env.TOPIC_FIRST_REC_PENDING)

            let field = leadResult.Fields.Field.filter(f => { return f.FieldId === account.velocify_rec_field_1_id.toString() })[0];
            success = (typeof field != 'undefined' && typeof field.Value != 'undefined' && field.Value.trim().length > 0)

            if (success) {
              //reportRequest(process.env.TOPIC_FIRST_REC_SUCCESS)
              let lastElement = field.Value.trim().split(":")[field.Value.trim().split(":").length - 1];
              if (lastElement.includes("r")) {
                result = "RANDOM"
                //reportRequest(process.env.TOPIC_FIRST_REC_RESULT_RANDOM)
              } else if (lastElement.includes("y")) {
                result = "ERROR"
                //reportRequest(process.env.TOPIC_FIRST_REC_RESULT_ERROR)
              } else {
                result = "RECOMMENDATION"
                //reportRequest(process.env.TOPIC_FIRST_REC_RESULT_REC)
              }
            } else {
              //reportRequest(process.env.TOPIC_FIRST_REC_FAILURE)
            }
          } else if (request.recType === 'Second') {
            //reportRequest(process.env.TOPIC_SECOND_REC_PENDING)

            let field1 = leadResult.Fields.Field.filter(f => { return f.FieldId === account.velocify_rec_field_1_id.toString() })[0];
            //TODO: Un-hardcode this.
            let field2 = account.velocify_branch_rec_field_1_id == null ? { Value: '1' } : leadResult.Fields.Field.filter(f => { return f.FieldId === account.velocify_branch_rec_field_1_id.toString() })[0];

            secureLog("::::: Validating 2nd request")
            secureLog(field1)
            secureLog(field2)

            if (typeof field2 == 'undefined') {
              secureLog("Field 2 is undefined, debugging:")
              secureLog(account.velocify_branch_rec_field_1_id)
              secureLog(leadResult.Fields.Field)
            }

            success = (typeof field1 != 'undefined' && typeof field1.Value != 'undefined' && field1.Value.trim().length > 0 && typeof field2 != 'undefined' && typeof field2.Value != 'undefined' && field2.Value.trim().length > 0)

            secureLog(success)

            if (success) {
              //reportRequest(process.env.TOPIC_SECOND_REC_SUCCESS)
              let lastElement = field1.Value.trim().split(":")[field1.Value.trim().split(":").length - 1];
              if (lastElement.includes("r")) {
                result = "RANDOM"
                //reportRequest(process.env.TOPIC_SECOND_REC_RESULT_RANDOM)
              } else if (lastElement.includes("y")) {
                result = "ERROR"
                //reportRequest(process.env.TOPIC_SECOND_REC_RESULT_ERROR)
              } else {
                result = "RECOMMENDATION"
                //reportRequest(process.env.TOPIC_SECOND_REC_RESULT_REC)
              }
            } else {
              //reportRequest(process.env.TOPIC_SECOND_REC_FAILURE)
            }
          }

          let date = new Date();
          let dateString = date.getFullYear() + "-" + ("0" + (date.getMonth() + 1)).slice(-2) + "-" + ("0" + (date.getDate())).slice(-2) + " " + ("0" + (date.getHours())).slice(-2) + ":" + ("0" + (date.getMinutes())).slice(-2) + ":" + ("0" + (date.getSeconds())).slice(-2);

          let verification = {
            request_id: request.requestId,
            lead_id: request.leadId,
            account_id: request.accountId,
            account_name: request.accountName,
            rec_type: request.recType,
            success: success,
            result: result,
            rec_count: request.recCount,
            sre_timestamps: request.sreTimestamps,
            created_at: dateString
          }
          verifications.push(verification);
        } catch (e) {
          secureLog("::::: ERROR! Moving on to next lead", e)
          continue
        }
      }

      resolve(verifications)
    } catch (e) {
      reject(e)
    }
  });
}

function insertIntoDB(rows) {
  return new Promise((resolve, reject) => {

    // Categorizing results
    const rec_count = rows.filter(row => row.result === "RECOMMENDATION").length;
    const rand_count = rows.filter(row => row.result === "RANDOM").length;
    const err_count = rows.filter(row => row.result === "ERROR").length;
    const lead_counts = { recs: rec_count, randoms: rand_count, errors: err_count }



    redshiftClient.query(`SELECT \"column\",type from pg_table_def where tablename='leads_out'`)
      .then(res => {
        const dbColumns = _.pickBy(_.keyBy(res.rows, row => row.column), item => item.column !== 'id');
        let query = `INSERT INTO leads_out_upsert(${Object.keys(dbColumns)}) VALUES`

        rows.forEach((row, i) => {

          let recordValues = []
          Object.keys(dbColumns).forEach((key, i) => {
            let value = typeof row[key] === 'undefined' ? null : row[key];
            let dataType = dbColumns[key].type;

            if (typeof value === 'string') {
              value = `$$${value}$$::${dataType}`;
            } else if (key === 'created_at') {
              value = 'getdate()'
            } else if (value === null) {
              value = `NULL`
            } else {
              value = `${value}::${dataType}`
            }

            recordValues.push(value)
          })

          query += `(${recordValues})`
          if (i < rows.length - 1) {
            query += ', ';
          } else {
            query += ';'
          }
        })

        return redshiftClient.query(query);
      }).then(res => {
        return redshiftClient.query(`call sp_leads_out_incremental()`);
      }).then(res => {
        console.log("::::: Redshift connection closed")
        resolve({ res: res, lead_counts: lead_counts });
      }).catch(error => {
        secureLog("ERROR", error);
        reject({ code: 401, error: error, message: "Failed to insert leads" });
      });
  })
}

const reflect = p => p.then(v => ({ v, status: "fulfilled" }),
  e => ({ e, status: "rejected" }));

function reportToRollbar(e) {
  console.log(":::::::::: GOT AN ERROR TO REPORT :::::::::::::::");
  console.log(e);

  var errorParams = {
    Message: JSON.stringify({ error: e, referer: "RequestVerification", account: '' }),
    Subject: "Rollbar Error",
    TopicArn: process.env.TOPIC_ROLLBAR
  };

  sns.publish(errorParams, function (err, data) {
    if (err) {
      console.log("ERROR: Failed to report exception to Rollbar");
      console.log(err);
      console.log(e);
    }
  });

}

exports.handler = async (event, context, callback) => {
  secureLog("::::: Verify Requests Service Starting")
  // This will allow us to freeze open connections to a database
  context.callbackWaitsForEmptyEventLoop = false;

  if (typeof redshiftClient === 'undefined') redshiftClient = await getRedshiftClient(false)

  if (event.refresh) {
    secureLog(":::: Refreshing Cache.")
    try {

      redshiftClient = await getRedshiftClient(false, true)
      secureLog(":::: Caches Successfully Refreshed");
      callback(null, {
        statusCode: 200,
        headers: {},
        body: { status: 'ok' }
      })
      return
    } catch (e) {
      secureLog("Error while refreshing cache:", e)
      callback(null, {
        statusCode: 500,
        headers: {},
        body: JSON.stringify({ status: e.message })
      })
      return
    }
  }
  secureLog("::::: Initialize secure logging", null, null, 1)
  // initialize errors
  leadErrors = {}

  await fetchVelocifyLeads().then(result => {
    if (result) {
      Promise.all(result.map(reflect)).then((result) => {
        var passed = result.filter(x => x.status === 'fulfilled');
        var failed = result.filter(x => x.status === 'rejected');
        secureLog("::::: Leads Found", passed.length)
        secureLog("::::: Leads Rejected", failed.length)

        let groupedErrors = {}
        if (leadErrors !== {}) {
          Object.keys(leadErrors).forEach(key => {

            var groups = _.groupBy(leadErrors[key], function (value) {
              // if(body.include("Lead does not exist"))
              let error = value.error ? value.error : "None"
              let body = value.body ? value.body : "None"
              if (body.includes("Lead does not exist")) {
                body = "Lead does not exist";
              }
              return error + '#' + body;
            })

            secureLog(`:::::::::: ${key.toUpperCase()} ERRORS ::::::::::`)
            _.forEach(groups, group => {
              console.log("<<--------------------->>")
              secureLog(`::::: ERROR COUNT: ${group.length}`)
              secureLog(`::::: ERROR: ${group[0].error}`)
              secureLog(`::::: BODY: ${group[0].body}`)
              secureLog(`::::: STATUS CODE: ${group[0].statusCode}`)
              console.log("<<--------------------->>")
              let failureMessage = { message: group[0].body ? group[0].body : group[0].error, code: group[0].statusCode ? group[0].statusCode : 500, account: key }
              if (process.env.ENV === 'production') reportToRollbar(failureMessage)
            });

          });
        }

        if (passed.length > 0) {
          return passed
        } else {
          throw ({ code: 401, message: "Failed to retrieve any leads" })
        }
      }).then(result => {
        return getVerifications(result)
      }).then(result => {
        return insertIntoDB(result)
      }).then(result => {
        secureLog(`::::: Successfully Inserted Leads`)
        const recap = `Recommendations: ${result.lead_counts.recs}, Randoms: ${result.lead_counts.randoms}, Errors: ${result.lead_counts.errors}`
        secureLog(':::::', recap)

        callback(null, {
          statusCode: 200,
          headers: {},
          body: JSON.stringify({ status: 'ok', message: recap })
        });
      }).catch(e => {
        reportToRollbar(e)

        callback(null, {
          statusCode: e.code || 500,
          headers: {},
          body: JSON.stringify({ status: e.message || e })
        });
      })

    } else {
      secureLog("::::: No Recent Leads Found")

      callback(null, {
        statusCode: 200,
        headers: {},
        body: JSON.stringify({ status: 'ok', message: "No Recent Leads Found" })
      });
    }
  }).catch(e => {
    reportToRollbar(e)
    callback(null, {
      statusCode: e.code || 500,
      headers: {},
      body: JSON.stringify({ status: e.message || e })
    });
  });


  //startQueryExecution().then(function(result){
  //  secureLog(result);
  //  getQueryResult(result['QueryExecutionId'], callback);
  //});
}
