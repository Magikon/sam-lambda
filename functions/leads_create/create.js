'use strict';
var AWS = require('aws-sdk');
AWS.config.update({ region: 'us-west-1' });

let lambda = new AWS.Lambda({ region: 'us-west-1' });
let s3 = new AWS.S3({ region: 'us-west-1' });
let sns = new AWS.SNS();

const axios = require('axios').default;
let _ = require('lodash');
const moment = require('moment');

// Global Variables
let globalAccounts;
let globalCampaigns;
let coldStart = true;

const ssm = new AWS.SSM({ apiVersion: '2014-11-06' });
// Utilities
const { executeQueryOnRedshiftRawConn} = require('pplibs/dbUtils')
const { generateToken, encrypt, decrypt } = require('pplibs/mainUtils')
const { createMetric } = require('pplibs/metrics_util')
const { retrieveCache } = require('pplibs/redisUtils')
const { secureLog, getConfigurationById, getEncryptionPassword } = require('pplibs/logUtils')
const { modifyLeadField, modifyLeads, getCampaigns } = require('pplibs/velocifyHelper')
const { reportToRollbar, ProPairError } = require('pplibs/customErrors')

function getAccounts(encPassword) {
  secureLog("::::: Fetching Accounts")

  return new Promise(async (resolve, reject) => {

    let res = await executeQueryOnRedshiftRawConn("SELECT * FROM accounts where token is not null;")
    let accounts = {}
    for (let item of res.rows) {
      item.token = item.token ? encrypt(item.token, encPassword) : item.token
      accounts[item.name] = item
    }
    if (accounts == null || typeof accounts === "undefined" || accounts == {}) {
      let error = new ProPairError("Error while accessing redshift to extract accounts", "Misc")
      reject(error)
    }
    else {
      resolve(accounts)
    }
    return;

  });
}

function getAccountCampaigns(account) {
  return new Promise(async (resolve, reject) => {
    try {
      if (!globalCampaigns || typeof globalCampaigns[account.name] === 'undefined') {
        secureLog(`::::: Local Campaign cache expired.. Retrieving..`)
        let unique_key = `${account.name}_campaigns`
        if (!globalCampaigns) globalCampaigns = {}
        globalCampaigns[account.name] = await retrieveCache(unique_key, () => getCampaigns(account['velocify_username'], account['velocify_password']))
      }

      resolve(globalCampaigns[account.name])
      return;

    } catch (error) {
      let miscError = ProPairError(error, "Misc")
      reject(miscError)
    }
  });
}

function authenticate(event) {
  console.log("::::: Authenticating Request")
  return new Promise(async (resolve, reject) => {
    try {
      const unique_key = `accounts`
      const authKey = event.headers.Authorization.replace('Bearer ', '')
      const encPassword = await getEncryptionPassword()

      if (!globalAccounts) {
        globalAccounts = await retrieveCache(unique_key, () => getAccounts(encPassword), 72)
      }

      let account;
      if (process.env.ENV === "staging") {
        let eventBody = JSON.parse(event.body);
        account = globalAccounts[Object.keys(globalAccounts).find(key => parseInt(globalAccounts[key].id, 10) === parseInt(eventBody.lead.payload['CustomerId'], 10))]
      } else {
        account = globalAccounts[Object.keys(globalAccounts).find(key => decrypt(globalAccounts[key].token, encPassword) === authKey)]
      }
      if (account) {
        resolve(account)
        return;
      } else {
        let unauthorizedError = new ProPairError('Authorization token invalid', "UnauthorizedError");
        reject(unauthorizedError);
      }

    } catch (error) {
      let miscError = new ProPairError(error, "Misc")
      reject(miscError)
    }
  });
}

async function queryKSQL(ksql) {
  let url = "http://10.34.8.111:8088/query"
  let content = { ksql, streamsProperties: {} }
  let options = {
      method: 'POST',
      headers: { 'Accept': "application/vnd.ksql.v1+json" },
      data: JSON.stringify(content),
      url
  };

  let response;
  try {
      response = await axios(options)
      if (response.data.length > 1) {
          const header = response.data.shift().header
          const reg = /`\w*`/g
          const headers = header.schema.match(reg).map(x => x.toLowerCase().replace('`', ''))
          return response.data.map(x => {
              let row = {}
              x.row.columns.forEach((val, i) => {
                  row[headers[i]] = val
              })
              return row
          })
      } else {
          console.log('No data found!')
          console.log('SQL:', ksql)
          return null
      }
  } catch (e) {
    try {
      console.log('Request Failure!', e.response.data.message)
    } catch(err) {
      console.log(e.message ? e.message : e)
    }
    return null
  }
}


function getLead(customer, accountLeadId) {
  return new Promise(async (resolve, reject) => {
    try {
      const sql = `
          select
            payload
          from
              leads
          where
              state = 'NEW'
              and customer_id = ${customer.id}
              and customer_lead_id = ${accountLeadId};`;
      let result = await executeQueryOnRedshiftRawConn(sql)

      //console.time('KSQL Performance')
      //let sq = `select payload from LEADS_KSQL_TABLE where ROWKEY=${customer.id}${accountLeadId} emit changes;`
      //let leader = await queryKSQL(sq)
      //console.log("KSQL DATA: " + leader)
      //console.timeEnd('KSQL Performance')

      resolve(result.rows[0]);

    } catch (error) {
      secureLog("ERROR", error);
      let miscError = new ProPairError(error.message, "Misc")
      reject(miscError);

    }
  })
}

function map(lead, customer, inContactAvailability = null, accountConfig, pp_id) {
  secureLog("::::: Generating Value Map")
  return new Promise((resolve, reject) => {
    try {
      const payload = JSON.stringify({ lead, customer, availability: inContactAvailability, accountConfig, pp_id });
      lambda.invoke(
        {
          FunctionName: process.env.FUNC_MAP,
          Payload: payload
        }, function (error, data) {
          if (error || data === null || typeof JSON.parse(data.Payload).error !== 'undefined' || JSON.parse(data.Payload) == null || JSON.parse(data.Payload).body == null || typeof JSON.parse(data.Payload).body.map == 'undefined' || JSON.parse(data.Payload).body.map == null) {
            if (data) {
              try {
                reject(JSON.parse(data.Payload).error);
              } catch (error) {
                let e = new ProPairError("Mapping Parse Failure", "Misc")
                e.stack = error.stack
                reject(e);
              }

            } else {
              let e = new ProPairError(error.message, error.name)
              reject(e);
            }

          } else {
            //secureLog("[" + lead.id + "] ::::: Value Map Generated!!!!");

            const result = JSON.parse(data.Payload).body;
            const { map, availability } = result;
            resolve({ map, customer, availability });
            //secureLog(result);
          }
        });
    }

    catch (err) {
      secureLog("!!!!")
      secureLog(err)
    }
  });
}

function trimRec(rec, exclude_y, exclude_n) {
  if (exclude_y) {
    return ":" + rec.split(":").filter(n => n.toUpperCase().includes("N") || n.toUpperCase().includes("CR")).map(n => n.replace("nr", "n").replace("ne", "n").replace("ns", "n")).join(":") + ":" + rec.split(":")[rec.split(":").length - 1];
  } else if (exclude_n) {
    return ":" + rec.split(":").filter(n => n.toUpperCase().includes("Y") || n.toUpperCase().includes("CR")).map(n => n.replace("yr", "y").replace("ye", "y").replace("ys", "y")).join(":") + ":" + rec.split(":")[rec.split(":").length - 1];
  } else {
    return rec
  }
}

function recommend(
  lead,
  customer,
  map,
  config,
  availability = null,
  recommendationString = null,
  probabilities = null,
  existingLeadPayload = null,
  active_prospect = false
) {
  secureLog("::::: Generating Recommendation for customer " + customer)
  return new Promise((resolve, reject) => {
    try {
      const payload = JSON.stringify({
        customer,
        map,
        availability,
        recommendation: recommendationString,
        probabilities,
        account_config: config,
        existingLeadPayload,
        lead_id: lead.id,
        active_prospect
      });

      secureLog('::::: Recommend Payload:', JSON.parse(payload));

      lambda.invoke({
        FunctionName: process.env.FUNC_RECOMMEND,
        Payload: payload
      }, function (error, data) {
        if (error) {
          secureLog(":::: Error invoking Reccommend", error)
          let miscError = new ProPairError("Error invoking Reccommend", "Misc")
          reject(miscError);
        } else {

          try {
            let response = JSON.parse(data.Payload)
            secureLog("::::: Recommendation Response", response)

            lead.propair_id = response['lead']['propair_id']
            lead.probabilities = response['probabilities'];
            lead.features = response['features'];
            lead.reassignment = 0;
            lead.custom_rank = response['custom_rank'];
            lead.custom_rank_bin = response['custom_rank_bin'];
            lead.availableAgents = response['available_agents']
            lead.availableAgentsThreshold = response['available_agents_threshold'];
            lead.agentsAdded = response['agents_added'];
            lead.agentOrderRecommendation = response['agent_order_recommendation'];
            lead.agentOrderProbabilities = response['agent_order_probabilities'];
            lead.trimmedRecommendation = null;

            lead.recommendation = response['recommendation'] ? response['recommendation'].replace(/['"]+/g, '') : null
            lead.originalRecommendation = response['original_recommendation'] ? response['original_recommendation'].replace(/['"]+/g, '') : null
            if (lead.recommendation) {
              let rec = lead.recommendation

              if (rec.match(/:\d+\w+/i)) {
                secureLog("::::: Recommendation format validates!")
                const configVars = config.prospect_matching.variables
                let exclude_y = typeof configVars['exclude_rec_y'] !== "undefined" ? configVars['exclude_rec_y'] : true
                let exclude_n = typeof configVars['exclude_rec_n'] !== "undefined" ? configVars['exclude_rec_n'] : false

                lead.trimmedRecommendation = trimRec(rec, exclude_y, exclude_n)

              } else {
                let parsingError = new ProPairError(`Payload format error. Value: ${rec}`, "ProPairParsingRecommendError")
                reject(parsingError);
              }
            } else {
              secureLog("::::: No Recommendation Returned")
              if (typeof response["errorMessage"] !== 'undefined' && response["errorMessage"] != null) {
                if (!response["errorMessage"].includes("cardinal")) {
                  let parsingError = new ProPairError(response["errorMessage"], "ProPairParsingRecommendError")
                  reject(parsingError);
                }
              }
            }

            resolve({ lead: lead, customer: customer, coldstart: response.coldstart });
          } catch (err) {
            secureLog(err)
            secureLog("Python rec error")
            let parsingError = new ProPairError(err.message, "ProPairParsingRecommendError")
            reject(parsingError);
          }
        }
      });
    } catch (err) {
      secureLog(err)
      let miscError = new ProPairError("SNS Exception", "Misc")
      reject(miscError);
    }
  });
}

function modifyLead(account, lead, fields) {
  return new Promise((resolve, reject) => {
    let fieldsPayload = []
    fields.forEach(f => {
      if (typeof account[f['accountField']] === 'undefined') {
        let err = { message: `Field does not exist`, name: "Database Exception", field: f['accountField'], type: "error" };
        secureLog(`::::: FAILED modify lead in Velocify. Error: ${err.message}. Field: ${f['accountField']}`);
        let modifyLeadError = new ProPairError("Field does not exist", "ModifyLeadError")
        modifyLeadError.field = f['accountField']
        reject(modifyLeadError)
      }
      if (account[f['accountField']] === null) {
        let err = { message: `Field ID not found! Value is NULL.`, name: "Database Exception", field: f['accountField'], type: "error" }
        secureLog(`::::: FAILED modify lead in Velocify. Error: ${err.message}. Field: ${f['accountField']}`);
        let modifyLeadError = new ProPairError(err.message, "ModifyLeadError")
        modifyLeadError.field = err.field
        reject(modifyLeadError)
      }
      if (typeof lead[f['leadField']] === 'undefined') {
        let err = { message: `Lead Result Field not found!.`, name: "CreateLead Exception", field: f['leadField'], type: "error" }
        secureLog(`::::: FAILED modify lead in Velocify. Error: ${err.message}. Field: ${f['leadField']}`);
        let modifyLeadError = new ProPairError(err.message, "ModifyLeadError")
        modifyLeadError.field = err.field
        reject(modifyLeadError)
      }
      secureLog(`::::: Modifying field: ${f['accountField']} with value: ${lead[f['leadField']]} for lead: ${lead.id}`);
      fieldsPayload.push({ id: account[f['accountField']], value: lead[f['leadField']], name: f['accountField'] })
    })
    modifyLeads([lead],
      account['velocify_username'],
      account['velocify_password'],
      fieldsPayload)
      .then(r => {
        let status = parseInt(r["Lead"].attributes.statusId, 10)

        if (status === 3 && r['Lead'].attributes.message === 'No update needed') {
          secureLog(`::::: No update needed for lead in Velocify`);
          resolve(true)
        } else if (status === 2) {
          secureLog("::::: Successfully modified lead in Velocify");
          _.forEach(Array.isArray(r['Lead']['Field']) ? r['Lead']['Field'] : [r['Lead']['Field']], item => {
            let fieldMatch = _.find(fieldsPayload, x => x.id.toString() === item.attributes.id)
            secureLog(`::::: Field: ${fieldMatch.name}, Message: ${item.attributes.message}`)
          })

          resolve(r['Lead']);
        } else {
          try {
            let fieldErr = _.find(fieldsPayload, x => x.id.toString() === r['Lead']['Field'].attributes.id)
            let err = {
              message: r['Lead']['Field'].attributes.message,
              field: fieldErr.name,
              name: "VelocifyException",
              type: "error"
            }
            secureLog(`::::: FAILED modify lead in Velocify. Error: ${err.message}. Field: ${err.field}. Value: ${lead[fieldErr.value]}`);
            let velocifyError = new ProPairError(err.message, "VelocifyError")
            velocifyError.field = err.field
            reject(velocifyError)
          } catch (e) {
            secureLog(e)
            let err = {
              message: r['Lead'].attributes.message,
              name: "VelocifyException",
              type: "error"
            }
            let velocifyError = new ProPairError(err.message, "VelocifyError")
            reject(velocifyError)
          }
        }

      }).catch(e => {
        let velocifyError = {}
        if (typeof e === "string") {
          velocifyError = new ProPairError(e, "VelocifyError")
        }
        else if (typeof e.message !== "undefined" && e.message != null) {
          velocifyError = new ProPairError(e.message, "VelocifyError")
        }

        reject(velocifyError)
      })
  })
}

async function getInsellerateTokens() {
  var items = {};
  var params = {
      Path: `/staging/insellerateToken/`,
      WithDecryption: true,
      MaxResults: 1
  };

  try {

      function list(params) {
          return new Promise((resolve, reject) => {
              ssm.getParametersByPath(params, async function (err, data) {
                  if (err) {
                      reject(err, err.stack); // an error occurred
                  } else {
                      var contents = data.Parameters;
                      contents.forEach(x => {
                          //let paths = x.Name.split('/');
                          let account = x.Name.split('/').pop()
                          items[account] = x.Value
                      })

                      if (data.NextToken) {
                          params.NextToken = data.NextToken;
                          console.log("get further list...");
                          await list(params);
                      }
                      resolve();
                  }
              });
          })
      }

      await list(params);
      if (Object.keys(items).length > 0) {
          return items
      } else {
          throw 'No insellerate Tokens found'
      }
  } catch (e) {
      console.log('List Objects Error!', e)
      console.log('Params:', params)
      throw e
  }
}

function insellerate(lead, account, fields) {
  return new Promise(async (resolve, reject) => {
    const url = `https://app.insellerate.com/api/integration/UpdateApplication?orgId=1338&applicationId=${lead.id}`
    const tokens = await retrieveCache(`insellerate_tokens`, () => getInsellerateTokens(), 72);
    const token = tokens[account.name]

    let body = []
    fields.forEach(f => {
      body.push({ "FieldId": account[f.accountField], "Value": lead[f.leadField] })
    })

    let options = {
      method: 'POST',
      headers: { "Authorization": `Basic ${token}` },
      data: body,
      url
    };
    secureLog(":::: Posting to Insellerate", body)
    let response;
    try {
      response = await axios(options)
      resolve(response)
    } catch (e) {
      secureLog(e)
      reject(e)
    }
  })
}

function velocify(result, account, passwords, map, config) {
  let lead = result.lead;

  let write_rec = false
  let write_cr = false
  let write_day_0 = false
  let use_custom_api = typeof config.use_custom_api !== "undefined" ? config.use_custom_api : false

  const dataSource = config.data_source ? config.data_source : "velocify"

  if (config.prospect_matching) write_rec = typeof config.prospect_matching.triggers['use_prospect_matching'] !== "undefined" ? config.prospect_matching.triggers['use_prospect_matching'] : true  // Defaults to true
  if (config.custom_rank) write_cr = typeof config.custom_rank.triggers['use_custom_rank'] !== "undefined" ? config.custom_rank.triggers['use_custom_rank'] : false
  if (config.dynamic_rank) write_day_0 = typeof config.dynamic_rank.triggers['write_day_0'] !== "undefined" ? config.dynamic_rank.triggers['write_day_0'] : false

  let fields = []

  if (write_rec) {
    fields.push({ accountField: 'velocify_rec_field_1_id', leadField: 'trimmedRecommendation' })
  }

  if (write_cr) {
    fields.push({ accountField: "custom_rank_bin_id", leadField: "custom_rank_bin" })
  }

  if (write_day_0) {
    fields.push({ accountField: "day_0_rank_id", leadField: "custom_rank_bin" })
  }

  function getSubstringBetween(text, a1, a2) {
    return text.substring(
      text.lastIndexOf(a1) + a1.length, text.lastIndexOf(a2)
    )
  }

  return new Promise((resolve, reject) => {

    if (fields.length > 0) {
      if (use_custom_api && process.env.ENV == 'production') {
        secureLog("::::: Posting to Cardinals API");
        var params = {
          Name: `/${process.env.ENV}/cardinal-api-post`,
          WithDecryption: true
        };

        ssm.getParameter(params, function (err, data) {
          if (err) {
            let cardinalError = {}
            if (typeof err === "string") {
              cardinalError = new ProPairError(err, "CardinalAPIError")
            }
            else {
              cardinalError = new ProPairError(err.message, "CardinalAPIError")
            }

            reject(cardinalError);
          } else {
            var url = data.Parameter.Value;
            var params = `&XmlResponse=True&LeadId=${lead.id}`;
            if (write_rec) params += `&ProPairInput=${lead.trimmedRecommendation}`
            if (write_cr) params += `&ProPairCustomRank=${lead.custom_rank_bin}`
            if (write_day_0) params += `&Day0Rank=${lead.custom_rank_bin}`

            secureLog("::::: PARAMS", params)
            axios.post(url + params)
              .then(response => {
                var result = getSubstringBetween(response.data, "<UpdateResult>", "</UpdateResult>")
                var desc = getSubstringBetween(response.data, "<UpdateResultDescription>", "</UpdateResultDescription>")
                secureLog(`::::: Write-back Result: ${result}! Description: ${desc === "" ? "None" : desc}`)
                resolve(response)
              })
              .catch(e => {
                secureLog(e)
                resolve(e)
              })

          }
        });
      } else if (dataSource === 'insellerate') {

        insellerate(lead, account, fields).then(res => {
          secureLog("::::: Insellerate post successfull!")
          resolve(res)
          
        }).catch(e => {
          secureLog("::::: Error in Insellerate Post. Posting Warning to Rollbar. Account:", account.name)
          secureLog(e)
          reject(e)
        });
      } else {
        secureLog("::::: Posting to Velocify");
        modifyLead(account, lead, fields).then(res => {
          resolve(res)
        }).catch(e => {
          secureLog("::::: Error in Modifying Lead. Posting Warning to Rollbar. Account:", account.name)
          secureLog(e)
          reject(e)
        });
      }

    } else {
      let miscError = new ProPairError(`::::: Invalid Dynamo config! No features set to write-back. Valid Usage: {prospect_matching.triggers.use_prospect_matching: true or custom_rank.triggers.use_custom_rank: true}`, "Misc")
      reject(miscError)

    }
  });
}

function log(lead, account, accountConfig, reassignment, isActiveProspect, isRepost, data) {
  return new Promise(async (resolve, reject) => {
    try {
      console.time('LOGGING')

      let promises = []
      promises.push(logLeadMeta(lead, account, accountConfig, reassignment, isActiveProspect, data))
      promises.push(insertOrUpdateLead(lead, account, accountConfig, data))
      if (!isRepost) promises.push(logLeadIn(lead, account, isActiveProspect, data))

      await Promise.all(promises)
      console.timeEnd('LOGGING')
      resolve()

    } catch (e) {
      reject(e)
    }
  })
}

function insertOrUpdateLead(lead, customer, accountConfig, data) {
  return new Promise((resolve, reject) => {
    try {
      secureLog(`::::: Logging Lead payload`)
      var leadParams = {
        Message: JSON.stringify({ lead: data.lead ? data.lead : lead, customer: customer, accountConfig }),
        Subject: "Insert Lead",
        TopicArn: process.env.TOPIC_INSERT_LEAD
      };
      sns.publish(leadParams, function (err, data) {
        if (err) {
          let miscError = new ProPairError(err, "Misc")
          secureLog(`::::: Error logging Lead payload: ${miscError}`)
          reject(miscError);
        } else {
          secureLog(`::::: Successfully logged Lead Payload`)
          resolve()
        }
      });
    } catch (e) {
      let miscError = new ProPairError(e, "Misc")
      secureLog(`::::: Error logging Lead payload: ${miscError}`)
      reject(miscError)
    }
  })
}

function logLeadMeta(lead, account, accountConfig, reassignment, isActiveProspect, data) {
  return new Promise((resolve, reject) => {
    try {
      secureLog(`::::: Logging Lead meta data`)
      if (data.lead) {
        var leadParams = {
          Message: JSON.stringify({ lead: data.lead, reassignment, active_prospect: isActiveProspect, account, accountConfig }),
          Subject: "Log Lead",
          TopicArn: process.env.TOPIC_LOG_LEAD
        };
        sns.publish(leadParams, function (err, data) {
          if (err) {
            let miscError = new ProPairError("SNS Error " + err, "Misc")
            secureLog(`::::: Error logging Lead meta data: ${miscError}`)
            reject(miscError);
          } else {
            secureLog(`::::: Successfully logged Lead Meta Data`)
            resolve()
          }
        });
      } else {
        secureLog(`::::: Invalid data! Cannot send Log Lead Meta data`)
        resolve()
      }
    } catch (e) {
      secureLog(`::::: Error logging Lead meta data: ${e}`)
      reject(e)
    }
  });
}

function logLeadIn(lead, account, isActiveProspect, data) {
  try {
    //Log lead in
    let requestToken = generateToken(15)

    var item = {
      request_id: requestToken,
      lead_id: lead.id,
      propair_id: lead.propair_id,
      account_id: account.id,
      account_name: account.name
    }

    return new Promise((resolve, reject) => {
      secureLog(`::::: Logging Lead In`)

      item['sre_timestamps'] = data.sre_timestamps ? data.sre_timestamps : null
      item['recommendation'] = data.lead ? data.lead.trimmedRecommendation : null
      item['custom_rank_bin'] = data.lead ? data.lead.custom_rank_bin : null
      item['error'] = data.lead ? 0 : 1

      var leadParams = {
        Message: JSON.stringify({ lead: item, account, isActiveProspect }),
        Subject: "Log Lead In",
        TopicArn: process.env.TOPIC_LOG_LEAD_IN
      };

      sns.publish(leadParams, function (err, data) {
        if (err) {
          let miscError = new ProPairError("SNS Error " + err, "Misc")
          secureLog(`::::: Error logging SRE lead_in: ${miscError}`)
          reject(miscError);
        } else {
          secureLog(`::::: Successfully logged SRE lead_in`)
          resolve()
        }
      });

    });

  } catch (error) {
    secureLog(`::::: Error logging lead ${error.message ? error.message : error}`);
    throw (error);
  }
}

async function sendActiveProspectTimeToCloudwatch(account, timestamp) {
  let dimensions = [
    {
      Name: "Environment",
      Value: process.env.ENV
    }
  ]
  await createMetric(account, dimensions, timestamp, "ActiveProspect")
}


function extractRecAndVersion(recString) {

  let recommendation = '';
  let version = '';

  const recStringSplit = recString.split(':');

  if (_.includes(recStringSplit[recStringSplit.length - 2], 'CR')) {
    version = `:${recStringSplit[recStringSplit.length - 2]}:${recStringSplit[recStringSplit.length - 1]}`;
    recStringSplit.pop();
    recStringSplit.pop();
  } else {
    version = `:${recStringSplit[recStringSplit.length - 1]}`;
    recStringSplit.pop();
  }

  recommendation = recStringSplit.join(':');

  return { recommendation, version };
}

async function sendTotalTimes(value, metricName) {
  let dimensions = [
    {
      Name: "Environment",
      Value: process.env.ENV
    }
  ]
  await createMetric(metricName, dimensions, value, "RecommendationTimes")
}


exports.handler = async (event, context, callback) => {
  // This will allow us to freeze open connections to a database
  context.callbackWaitsForEmptyEventLoop = false;

  console.log("::::: Lead Creation Service Starting");

  const initialTime = moment()
  let timestamp = moment().diff(initialTime)

  if (typeof event.body === 'undefined' || JSON.parse(event.body).lead.payload.id === "1") {
    // Just a ping to keep the function warm, ignoring.
    console.log('PING');
    return {
      statusCode: 200,
      headers: {},
      body: JSON.stringify({ status: "PONG" })
    }
  }
  const eventBody = JSON.parse(event.body);


  let lead = eventBody.lead.payload
  let sre_timestamps = {}
  const isQa = eventBody.isQa;
  const isRepost = eventBody.isRepost;
  const isIncontact = lead.incontact ? true : false
  let reassignment = false;

  let availability = null;
  let account = null;
  let accountConfig = null;

  // Set Active Prospect Variables
  const isActiveProspect = typeof lead['ActiveProspect'] === 'undefined' ? false : true
  const pp_id = isActiveProspect || typeof lead.id === "undefined" ? generateToken(20) : null
  const leadID = lead.id ? lead.id : pp_id

  lead['id'] = typeof lead.id === 'undefined' ? null : lead.id
  lead['propair_id'] = typeof lead['ProPairId'] === 'undefined' ? null : lead['ProPairId']


  sre_timestamps.isColdStart = coldStart
  if (coldStart) coldStart = false

  if (eventBody.availability && eventBody.availability.payload) {
    availability = eventBody.availability.payload;
  }

  try {
    timestamp = moment().diff(initialTime)

    account = await authenticate(event);

    console.log(`::::: [TIMESTAMP] Authentication: ${moment().diff(initialTime) - timestamp}ms`)
    timestamp = moment().diff(initialTime)


    accountConfig = await getConfigurationById(account.id);
    const writeLeadBack = typeof accountConfig['write_to_velocify'] === 'undefined' || isActiveProspect ? false : accountConfig['write_to_velocify']
    const dataSource = accountConfig.data_source ? accountConfig.data_source : "velocify"

    console.log(`::::: [TIMESTAMP] Account Configuration: ${moment().diff(initialTime) - timestamp}ms`)
    timestamp = moment().diff(initialTime)


    secureLog(":::: Initializing secure logging", null, accountConfig, leadID)
    secureLog('::::: Lead', lead);
    secureLog(`::::: Data Source: ${dataSource}`)
    secureLog('::::: Agent availability', availability);

    if (isActiveProspect) {
      secureLog("::::: This is an Active Prospect Lead")

      if (dataSource === "velocify") {
        const campaigns = await getAccountCampaigns(account)

        let accountCampaignsCache = moment().diff(initialTime) - timestamp
        sendActiveProspectTimeToCloudwatch(account.name, accountCampaignsCache)

        const campaign = campaigns[lead['campaignId']]

        if (typeof lead['campaignGroup'] === 'undefined') {
          if (campaign) {
            if (typeof campaign['CampaignGroupTitle'] !== 'undefined') {
              secureLog(`::::: Retreived campaginGroup: ${campaign['CampaignGroupTitle']} from campaign ID ${lead['campaignId']}`)
              lead['campaignGroup'] = campaign['CampaignGroupTitle']
            } else {
              secureLog(`::::: No campaignGroup found for campaign id ${lead['campaignId']}. Setting to null.`)
              lead['campaignGroup'] = ''
            }
          } else {
            throw new ProPairError(`Could not find campaign id ${lead['campaignId']} in Velocify`, "Misc")
          }
        }

        if (typeof lead['campaign'] === 'undefined') {
          if (campaign) {
            if (typeof campaign['CampaignTitle'] !== 'undefined') {
              secureLog(`::::: Retreived campagin: ${campaign['CampaignTitle']} from campaign ID ${lead['campaignId']}`)
              lead['campaign'] = campaign['CampaignTitle']
            } else {
              secureLog(`::::: No campaign found for campaign id ${lead['campaignId']}. Setting to null.`)
              lead['campaign'] = ''
            }
          } else {
            throw new ProPairError(`Could not find campaign id ${lead['campaignId']} in Velocify`, "Misc")
          }
        }

        secureLog(`::::: [TIMESTAMP] GetCampaigns: ${moment().diff(initialTime) - timestamp}ms`)
        timestamp = moment().diff(initialTime)
      } else {
        secureLog(`::::: Data Source is: ${dataSource}. Skipping Campaign Retrieval`)
      }
    }


    let mapResult = await map(lead, account.name, availability, accountConfig, pp_id);

    secureLog(`::::: [TIMESTAMP] Map: ${moment().diff(initialTime) - timestamp}ms`)
    sre_timestamps.map = moment().diff(initialTime) - timestamp
    if (mapResult.map.coldstart) sre_timestamps.isColdStart = true
    timestamp = moment().diff(initialTime)

    let leadPayload;
    let recommendationString;
    let probabilities;

    if (isIncontact && availability) {
      secureLog(`::::: This is an inContact Lead`)
      const dbLead = await getLead(account, mapResult.map['account_lead_id']);

      if (dbLead && dbLead.payload) {
        secureLog(`::::: Found Lead... retrieving payload`)
        try {
          let test = JSON.parse(dbLead.payload);
          if (test && typeof test === 'object') {
            leadPayload = test;
            recommendationString = leadPayload.agentOrderRecommendation ? leadPayload.agentOrderRecommendation : null;
            probabilities = leadPayload.agentOrderProbabilities ? leadPayload.agentOrderProbabilities : null;
          }
        } catch (e) {
          secureLog(`::::: Error retrieving payload from lead: ${e}`)
        }
      } else {
        secureLog(`::::: Unable to find inContact lead: ${lead.id}`)
      }

      secureLog(`::::: [TIMESTAMP] Get Lead: ${moment().diff(initialTime) - timestamp}ms`)
      timestamp = moment().diff(initialTime)
    }

    const recommendResult = await recommend(
      lead,
      account.name,
      mapResult.map,
      accountConfig,
      mapResult.availability,
      recommendationString,
      probabilities,
      leadPayload,
      isActiveProspect
    );

    secureLog(`::::: [TIMESTAMP] Recommendation: ${moment().diff(initialTime) - timestamp}ms`)
    sre_timestamps.recommend = moment().diff(initialTime) - timestamp
    if (recommendResult.coldstart) sre_timestamps.isColdStart = true;
    timestamp = moment().diff(initialTime)



    const { recommendation, custom_rank, custom_rank_bin } = recommendResult.lead;

    if (isQa) {

      secureLog('::::: QA request, no need to continue...');

      const response = {
        statusCode: 200,
        headers: {},
        body: JSON.stringify({
          status: 'ok',
          recommendResult
        })
      };

      secureLog("::::: RESPONSE", response)
      callback(null, response);

      return;
    } else if (recommendationString) {
      secureLog('::::: Lead already recommended, nothing more to do.');
      reassignment = true;

    } else {
      let totalRecommendTime = moment().diff(initialTime)
      sendTotalTimes(totalRecommendTime, "RecommendationTotalTime")
      if (writeLeadBack && lead.id) {
        await velocify(recommendResult, account, null, mapResult.map, accountConfig);
        secureLog(`::::: [TIMESTAMP] Lead Update: ${moment().diff(initialTime) - timestamp}ms`)
        let totalRecommendTimeWithWriteBack = moment().diff(initialTime)
        sendTotalTimes(totalRecommendTimeWithWriteBack, "RecommendVelocifyTotalTime")
        sre_timestamps.velocify = moment().diff(initialTime) - timestamp
        timestamp = moment().diff(initialTime)
      } else {
        secureLog("::::: Lead Update is disabled or is Active Prospect. Skipping..")
      }

      secureLog("All done");
      secureLog("Lambda RecString: " + lead.recommendation);
      secureLog("Lambda Probabilities: " + JSON.stringify(lead.probabilities));
      secureLog("Lambda Features: " + JSON.stringify(lead.features));
    }

    let body;

    if (isActiveProspect) {
      body = {
        custom_rank_bin: recommendResult.lead.custom_rank_bin,
        recommendation: recommendResult.lead.trimmedRecommendation,
        propair_id: pp_id,
        status: 'ok'
      }
    } else {
      body = recommendation ? { ...extractRecAndVersion(recommendation), status: 'ok', lead: recommendResult.lead } : { status: 'ok', lead: recommendResult.lead }
    }

    const response = {
      statusCode: 200,
      headers: {},
      body: JSON.stringify(body)
    };

    secureLog('::::: Response', response);

    // emitting lead results
    await log(lead, account, accountConfig, reassignment, isActiveProspect, isRepost, { sre_timestamps, lead: recommendResult.lead })

    callback(null, response);

  } catch (e) {
    secureLog(":::::::::: GOT AN ERROR TO REPORT :::::::::::::::");
    secureLog(e)
    console.log(e.stack)

    if (!isQa) await log(lead, account, accountConfig, reassignment, isActiveProspect, isRepost, {})

    if (typeof account !== 'undefined' && account != null) {
      e.lead = lead
      e.account = account && account.name
      e.account_id = account && account.id
    }
    else if (typeof lead !== 'undefined' && lead != null) {
      e.lead = { "id": lead.id }
    }

    await reportToRollbar(e, process.env.TOPIC_ROLLBAR, "Create")

    callback(null, {
      statusCode: e.code || 500,
      headers: {},
      body: JSON.stringify({ status: e.message })
    });
  }
};
