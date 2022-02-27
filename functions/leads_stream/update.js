'use strict';
var AWS = require('aws-sdk');
AWS.config.update({ region: 'us-west-1' });

let lambda = new AWS.Lambda({ region: 'us-west-1' });
let s3 = new AWS.S3({ region: 'us-west-1' });
let sns = new AWS.SNS();

const axios = require('axios').default;
let _ = require('lodash');
const moment = require('moment');
var kafka = require('kafka-node'),
    Producer = kafka.Producer;

const { getKafkaClient, getKafkaAvroClient, parseAvroData } = require('pplibs/kafkaUtils')

// Global Variables
let globalAccounts;
let globalMap = {};
let agentMaps = {};
let creditLookups = {};

const ssm = new AWS.SSM({ apiVersion: '2014-11-06' });
// Utilities
const { executeQueryOnRedshiftRawConn, cast } = require('pplibs/dbUtils')
const { encrypt, decrypt } = require('pplibs/mainUtils')
const { retrieveCache } = require('pplibs/redisUtils')
const { secureLog, getEncryptionPassword, getConfigurationById } = require('pplibs/logUtils')
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

            let account = globalAccounts[Object.keys(globalAccounts).find(key => decrypt(globalAccounts[key].token, encPassword) === authKey)];

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

async function fetchAgentMap(account) {
    if (typeof agentMaps[account.id] !== 'undefined') {
        return agentMaps[account.id]
    } else {
        secureLog("::::: Local Cache Expired; Retrieving agent map data")
        const unique_key = `agent_id_map_${account.name}`
        let res = await retrieveCache(unique_key, () =>
            executeQueryOnRedshiftRawConn(`SELECT id as propair_id, agent_id, email, name_velocify from agent_profiles WHERE account_id=${account.id} and email is not null;`)
        );

        if (res.rows.length > 0) {
            agentMaps[account.id] = _.keyBy(res.rows, i => i.email.toUpperCase())
            return agentMaps[account.id]
        }

    }

}
async function map(lead, account) {
    if (typeof globalMap[account.name] === 'undefined') {
        secureLog("::::: Local Cache Expired; Retrieving global attr mapping data")
        const unique_key = `leads_stream_mapping_${account.name}`
        let res = await retrieveCache(unique_key, () => executeQueryOnRedshiftRawConn(`
            select propair_field, customer_field_name, datatype, table_name from global_attribute_lookup 
            where account_id=${account.id} and account_system='insellerate' and table_name in ('lead', 'lead_detail', 'agent_profiles') 
            and customer_field_name is not null;
            `))

        if (res.rows.length > 0) {
            globalMap[account.name] = res.rows
        } else {
            throw "Missing Global Atrribute mapping data for Insellerate"
        }
    }


    var attrMap = globalMap[account.name]
    var leadMap = {}

    attrMap.forEach(map => {
        if (!leadMap[map['table_name']]) leadMap[map['table_name']] = { account_id: account.id }

        if (map['customer_field_name'] in lead) {
            leadMap[map['table_name']][map['propair_field']] = cast(lead[map['customer_field_name']], map['propair_field'], map['datatype']);
        } else if (map['customer_field_name'] ? map['customer_field_name'].includes(':') : false) {
            let components = map['customer_field_name'].split(':')
            if (components[0] in lead) {
                const values = lead[components[0]].split(map['customer_split'])
                if (values.length > parseInt(components[1], 10)) {
                    leadMap[map['table_name']][map['propair_field']] = cast(values[parseInt(components[1], 10)], map['propair_field'], map['datatype']);
                }
            }
        } else if (map['customer_original_field_name'] in lead) {
            leadMap[map['table_name']][map['propair_field']] = cast(lead[map['customer_original_field_name']], map['propair_field'], map['datatype']);;
        }
    })

    return leadMap

}

function calculateCredit(score) {
    let creditProfile = "";
    let creditScore = String(score).replace(/[^0-9]/gi, "")
    let creditScoreStr = String(score).replace(/[^a-zA-Z]/gi, "")
    if (creditScore != "") {
        if (creditScore.length == 3 || creditScore.length == 6) {
            //If credit score is above 1000, calculate the average 
            creditScore = parseInt(creditScore, 10) < 1000 ? parseInt(creditScore, 10) : (parseInt(creditScore.slice(0, 3), 10) + parseInt(creditScore.slice(3, 6), 10)) / 2

            if (creditScore > 0) creditProfile = 'Poor'
            if (creditScore >= 620) creditProfile = 'Fair'
            if (creditScore >= 660) creditProfile = 'Good'
            if (creditScore >= 720) creditProfile = 'Excellent'

        }
    } else if (creditScoreStr != "") {
        creditScore = String(score).replace(/[^a-zA-Z]/gi, "")
        creditScore = creditScore.toUpperCase();

        if (creditScore == "POOR") creditProfile = 'Poor'
        if (creditScore == "FAIR") creditProfile = 'Fair'
        if (creditScore == "GOOD") creditProfile = 'Good'
        if (creditScore == "EXCELLENT") creditProfile = 'Excellent'
    }
    return creditProfile;
}

function searchInLookup(value, mapping) {
    let binResult = mapping[String(value).replace(/[^a-zA-Z0-9.+@]/gi, "").toUpperCase()]
    return (typeof binResult !== "undefined" && binResult != null) ? binResult : undefined
}

async function fetchCreditLookup(account) {
    if (typeof creditLookups[account.name] === 'undefined') {
        secureLog("::::: Local Cache Expired; Retrieving credit lookup mapping data")
        let unique_key = `credit_lookup_${account.name}`
        let res = await retrieveCache(unique_key, () =>
            executeQueryOnRedshiftRawConn(`select a.name, source, credit_profile_stated, "bin_credit_profile_stated" from credit_profile_stated_lookup p inner join accounts a on p.account_id = a.id`)
        );

        let grouped = _.groupBy(res.rows, i => i.name);
        Object.keys(grouped).forEach(account => {
            let set = new Set()
            let unique_lookups = {}
            grouped[account].forEach(o => {
                var unique_key = `${o.credit_profile_stated}_${o.bin_credit_profile_stated}`
                if (!set.has(unique_key)) {
                    set.add(unique_key)
                    unique_lookups[o.credit_profile_stated] = o.bin_credit_profile_stated
                }
            })
            grouped[account] = unique_lookups

        })
        
        creditLookups = grouped
        return creditLookups[account.name]
    } else {
        return creditLookups[account.name]
    }
}

function getBinCreditProfile(lead, priority_list, lookup) {
    let result = null
    if (typeof priority_list === "undefined" || !priority_list) {
        priority_list = ["credit_score_stated", "credit_score_range_stated", "credit_profile_stated"]
    }

    for (const element of priority_list) {

        let value = lead[element]

        if (typeof value !== "undefined" && value !== '' && value !== null) {

            result = calculateCredit(value)

            if (!result || typeof result === 'undefined') {
                secureLog(`::::: ${element.toUpperCase()} NOT A NUMBER; FETCHING FROM LOOKUP`)

                result = searchInLookup(lead[element], lookup)
                if (result != null || typeof result !== 'undefined') {
                    break
                }

            }

            if (!result || typeof result === 'undefined') {
                secureLog(`::::: COULD NOT PARSE ${element.toUpperCase()}`)
                result = null
                continue
            } else {
                break
            }
        } else {
            secureLog(`::::: MISSING VALUE FOR ${element.toUpperCase()}`)
            continue
        }
    }
    return result
}
function parseName(name) {
    let Regx1 = /\w+ \w+/gi
    let Regx2 = /\w+,\s?\w+/gi
    
    if (Regx1.test(name)) {
        let nameParts = name.trim().split(' ');
        return {first: nameParts[0], last: nameParts[1]}
    } else if (Regx2.test(name)) {
        let nameParts = name.trim().split(',');
        return {first: nameParts[1], last: nameParts[0]} 
    } else {
        return {first: null, last: null}
    }
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

exports.handler = async (event, context, callback) => {
    // This will allow us to freeze open connections to a database
    context.callbackWaitsForEmptyEventLoop = false;
    var account;

    try {
        account = await authenticate(event);
        let accountConfig = await getConfigurationById(account.id);
        
        let dateNow = moment().format('YYYY-MM-DD HH:mm:ss');
        const eventBody = JSON.parse(event.body);
        let lead = eventBody.lead.payload

        secureLog(":::: Initializing secure logging", null, accountConfig, lead.id)
        secureLog(lead)

        let mapRes = await map(lead, account)
        let normalizedLead = mapRes.lead
        let normalizedLeadDetail = mapRes.lead_detail
        let agentProfile = mapRes.agent_profiles

        normalizedLead.account_lead_id = normalizedLeadDetail.account_lead_id = lead.id

        const agentIdMap = await fetchAgentMap(account)

        // Match propair agent id on email
        if (agentProfile.original_agent_id) {
            agentProfile.agent_id = agentProfile.original_agent_id
            agentProfile.email = agentProfile.original_agent_email
            agentProfile.name = agentProfile.original_agent_name
            
            secureLog(`::::: Detected First Assignment. Original Agent: ${agentProfile.agent_id}`)

            let {first, last} = parseName(agentProfile.name)
            agentProfile.first_name = first
            agentProfile.last_name = last

            let match = agentIdMap[agentProfile.original_agent_email.toString().toUpperCase()]
            if (match) {
                normalizedLeadDetail['profile_id_first_assignment_user'] = match.propair_id
                normalizedLeadDetail['profile_id_loan_officer_default'] = match.propair_id
                secureLog(`::::: first_assignment, loan_officer_default = (${normalizedLeadDetail['profile_id_first_assignment_user']}, ${normalizedLeadDetail['profile_id_loan_officer_default']})`)
            } else {
                secureLog((`::::: Warning! No match found for original agent ${agentProfile.agent_id}`))
            }
        }

        if (agentProfile.current_agent_id) {
            agentProfile.agent_id = agentProfile.current_agent_id
            agentProfile.email = agentProfile.current_agent_email
            agentProfile.name = agentProfile.current_agent_name
            
            secureLog(`::::: Detected Reassignment. Current Agent: ${agentProfile.agent_id}`)
            
            let {first, last} = parseName(agentProfile.name)
            agentProfile.first_name = first
            agentProfile.last_name = last
    
            let match = agentIdMap[agentProfile.current_agent_email.toString().toUpperCase()]
            if (match) {
                normalizedLeadDetail['profile_id_user'] = match.propair_id
                normalizedLeadDetail['profile_id_loan_officer_default'] = match.propair_id
                secureLog(`::::: profile_id_user, loan_officer_default = (${normalizedLeadDetail['profile_id_user']}, ${normalizedLeadDetail['profile_id_loan_officer_default']})`)
            } else {
                secureLog((`::::: Warning! No match found for current agent ${agentProfile.agent_id}`))
            }
        }

        agentProfile.created_at = agentProfile.updated_at = dateNow

        // Add originated loan
        normalizedLeadDetail['second_loan'] = 0

        ////// Domain Matching /////
        const domainList = accountConfig["domains"]["list"]

        if (normalizedLead.email_stated) {
            let emailParts = normalizedLead.email_stated.split('@') || []
            if (emailParts.length > 1) {
                let domainParts = emailParts[1].split(".") || []
                if (domainParts.length > 0) {
                    normalizedLeadDetail.domain = domainParts[0].toUpperCase()
                    normalizedLeadDetail.domain_tld = domainParts[domainParts.length - 1]
                }
            } else if (emailParts.length > 0) {
                normalizedLeadDetail.domain = emailParts[0].toUpperCase()
                normalizedLeadDetail.domain_tld = emailParts[0].toUpperCase()
            }

            if (normalizedLeadDetail.domain) {
                normalizedLeadDetail['BIN_domain'] = domainList.includes(normalizedLeadDetail.domain) ? normalizedLeadDetail.domain : 'OTHER'
            }
        }

        if (normalizedLeadDetail.lead_datetime) {
            let date = moment(normalizedLeadDetail.lead_datetime, 'M/DD/YYYY h:m:s a');
            if (date.isValid()) {
                normalizedLeadDetail.lead_date = date.format("YYYY-MM-DD");
            } else {
                date = moment(normalizedLeadDetail.lead_datetime);
                if (date.isValid()) normalizedLeadDetail.lead_date = date.format("YYYY-MM-DD");
            }
        }

        let creditLookup = await fetchCreditLookup(account);
        let binCredit = getBinCreditProfile(normalizedLead, null, creditLookup)
        if (binCredit) normalizedLeadDetail['bin_credit_profile_stated'] = binCredit

        const kafkaClient = await getKafkaClient();
        const producer = new Producer(kafkaClient);
        
        const payloads = []
        payloads.push({ topic: 'external_leads', messages: JSON.stringify(normalizedLead) })
        payloads.push({ topic: 'external_lead_details', messages: JSON.stringify(normalizedLeadDetail) })
        payloads.push({ topic: 'agent_profiles', messages: JSON.stringify(agentProfile) })

        
        await new Promise( async (resolve, reject) => {
            while(!producer.ready) {
                await sleep(1000);
                console.log("[PING] Waiting for producer...")
            }
            producer.send(payloads, (err, data) => {
                if (err) reject(err)
                console.log(data)
                producer.close()
                resolve(data)
            })
        });

        callback(null, {
            statusCode: 200,
            headers: {},
            body: JSON.stringify({ status: 'ok' })
        });
        return

    } catch (e) {
        console.log("::::: ERROR :::::")
        console.log(e.stack || e)
        let error = new ProPairError(e, "LeadsStreamError")
        if (account) error.account = account.name

        await reportToRollbar(error, process.env.TOPIC_ROLLBAR, "LeadsStream")

        callback(null, {
            statusCode: e.code || 500,
            headers: {},
            body: JSON.stringify({ status: e.message || e })
        });
    }

}
