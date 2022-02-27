'use strict';
var AWS = require('aws-sdk');
AWS.config.update({ region: 'us-west-1' });


let lambda = new AWS.Lambda({ region: 'us-west-1' });
let sns = new AWS.SNS();
var accountsConfigurations;

let _ = require('lodash');
const soap = require('soap');
const axios = require("axios");
const moment = require("moment");

// Utilities
const { getRedshiftClient } = require('pplibs/dbUtils')
const { secureLog, getConfigurations } = require('pplibs/logUtils')
const { reportToRollbar, ProPairError } = require('pplibs/customErrors')

const reflect = p => p.then(v => ({ v, status: "fulfilled" }),
    e => ({ e, status: "rejected" }));

const delayIncrement = 500;


function getAccounts(keys) {
    secureLog(`::::: Fetching Accounts`)
    return new Promise((resolve, reject) => {
        try {
            getRedshiftClient().then(client => {
                client.connect((err) => {
                    let query = `SELECT * FROM accounts WHERE id in (${keys});`
                    client.query(query)
                        .then(res => {
                            client.close()
                            if (res.rows.length > 0) {
                                secureLog(`::::: Succesfully Fetched Accounts: (${keys})`)
                                resolve(res.rows);
                            } else {
                                let e = new ProPairError(`Accounts: (${keys}) does not exist!`, "RepostScheduler")
                                reject(e);
                            }
                        }).catch(error => {
                            let e = new ProPairError("Failed to fetch account", "RepostScheduler")
                            reject(e);
                        });
                })
            });
        } catch (e) {
            secureLog("ERROR", e);
            let error = new ProPairError("Failed to connect to database", "RepostScheduler")
            reject(error);
        }
    });
}



function getGlobalAttr() {
    secureLog(`::::: Fetching Global Attribute Lookup`)
    return new Promise((resolve, reject) => {
        try {
            getRedshiftClient().then(client => {
                client.connect((err) => {
                    if (err) { console.log(err); reject(err); }
                    let query = `
                        SELECT * FROM global_attribute_lookup 
                        WHERE table_name IN ('xml_post', 'lead', 'lead_detail');
                        `
                    client.query(query)
                        .then(res => {
                            client.close();
                            secureLog("::::: Succesfully Fetched  Global Attribute Lookup")
                            const lookups = _.reduce(res.rows, (result, value, key) => {
                                (result[value['account_id']] || (result[value['account_id']] = [])).push(value)
                                return result
                            }, {})

                            resolve(lookups);
                        }).catch(error => {
                            secureLog("ERROR", error);
                            let e = new ProPairError("Failed to fetch Global Attribute Lookup", "RepostScheduler")
                            reject(e);
                        });
                })
            });
        } catch (e) {
            secureLog("ERROR", e);
            let error = new ProPairError("Failed to connect to database", "RepostScheduler")
            reject(error);
        }
    });
}

function mapLeads(data, attrMap) {
    var leads = [];
    var fieldsToPost = attrMap.filter(item => item['table_name'] === 'xml_post').map(a => a.propair_field)

    data.forEach(lead => {

        var firstLeadMap = {}
        attrMap.filter(item => (item['table_name'] !== 'xml_post' && fieldsToPost.includes(item['propair_field']))).forEach(map => {
            if (map['customer_field_name'] in lead) {
                firstLeadMap[map['propair_field']] = lead[map['customer_field_name']];
            } else if (map['customer_field_name'] ? map['customer_field_name'].includes(':') : false) {
                let components = map['customer_field_name'].split(':')
                if (components[0] in lead) {
                    const values = lead[components[0]].split(map['customer_split'])
                    if (values.length > parseInt(components[1], 10)) {
                        firstLeadMap[map['propair_field']] = values[parseInt(components[1], 10)];
                    }
                }
            } else if (map['customer_original_field_name'] in lead) {
                firstLeadMap[map['propair_field']] = lead[map['customer_original_field_name']];
            } else {
                firstLeadMap[map['propair_field']] = ""
            }
        })

        var finalLeadMap = {}
        var splitFields;
        attrMap.filter(item => item['table_name'] === 'xml_post').forEach(map => {
            if (map['customer_field_name'] ? map['customer_field_name'].includes(':') : false) {
                if (map['propair_field'] in firstLeadMap) {
                    var split = map['customer_field_name'].split(":");
                    var customerField = split[0];
                    var val = firstLeadMap[map['propair_field']];

                    if (!splitFields) splitFields = {}
                    if (typeof splitFields[customerField] === 'undefined') splitFields[customerField] = { values: [], delimiter: map['customer_split'] }

                    splitFields[customerField]['values'][split[1]] = val;
                }
            } else if (map['propair_field'] in firstLeadMap) {
                if (map['customer_field_name'] !== null) {
                    finalLeadMap[map['customer_field_name']] = firstLeadMap[map['propair_field']];
                }
            }

        })

        if (splitFields) {
            Object.keys(splitFields).forEach(key => {
                var delimiter = splitFields[key]['delimiter']
                finalLeadMap[key] = splitFields[key]['values'].join(`${delimiter}`)
            })
        }

        leads.push(finalLeadMap)
    });

    return leads
}


function getClient() {
    return soap.createClientAsync(process.env.VELOCIFY_URL);
}

async function fetchReportId(account, configuration) {
    secureLog(`::::: Fetching Report list`, null, configuration)
    const username = account.velocify_username;
    const password = account.velocify_password;
    const payload = { username, password };

    const client = await getClient();
    const result = await client.GetReportsAsync(payload);
    const reports = result[0]['GetReportsResult']['Reports']['Report']

    var reportId;
    var startFilterId;
    var endFilterId;
    reports.forEach(report => {
        if (report['attributes']['ReportTitle'] === "ProPair Custom Rank Bin") {
            reportId = report['attributes']['ReportId']
            if (typeof report['FilterItems'] !== 'undefined') {
                if (Array.isArray(report['FilterItems']['FilterItem'])) {
                    report['FilterItems']['FilterItem'].forEach(filter => {
                        if (String(filter['attributes']['FieldTitle']).replace(/[^a-zA-Z]/gi, "").toLowerCase() === 'dateadded') {
                            if (String(filter['attributes']['Operator']).replace(/[^a-zA-Z]/gi, "").toLowerCase().includes('greater')) {
                                startFilterId = filter['attributes']['FilterItemId']
                            } else if (String(filter['attributes']['Operator']).replace(/[^a-zA-Z]/gi, "").toLowerCase().includes('less')) {
                                endFilterId = filter['attributes']['FilterItemId']
                            }
                        }
                    })
                } else {
                    var filter = report['FilterItems']['FilterItem']
                    if (String(filter['attributes']['FieldTitle']).replace(/[^a-zA-Z]/gi, "").toLowerCase() === 'dateadded') {
                        if (String(filter['attributes']['Operator']).replace(/[^a-zA-Z]/gi, "").toLowerCase().includes('greater')) {
                            startFilterId = filter['attributes']['FilterItemId']
                        } else if (String(filter['attributes']['Operator']).replace(/[^a-zA-Z]/gi, "").toLowerCase().includes('less')) {
                            endFilterId = filter['attributes']['FilterItemId']
                        }
                    }
                }
            } else {
                let error = new ProPairError(`[${account.name}] No filters found for this account`, "VelocifyException")
                throw error
            }
        }
    })
    
    if (!startFilterId) throw new ProPairError(`[${account.name}] Unable to find filter: Date Greater Than`, "VelocifyException")

    if (!reportId) throw new ProPairError(`[${account.name}] Unable to find report ProPair Custom Rank Bin`, "VelocifyException")

    return { startFilterId, endFilterId, reportId }
    
}

async function fetchReport(reportId, account, filters, configuration) {
    secureLog(`::::: Fetching Report`, null, configuration)

    const username = account.velocify_username;
    const password = account.velocify_password;

    const payload = {
        username,
        password,
        reportId,
        templateValues: {
            FilterItems: {
                FilterItem: filters.map(f => {
                    return {
                        attributes: {
                            FilterItemId: f.id
                        },
                        $value: f.value

                    }
                })
            }
        }
    }

    const client = await getClient();
    const result = await client.GetReportResultsAsync(payload);

    return result[0]['GetReportResultsResult']['ReportResults']['Result']
}



function executeLeadsApi(lead, account, total, count, configuration) {
    // Increment delay for each lead so they will trigger in intervals.
    count['delay'] += delayIncrement;

    const headers = {
        Authorization: `Bearer ${account.token}`
    };
    const body = {
        lead: {
            payload: lead
        },
        isRepost: true
    };



    return new Promise((resolve, reject) => {
        new Promise(resolve => setTimeout(resolve, count['delay'])).then(() => {
            count['num']++
            //secureLog(`::::: Processing lead: ${lead.id}`)
            if (count['num'] % 100 === 0) secureLog(`::::: Processing lead ${count['num']} out of ${total}`, null, configuration)
            var result = axios.post(process.env.LEADS_ENDPOINT, body, { headers })
            resolve(result);
        })
    });
}

async function processLeads(leads, account, configuration) {
    secureLog(`::::: Processing ${leads.length} failed leads`, null, configuration)
    var promises = []
    var count = { num: 0, delay: 0 }

    try {
        leads.forEach(lead => {
            promises.push(executeLeadsApi(lead, account, leads.length, count, configuration));
        })

        const result = await Promise.all(promises.map(reflect))
        const failed = result.filter(item => item.status === 'rejected');
        const succeeded = result.filter(item => item.status === 'fulfilled');
        if (failed) {
            failed.forEach(failure => {
                if (typeof failure.e.response !== 'undefined') {
                    secureLog("ERROR", failure.e.response.data, configuration)
                } else {
                    secureLog("ERROR", failure.e.message, configuration)
                }
            })
        }
        secureLog(`[${account.name}] PASSED: ${succeeded ? succeeded.length : 0}, FAILED: ${failed ? failed.length : 0}`)
        if (succeeded) {
            let leadsWithCustomRank = {}

            succeeded.forEach(item => {
                let lead = item.v.data.lead
                if (lead.custom_rank !== 0 && lead.custom_rank) {
                    leadsWithCustomRank[lead.id] = lead
                }
            })
            var rate = Math.round((Object.keys(leadsWithCustomRank).length / leads.length) * 100)
            secureLog(`::::: Total Reposted Leads ${leads.length}`, null, configuration)
            secureLog(`::::: Custom Rank = NULL: ${failed ? failed.length : 0}`, null, configuration)
            secureLog(`::::: Custom Rank = 0: ${succeeded.length - Object.keys(leadsWithCustomRank).length}`, null, configuration)
            secureLog(`::::: Custom Rank = Value: ${Object.keys(leadsWithCustomRank).length}`, null, configuration)

            secureLog(`::::: Repost Success Rate ${rate}%`, null, configuration)
            secureLog(`::::: Leads:`, leads, configuration)
            if (!_.isEmpty(leadsWithCustomRank)) secureLog("::::: Passed Leads With Custom Rank:", leadsWithCustomRank, configuration)

            return leadsWithCustomRank
        } else {
            throw new ProPairError("All leads failed to post", "RepostScheduler")
        }
    } catch (e) {
        secureLog("ERROR", e)
        throw e
    }
}

async function invokeRepost(account, lookup, configuration, startDate, endDate, runTest, callback) {
    try {
        // Need to pass configuration into each secureLog in order to give the right account
        secureLog(`::::: Processing account: ${account.name}`, null, configuration)
        const { startFilterId, endFilterId, reportId } = await fetchReportId(account, configuration);

        let filters = [
            { id: startFilterId, value: startDate }
        ]
        if (endFilterId) filters.push({ id: endFilterId, value: endDate })

        const report = await fetchReport(reportId, account, filters, configuration);
        const threshold = configuration['repost_failure_threshold']

        if (report) {

            let failedLeads = report.filter(item => (typeof item['ProPairCustomRank'] === "undefined" || [0, "0", null].includes(item['ProPairCustomRank'])))
            let succeededLeads = report.filter(item => (typeof item['ProPairCustomRank'] !== "undefined" && ![0, "0", null].includes(item['ProPairCustomRank'])))
            failedLeads = await mapLeads(failedLeads, lookup)
            succeededLeads = await mapLeads(succeededLeads, lookup)

            secureLog(`::::: Total Leads ${report.length}`, null, configuration)
            secureLog(`::::: Leads with custom rank ${succeededLeads.length}`, null, configuration)
            secureLog(`::::: Leads with no custom rank ${failedLeads.length}`, null, configuration)

            const sampleSize = typeof configuration['repost_sample_size'] !== "undefined" ? configuration['repost_sample_size'] : 50
            const leadsSample = _.sampleSize(succeededLeads, sampleSize)
            
            let customRankTest;
            if (runTest) {
                secureLog(`::::: Running custom rank test for ${leadsSample.length} leads. Min Sample Size: ${sampleSize}`, null, configuration)
                customRankTest = await new Promise((resolve, reject) => {
                    const payload = JSON.stringify({ account, leads: leadsSample, config: configuration });
                    lambda.invoke({
                        FunctionName: process.env.FUNC_REPOST,
                        Payload: payload
                    }, function (error, data) {
                        if (error) {
                            throw error;
                        } else {
                            const response = JSON.parse(data.Payload)
                            const eventBody = JSON.parse(response.body)
                            if (response.statusCode === 500) {
                                var er = { type: "error", name: "Custom Rank Test Failure", result: eventBody.result, message: eventBody.message, leads: leadsSample, summaries: eventBody.summaries }
                                secureLog(er.message, null, configuration)
                                secureLog(`:::: FAILURE THRESHOLD ${threshold * 100}%`, null, configuration)
                                secureLog(`:::: FAILED LEADS:`, er.result.failed.length, configuration)
                                secureLog(`:::: SUCCESSFULL LEADS:`, er.result.failed.length, configuration)
                                secureLog(`:::: SAMPLE OF 3 LEADS:`, _.sampleSize(Object.keys(er.leads), 3), configuration)
                                secureLog(`:::: SAMPLE OF 3 FAILED LEADS:`, _.sampleSize(er.result.failed, 3), configuration)
                                secureLog(`:::: SAMPLE OF 3 SUCCESSFULL LEADS:`, _.sampleSize(er.result.passed, 3), configuration)
                                let error = new ProPairError(eventBody.message, "RepostScheduler")
                                error.leads = leadsSample
                                error.summaries = eventBody.summaries
                                error.result = eventBody.result
                                reject(error)
                            } else {
                                secureLog(`::::: Test successfull! Failure Rate: ${eventBody.results.failureRate}%, Failure Threshold: ${threshold * 100}%`, null, configuration);
                                resolve(eventBody.results)
                            }
                        }
                    });
                })
            } else {
                customRankTest = true;
            }

            if (customRankTest) {
                if (failedLeads) {
                    secureLog(`::::: Reposting Leads with failed custom rank values`, null, configuration)
                    const repostResults = await processLeads(failedLeads, account, configuration)
                    return repostResults
                } else {
                    secureLog(`::::: Did not find any failed custom rank values.. Exiting`, null, configuration)
                }
            }

        } else {
            secureLog(`:::: Report ID: ${reportId} returned 0 leads for Account: ${account.id}`, null, configuration)
        }
    } catch (e) {
        secureLog(":::::::::: GOT AN ERROR TO REPORT :::::::::::::::");
        secureLog("::::: ACCOUNT: ", account.name, configuration)
        secureLog(e, null, configuration);
        if (typeof account !== 'undefined' && account != null) {
            e.account = account && account.name
          }
        
        if (typeof e.message === 'undefined' || e.message == null) {
            let message = e.message ? e.message : 'Other'
            let err = new ProPairError(message, "RepostScheduler")
            e = err
        }

        reportToRollbar(e, process.env.TOPIC_ROLLBAR, "RepostScheduler")

        callback(null, {
            statusCode: 500,
            headers: {},
            body: JSON.stringify(e)
        });
    }
}

exports.handler = async (event, context, callback) => {
    console.log("::::: Repost Scheduler starting")
    try {


        const configurations = await getConfigurations()
        var accountsToRepost;
        secureLog("::::: Initializing Secure Logging", null, configurations[1])

        if (process.env.ENV === "staging") {
            accountsToRepost = _.pickBy(configurations, a => a.id === 10)
        } else {
            accountsToRepost = _.pickBy(configurations, a => _.get(a, 'repost.triggers.use_repost') === true)
        }

        if (_.isEmpty(accountsToRepost) || !accountsToRepost) {
            secureLog("::::: No accounts set to repost. Exiting...")
        } else {
            const accounts = await getAccounts(Object.keys(accountsToRepost))
            const lookup = await getGlobalAttr()
            var promises = [];
            accounts.forEach(account => {
                let config = configurations[account.id]
                let hours = config["repost_hours_from_now"] ? config["repost_hours_from_now"] : 48
                let runTest = true;

                const startDate = event.start_date ? moment(event.start_date, 'YYYY-M-D hh:mm:ss').format("M/D/YYYY hh:mm a") : moment().utcOffset(-8).subtract(hours + 2, "hours").format("M/D/YYYY hh:mm a")
                const endDate = event.end_date ? moment(event.end_date, 'YYYY-M-D hh:mm:ss').format("M/D/YYYY hh:mm a") : moment().utcOffset(-8).format("M/D/YYYY hh:mm a")

                // Turn off test if running repost manually
                if (event.start_date || event.end_date) runTest = false;

                secureLog('::::: Fetching leads from...')
                secureLog(`::::: Start Date: ${startDate}`)
                secureLog(`::::: End Date: ${endDate}`)

                if (moment(startDate, "M/D/YYYY hh:mm a") < moment(endDate, "M/D/YYYY hh:mm a")) {
                    promises.push(invokeRepost(account, lookup[account.id], config, startDate, endDate, runTest, callback))
                } else {
                    throw new Error(`Invalid Dates! End date must be greater than start date`)
                }
            })

            const result = await Promise.all(promises.map(reflect))
            secureLog(":::: ALL DONE")
        }
    } catch (e) {
        secureLog(e)
    }
}