'use strict';
var AWS = require('aws-sdk');
AWS.config.update({ region: 'us-west-1' });
var moment = require('moment-timezone');


var juice = require('juice');
const pug = require('pug');


//Utilities
const { getRedshiftClient, executeQueryOnRedshiftRawConn } = require('pplibs/dbUtils')
const { secureLog, getConfigurationById } = require('pplibs/logUtils')
const { createMetric } = require('pplibs/metrics_util');
const { capitalize } = require('lodash');

async function fetchData(query) {
    var result = await executeQueryOnRedshiftRawConn(query)

    return result.rows
}

function calculateTimestamps(times) {
    var result = []

    Object.keys(times).forEach(key => {
        let arr = times[key]
        let sum = arr.reduce((sum, val) => (sum += val));
        let len = arr.length;

        let arrSort = arr.sort();
        let mid = Math.ceil(len / 2);

        let average = Math.floor(sum / len);
        let median =
            len % 2 == 0 ? (arrSort[mid] + arrSort[mid - 1]) / 2 : arrSort[mid - 1];
        var min = Math.min.apply(null, arr),
            max = Math.max.apply(null, arr);

        result.push({ name: key, data: [median, average, min, max] })
    })

    return result
}

function extractData(data, model) {
    var allAccounts = data.map(x => x.account_name).filter((v, i, a) => a.indexOf(v) === i);

    // Templates
    var sre_timestamps = {}
    var custom_rank_template = {}
    var prospect_match_template = {}
    var totals = {}

    allAccounts.forEach(a => {
        custom_rank_template[a] = {
            0: 0,
            1: 0,
            2: 0,
            3: 0,
            4: 0,
            5: 0,
            6: 0,
            7: 0
        }

        prospect_match_template[a] = {
            recommendation: 0,
            random: 0,
            error: 0
        }

        totals[a] = {
            processed: 0,
            errors: 0
        }
    })

    var total = data.length
    var errors = data.filter(f => f.error === 1 || f.error === null).length

    let tables = {
        result: {
            title: 'Result',
            columns: ['Account', 'Processed', 'Critical Errors'],
            values: totals
        }
    }

    data.forEach(item => {

        if (item.error || item.error === null) {
            tables.result.values[item.account_name].errors++;
        } else {
            tables.result.values[item.account_name].processed++;

            let timestamps = item.sre_timestamps ? JSON.parse(item.sre_timestamps.replace(/'/gi, "\"").replace(/True/gi, "true").replace(/False/gi, "false")) : null
            if (timestamps && !timestamps.isColdStart) {
                Object.keys(timestamps).slice(1).forEach(k => {
                    if (!sre_timestamps[k]) {
                        sre_timestamps[k] = {
                            title: capitalize(k) + ' Timestamps (in milliseconds)',
                            values: {}
                        }
                    }

                    if (!sre_timestamps[k].values[item.account_name]) sre_timestamps[k].values[item.account_name] = []

                    sre_timestamps[k].values[item.account_name].push(timestamps[k])
                })
            }


            if (item.custom_rank_bin !== null) {
                var tableKey = 'custom_rank'
                var tableTitle = 'Custom Rank (in Bins)'
                if (item.custom_rank_timeframe) {
                    tableKey = `custom_rank_${item.custom_rank_timeframe}`
                    tableTitle = `${item.custom_rank_timeframe} day Custom Rank`
                }
                if (!tables[tableKey]) {
                    tables[tableKey] = {
                        title: tableTitle,
                        count: 0,
                        columns: ['Account', 0, 1, 2, 3, 4, 5, 6, 7],
                        values: JSON.parse(JSON.stringify(custom_rank_template))
                    }
                }

                if (typeof tables[tableKey].values[item.account_name][item.custom_rank_bin] !== 'undefined') {
                    tables[tableKey].values[item.account_name][item.custom_rank_bin]++;
                } else {
                    var sliced = tables[tableKey].columns.slice(1)
                    sliced.push(item.custom_rank_bin)
                    tables[tableKey].columns = [tables[tableKey].columns[0], ...sliced.sort((a, b) => a - b)]

                    tables[tableKey].values[item.account_name][item.custom_rank_bin] = 1
                }
                tables[tableKey].count++;
            }

            if (item.rec_result !== null) {
                var tableKey = 'prospect_match'
                var tableTitle = 'Prospect Matches'

                if (!tables[tableKey]) {
                    tables[tableKey] = {
                        title: tableTitle,
                        count: 0,
                        columns: ['Account', 'Recommendations', 'Randoms', 'Errors (000y)'],
                        values: JSON.parse(JSON.stringify(prospect_match_template))
                    }
                }

                if (typeof tables[tableKey].values[item.account_name][item.rec_result.toLowerCase()] !== 'undefined') {
                    tables[tableKey].values[item.account_name][item.rec_result.toLowerCase()]++;
                    tables[tableKey].count++;
                }
            }
        }
    })


    var result = {
        model,
        title: model,
        summaries: [
            { key: 'Total Leads Recieved', value: total },
            { key: 'Total Leads Processed', value: total - errors },
            { key: 'Critical Failures', value: errors }
        ]
    }

    if (Object.keys(sre_timestamps).length !== 0 && sre_timestamps.constructor === Object) {
        result.timestamps = {}
        Object.keys(sre_timestamps).forEach(k => {
            result.timestamps[k] = {
                title: sre_timestamps[k].title,
                columns: ['Account', 'Median', 'Average', 'Min', 'Max'],
                values: calculateTimestamps(sre_timestamps[k].values)
            }
        })
    }

    result.tables = Object.values(tables).map(table => {
        var r = {
            title: table.title,
            columns: table.columns,
            values: Array.isArray(table.values) ? table.values : Object.keys(table.values).map(k => { return { name: k, data: Object.values(table.values[k]) } })
        }

        if (table.count) r.count = table.count

        return r
    })

    return result;

}

async function reportSRE(entries, callback) {
    //secureLog("loop")

    console.log("::::: Grouping Data")

    let group = entries.leads_in.reduce((r, a) => {
        if (a.rec_type === 'First' || a.rec_type.toLowerCase() === 'firstafterap') {
            r['First Assignment'] = [...r['First Assignment'] || [], a];
        } else if (a.rec_type === 'Preassignment') {
            r['Active Prospect'] = [...r['Active Prospect'] || [], a];
        } else {
            r[a.rec_type] = [...r[a.rec_type] || [], a];
        }
        return r;
    }, {});

    group['Direct Mail'] = entries.direct_mail
    group['Dynamic Rank'] = entries.dynamic_rank

    let results = [];

    console.log("::::: Extracting Data")
    Object.keys(group).forEach(key => {
        results.push(extractData(group[key], key))
    })

    console.log(results)

    var emailTitle = 'Daily SRE Report - ' + moment().tz("America/Los_Angeles").subtract(1, 'days').startOf('day').utc().format('MMMM Do YYYY');
    // Compile template.pug, and render a set of data
    const html = pug.renderFile('base-template.pug', {
        data: results,
        header_title: emailTitle
    })

    var inlineHtml = await new Promise((resolve, reject) => {
        juice.juiceResources(html, {}, (e, r) => {
            if (e) reject(e)
            resolve(r)
        })
    })

    await sendEmail(inlineHtml)

    callback(null, {
        statusCode: 200,
        headers: {},
        body: ''
    })


}

function sendEmail(html, callback) {
    return new Promise((resolve, reject) => {
        var params = {
            Destination: {
                ToAddresses: [
                    'jdavid@pro-pair.com',
                    'djohnson@pro-pair.com',
                    'eewing@pro-pair.com'
                ]
            },
            Message: {
                Body: {
                    Html: {
                        Charset: "UTF-8",
                        Data: html
                    }
                },
                Subject: {
                    Charset: 'UTF-8',
                    Data: `Daily SRE report (${process.env.ENV})`
                }
            },
            Source: 'system@propair.ai'
        };

        // Create the promise and SES service object
        var sendPromise = new AWS.SES({ region: 'us-west-2', apiVersion: '2010-12-01' }).sendEmail(params).promise();

        // Handle promise's fulfilled/rejected states
        sendPromise.then(
            function(data) {
                secureLog(data.MessageId);
                resolve(data)
            }).catch(
            function(err) {
                console.error(err, err.stack);
                reject(err)
            });
    });
}

function formatData(data, rec_type, accounts) {
    if (data.length === 0) return []

    var template = {
        account_id: null,
        account_name: null,
        rec_type: rec_type,
        rec_count: 1,
        sre_timestamps: null,
        rec_result: null,
        custom_rank_bin: null,
        error: 0,
    }

    var result = []

    data.forEach(item => {
        var newItem = Object.assign({}, template)
        Object.keys(item).forEach(key => {
            if (key === 'account') {
                newItem['account_name'] = item[key]
            } else {
                newItem[key] = item[key]
            }
        })

        if (!newItem['account_name']) newItem['account_name'] = accounts.find(v => v.id === newItem.account_id).name
        result.push(newItem)
    })

    return result
}

exports.handler = async(event, context, callback) => {
    secureLog("::::: Summaries Reporting Service Starting")

    const start = moment().tz("America/Los_Angeles").subtract(1, 'days').startOf('day').utc();
    const end = moment().tz("America/Los_Angeles").subtract(1, 'days').endOf('day').utc();

    let query = 'select * from accounts';
    const accounts = await fetchData(query);

    query = `select * from leads_in
                    WHERE created_at BETWEEN '${start.format("YYYY-MM-DD HH:mm:ss")}' AND 
                    '${end.format("YYYY-MM-DD HH:mm:ss")}';`
    const leads_in = await fetchData(query)
    secureLog(`::::: Retrieved ${leads_in.length} Leads In rows`)

    query = `select account_id, error, custom_rank_bin, custom_rank_timeframe from summaries
                WHERE created_at BETWEEN '${start.format("YYYY-MM-DD HH:mm:ss")}'
                AND '${end.format("YYYY-MM-DD HH:mm:ss")}'
                AND custom_rank_timeframe > 1;`
    const drRankData = await fetchData(query)
    secureLog(`::::: Retrieved ${drRankData.length} Dynamic Rank rows`)

    query = `select account_id, custom_rank_bin from direct_mail
                    WHERE created_at BETWEEN '${start.format("YYYY-MM-DD HH:mm:ss")}' AND 
                    '${end.format("YYYY-MM-DD HH:mm:ss")}';`
    const dmData = await fetchData(query)
    secureLog(`::::: Retrieved ${dmData.length} Direct Mail rows`)

    console.log("::::: Formatting Data")
    var result = { leads_in, dynamic_rank: formatData(drRankData, 'Dynamic Rank', accounts), direct_mail: formatData(dmData, 'Direct Mail', accounts) }
    await reportSRE(result, callback);

}