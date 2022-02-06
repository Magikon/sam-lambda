const Pool = require('pg-pool');
const Redshift = require('node-redshift')
const moment = require('moment')
const AWS = require('aws-sdk');
const axios = require('axios');
AWS.config.update({ region: 'us-west-1' });
const ssm = new AWS.SSM({ apiVersion: '2014-11-06' });

let dbPassword;
let redshiftPassword;
let pool;
let redShiftClient;

// Utilities
const { secureLog } = require('pplibs/logUtils')
const _ = require('lodash')

let columnData = null
let columnDataTable = null

var dynamodb = new AWS.DynamoDB.DocumentClient({ region: 'us-west-1' });

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
        console.log(e)
        return null
    }
}

function getDBPassword(env, refresh = false) {
    return new Promise((resolve, reject) => {
        if (!dbPassword || refresh) {
            var params = {
                Name: `${env}-database-password`,
                WithDecryption: true
            };

            ssm.getParameter(params, function (err, data) {
                if (err) {
                    reject(err);
                } else {
                    dbPassword = data.Parameter.Value;
                    resolve(dbPassword);
                }
            });
        } else {
            resolve(dbPassword);
        }
    });
}

function getDBPool(env, refresh = false) {
    return new Promise((resolve, reject) => {
        if (!pool || refresh) {
            const {
                ENV: env,
                DB_ENDPOINT: host,
                DB_USER: user,
                DB_NAME: database
            } = process.env;

            getDBPassword(`${env}-database-password`, refresh)
                .then(password => {
                    pool = new Pool({
                        host,
                        database,
                        user,
                        password,
                        port: 5432,
                        max: 1,
                        min: 0,
                        idleTimeoutMillis: 300000,
                        connectionTimeoutMillis: 1000
                    });
                    resolve(pool);
                })
                .catch(err => reject(err));
        } else {
            resolve(pool);
        }
    });
}

function getRedshiftPassword(env, refresh = false) {
    return new Promise((resolve, reject) => {
        if (!redshiftPassword || refresh) {
            var params = {
                Name: `/${env}/redshift/master-database-password`,
                WithDecryption: true
            };

            ssm.getParameter(params, function (err, data) {
                if (err) {
                    reject(err);
                } else {
                    redshiftPassword = data.Parameter.Value;
                    resolve(redshiftPassword);
                }
            });
        } else {
            resolve(redshiftPassword);
        }
    });
}

function getRedshiftClient(isRaw = true, refresh = false) {
    return new Promise((resolve, reject) => {
        const {
            ENV: env,
            REDSHIFT_ENDPOINT: host,
            REDSHIFT_DB_USER: user,
            REDSHIFT_DB_NAME: database,
            REDSHIFT_DB_PORT: port
        } = process.env;

        getRedshiftPassword(env, refresh)
            .then(password => {
                var client = {
                    user,
                    database,
                    port,
                    host,
                    password
                }
                redshiftClient = new Redshift(client, { rawConnection: isRaw })
                resolve(redshiftClient)
            }).catch(e => {
                reject(e)
            })
    })
}

executeQueryOnRedshiftRawConn = async (query) => {
    return new Promise((resolve, reject) => {
        getRedshiftClient().then(client => {
            client.connect(err => {
                if (err) console.log(err)
                client.query(query)
                    .then(res => {
                        client.close()
                        resolve(res)
                    })
                    .catch(err => {
                        client.close()
                        reject(err)
                    })
            })
        })
    })
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function cast(val, field, dataType) {
    function getAverage(val) {
        console.log(`(PARSING AVERAGE) field, value = ${field}, ${val}`)
        let strVal = val.toString()
        let mid = strVal.length / 2
        val = ((parseInt(strVal.substring(0, mid)) + parseInt(strVal.substring(mid))) / 2)
        console.log(`(POST AVERAGE): field, value = ${field}, ${val}`)
        return val
    }
    const accepted_fields = [
        "purchase_price_stated",
        "home_value_stated",
        "loan_amount_stated",
        "down_payment",
        "cash_out_stated"
    ]

    if (!val) {
        return null
    } else if (dataType.toUpperCase().includes("INT")) {
        //TODO: Sanitize before casting
        if (typeof val === 'string') {
            if (val.toLowerCase() === 'true' || val.toLowerCase() === 'yes') {
                return "1"
            } else if (val.toLowerCase() === 'false' || val.toLowerCase() === 'no') {
                return "0"
            } else {
                // remove decimals
                val = val.replace(/\.\d{1,2}/gi, "") 
                // remove non-digits
                val = val.replace(/[^0-9]/gi, "")

                if (isNaN(val)) {
                    return null
                } else {
                    val = parseInt(val, 10)
                    if (accepted_fields.includes(field)) {
                        try {
                            if (val > 100000000) val = getAverage(val);
                        } catch (e) {
                            console.log(e)
                        }
                    }
                    return val
                }
            }
        } else {
            if (accepted_fields.includes(field)) {
                if (val > 100000000) val = getAverage(val);
            }
            return val
        }
    } else if (dataType.toUpperCase().includes("FLOAT")) {
        let testVal;
        try {
            testVal = parseFloat(val)
        } catch {
            try {
                testVal = parseFloat(String(val).replace(/[^0-9.]/gi, ""))
            } catch {
                testVal = null
            }
        }
        return testVal
    } else {
        return val
    }
}

function removeDuplicates(records, uniqueColumns) {
    var hash = Object.create(null),
        result = [];
    for (i = 0; i < records.length; i++) {
        var key = [..._.map(uniqueColumns, a => records[i][a] ? records[i][a] : 'null')].join('.')
        if (!hash[key]) {
            hash[key] = true;
            result.push(records[i]);
        }
    }
    return result
}

async function insertToDB(records, tableName, client, uniqueColumns, chunkNum = null, defaults = {}, update = true) {
    const chunk = chunkNum ? ` Chunk: ${chunkNum}` : ""
    try {
        if (records.length > 0) {
            secureLog(`::::: [${tableName}]${chunk} Retrieved ${records.length} records`);

            const uniqueRecords = removeDuplicates(records, uniqueColumns);
            secureLog(`::::: [${tableName}]${chunk} Removed ${records.length - uniqueRecords.length} records`);

            if (update) {
                secureLog(`::::: [${tableName}]${chunk} Upserting ${uniqueRecords.length} records`);
            } else {
                secureLog(`::::: [${tableName}]${chunk} Inserting ${uniqueRecords.length} records`);
            }

            // - Upsert Procedure:
            // BEGIN
            //1 - Lock events table to prevent serializable errors
            //2 - Create temporary table to store event data
            //3 - Insert event data into temporary table
            //4 - Update external_leads with matched unique events from temporary table
            //5 - Deleted matched unique events from temporary table in order to only leave 'new' events
            //6 - Insert remaining 'new' events from temporary table into external_leads
            // END   

            //Set this to the temporary table name
            const stageTableName = `${tableName}_stage`;

            let sq = `
                begin read write;
				lock ${tableName};
                create temp table ${stageTableName} (like ${tableName});
                `;
            //This will retrieve data insert, update, delete and final insert query
            sq += await getInsertStatement(uniqueRecords, stageTableName, tableName, client, uniqueColumns, defaults, update);

            sq += `
                end transaction;
                drop table ${stageTableName};`;

            let count = 0;
            let successful = false;
            while (!successful && count < 2) {
                try {
                    let res = await client.query(sq)
                    successful = true
                } catch (e) {
                    secureLog(`::::: [${tableName}]${chunk} Error - ${e} trying again...`);
                    secureLog(`::::: [${tableName}]${chunk} Num of retries: ${count}`);
                    client.query('rollback;')
                    await sleep(0.5 * count)
                }
                count++
            }

            if (successful) {
                secureLog(`::::: [${tableName}]${chunk} Rows upserted`);
            } else {
                throw (`[${tableName}]${chunk} After ${count} retries upsert was unable to complete`)
            }
        } else {
            secureLog(`There isn't anything to insert`)
        }
    } catch (err) {
        throw (err)
    }
}

async function getColumnData(tableName, client) {
    if (!columnData || columnDataTable !== tableName) {
        columnDataTable = tableName
        let sq = `SELECT \"column\",type from pg_table_def where tablename='${tableName}'`
        let count = 0
        let successful = false
        while (!successful && count < 2) {
            try {
                let res = await client.query(sq)
                columnData = res.rows
                if (columnData.length > 0) {
                    successful = true
                } else {
                    throw ("No column data found")
                }
            } catch (e) {
                secureLog(`::::: [${tableName}] Error - ${e} trying again...`);
                secureLog(`::::: [${tableName}]Num of retries: ${count}`);
                client.query('rollback;')
                await sleep(0.5 * count)
            }
            count++
        }

        if (successful) {
            secureLog(`::::: Found data types`)
            return columnData
        } else {
            throw (`[${tableName}] After ${count} retries, was unable to retrieve Column Data`)
        }
    } else {
        return columnData
    }
}

async function getInsertStatement(records, stageName, tableName, client, uniqueColumns, defaults, update) {

    let columnData = await getColumnData(tableName, client)
    columns = {}
    ignored_columns = ['id']

    //Set this to the unique columns required for this table

    columnData.forEach(line => {
        if (!ignored_columns.includes(line.column)) {
            columns[line.column] = { name: line.column, type: line.type }
        }
    })

    //Query start
    let query = `INSERT INTO ${stageName}(${Object.keys(columns).join(', ')})`
    //This variable will store the insert query for the event data
    let values = `VALUES `
    //This variable will store the update query for existing events
    let updateQuery = `update ${tableName} set `
    //This variable will store the delete query removing existing events
    let deleteQuery = `delete from ${stageName} using ${tableName} where `
    //This variable will store the insert query for remaining non-existant events
    let insertQuery = `insert into ${tableName}`

    let updateValues = []
    let insertValues = []

    Object.keys(columns).forEach(col => {
        insertValues.push(col)
        if (!uniqueColumns.includes(col) && col !== 'created_at') {
            updateValues.push(`${col} = ${stageName}.${col}`)
        }
    })

    insertQuery += `(${insertValues.join(', ')}) select ${insertValues.join(', ')} from ${stageName};`
    updateQuery += `${updateValues.join(', ')}`
    updateQuery += ` from ${stageName} where `

    uniqueColumns.forEach((uniq, i) => {
        updateQuery += `${tableName}.${uniq} = ${stageName}.${uniq}`
        deleteQuery += `${tableName}.${uniq} = ${stageName}.${uniq}`
        if (i < (uniqueColumns.length - 1)) {
            updateQuery += " and "
            deleteQuery += " and "
        } else {
            updateQuery += ";"
            deleteQuery += ";"
        }
    })

    records.forEach((record, i) => {

        let recordValues = []
        const date = moment().format('YYYY-MM-DD HH:mm:ss')

        Object.keys(columns).forEach((key) => {
            let value = typeof record[key] !== "undefined" || record[key] !== '' ? record[key] : null
            const dataType = columns[key].type


            //if (dataType.includes('timestamp') && value) {
            //	try {
            //		let parseDate = moment(value)
            //		if (parseDate.isValid()) {
            //			value = parseDate.format('YYYY-MM-DD hh:mm:ss')
            //		} else {
            //			throw "Unable to parse"
            //		}
            //	} catch (e) {
            //		secureLog(`::::: Unable to parse datetime for column ${key}; Value: ${value}`)
            //		value = null
            //	}
            //} else if (dataType.includes('date') && value) {
            //	try {
            //		let parseDate = moment(value)
            //		if (parseDate.isValid()) {
            //			value = parseDate.format('YYYY-MM-DD')
            //		} else {
            //			throw "Unable to parse"
            //		}
            //	} catch (e) {
            //		secureLog(`::::: Unable to parse datetime for column ${key}; Value: ${value}`)
            //		value = null
            //	}
            if (dataType.includes('integer') && value) {
                const maxInt = 2147483647;
                value = parseInt(value, 10) > maxInt ? maxInt : value
            } else if (dataType.includes('bigint') && value) {
                const maxInt = 9223372036854775800;
                value = parseInt(value, 10) > maxInt ? maxInt : value
            }

            if (['updated_at', 'created_at'].includes(key)) {
                value = `$$${date}$$`
            } else if (value === null || typeof value === "undefined" || value === '') {
                if (typeof defaults[key] !== "undefined") {
                    value = `$$${defaults[key]}$$::${dataType}`
                } else {
                    value = `NULL`
                }
            } else {
                value = `$$${value}$$::${dataType}`
            }

            recordValues.push(value)
        })

        values += `(${recordValues.join(', ')})`

        if (i < (records.length - 1)) {
            values += ", "
        }
    })

    if (update) {
        query += values + '; ' + updateQuery + " " + deleteQuery + " " + insertQuery
    } else {
        query += values + ';' + " " + deleteQuery + " " + insertQuery
    }

    return query
}

exports.insertToDB = insertToDB;
exports.cast = cast;
exports.getDBPassword = getDBPassword;
exports.getDBPool = getDBPool;
exports.getRedshiftPassword = getRedshiftPassword;
exports.getRedshiftClient = getRedshiftClient;
exports.getColumnData = getColumnData;
exports.executeQueryOnRedshiftRawConn = executeQueryOnRedshiftRawConn;
exports.queryKSQL = queryKSQL;