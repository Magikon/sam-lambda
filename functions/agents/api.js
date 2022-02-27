'use strict';
var AWS = require('aws-sdk');
AWS.config.update({ region: 'us-west-1' });
const Redshift = require('node-redshift')
const queryString = require('query-string');
let pg = require('pg');
let ssm = new AWS.SSM({ apiVersion: '2014-11-06' });
const Pool = require('pg-pool')
let moment = require('moment')
let _ = require('lodash')
var pool;

const { getRedshiftClient } = require('pplibs/dbUtils')
const { secureLog } = require('pplibs/logUtils')
function generateValues(columns, dbColumns, params, type = '') {
    let result = ''

    let i = 0;
    let columnsLength = Object.keys(dbColumns).length
    for (let col in dbColumns){
        let value = typeof params[col] !== 'undefined' ? params[col] : null;
        let dataType = dbColumns[col].data_type
            let prefix = type === 'update' ? col + '=' : ''
            if (typeof value === 'string' && value !== '') {
                if (dataType.includes('date') || dataType.includes('timestamp')) value = moment(value).format('YYYY-MM-DD HH:mm:ss')

                if (value !== "Invalid date") {
                    value = `$$${value}$$`
                } else {
                    console.log(`::::: ERROR! Invalid date for Field: ${col}, Value: ${params[col]}`)
                    value = null
                }
            }

            if (typeof value === 'object' && value) {
                try {
                    value = `$$${JSON.stringify(value)}$$`
                } catch (e) {
                    console.log(`::::: ERROR! Cannot parse object for Field: ${col}, Error: ${e}`)
                    value = null
                }
            }

            if (value === null || value === '') {
                value = 'NULL'
            } else {
                value += `::${dataType}`;
            }

            result += prefix + value
            if (columnsLength !== i + 1) result += ', ';
            i++
    }

    return result;
}

exports.index = (event, context, callback) => {
    console.log("::::: Index function invoked");
    let client;
    getRedshiftClient().then(client => {
        client.connect(err => {
            if (err) { console.log(err) }
            client.query("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_NAME = 'agent_profiles'")
            .then(res => {
                const columns = []
                const excluded = [
                    "encompass_name",
                    "encompass_name2",
                    "license_count",
                    "license_status",
                    "license_type",
                    "primary_phone",
                    "secondary_phone",
                    "regulatory_actions",
                    "license_list",
                    "home_city",
                    "home_state",
                    "zillow_profile",
                    "zillow_profile_count",
                    "recent_review",
                    "lt_profile",
                    "lt_profile_count",
                    "fb_profile",
                    "linkedin_profile",
                    "brokers_outpost_profile",
                    "college",
                    "graduation_year",
                    "days_account",
                    "days_account_lead",
                    "years_account",
                    "years_account_lead",
                    "years_industry",
                    "avg_tenure",
                    "id"
                ]
                // Exclude all columns that start with lo_
                const patt = new RegExp('lo_.*')
                res.rows.forEach(row => {
                    if (!patt.test(row.column_name) && !excluded.includes(row.column_name)) {
                        columns.push(row.column_name)
                    }
                })
    
                var query = `SELECT a.id, b.name as account_name, c.name as branch_name, `
    
                columns.sort().forEach((col, i) => {
                    query += `a.${col}`
                    if (columns.length > i + 1) query += ', '
                })
    
                query += ` FROM agent_profiles a INNER JOIN accounts b ON a.account_id = b.id LEFT JOIN branches c ON a.branch_id = c.id;`
    
                return client.query(query);
            }).then(res => {
                client.close();
                callback(null, {
                    statusCode: 200,
                    body: JSON.stringify(res.rows),
                    headers: {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    }
                });
            }).catch(error => {
                client.close()
                console.log("ERROR", error);
                callback(null, {
                    statusCode: 500,
                    body: error,
                    headers: {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    }
                });
            });
        });
    });
}

exports.show = (event, context, callback) => {
    console.log("::::: Show function invoked");

    let agentId = event['pathParameters']['agentId']

    let client;
    getRedshiftClient().then(client => {
        client.connect(err => {
            if (err) { console.log(err); reject(err); }
            client.query("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_NAME = 'agent_profiles'")
            .then(res => {
                const columns = []
    
                // Exclude all columns that start with lo_
                const patt = new RegExp('lo_.*')
                res.rows.forEach(row => {
                    if (!patt.test(row.column_name) || row.column_name === 'id') {
                        columns.push(row.column_name)
                    }
                })
    
                var query = `SELECT id, `
    
                columns.sort().forEach((col, i) => {
                    query += `${col}`
                    if (columns.length > i + 1) query += ', '
                })
    
                query += ` FROM agent_profiles WHERE id = ${agentId};`
    
                return client.query(query);
            }).then(res => {
                client.close();
    
                if (res.rows.length > 0) {
                    let agent = res.rows[0]
                    if (agent.account_start_date) { agent.account_start_date = moment(agent.account_start_date, 'YYYY-MM-DDTHH:mm:ss.Z').format("MM/DD/YYYY") }
                    if (agent.account_end_date) { agent.account_end_date = moment(agent.account_end_date, 'YYYY-MM-DDTHH:mm:ss.Z').format("MM/DD/YYYY") }
                    if (agent.rec_start_date) { agent.rec_start_date = moment(agent.rec_start_date, 'YYYY-MM-DDTHH:mm:ss.Z').format("MM/DD/YYYY") }
                    callback(null, {
                        statusCode: 200,
                        body: JSON.stringify(res.rows[0]),
                        headers: {
                            'Content-Type': 'application/json',
                            'Access-Control-Allow-Origin': '*'
                        }
                    });
    
                } else {
                    callback(null, {
                        statusCode: 404,
                        body: "",
                        headers: {
                            'Content-Type': 'application/json',
                            'Access-Control-Allow-Origin': '*'
                        }
                    });
                }
    
            }).catch(error => {
                client.close()
                console.log("ERROR", error);
                callback(null, {
                    statusCode: 500,
                    body: error,
                    headers: {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    }
                });
            });
        });
    });
}

exports.create = (event, context, callback) => {
    console.log("::::: Create function invoked");

    const params = JSON.parse(event.body);

    let client;
    let dateString = moment().format('YYYY-MM-DD HH:mm:ss')
    let columns = Object.keys(params)
    const excludeColumns = ['id', 'created_at', 'updated_at']

    columns = _.filter(columns, n => { return !excludeColumns.includes(n) })

    getRedshiftClient().then(client => {
        client.connect(err => {
            if (err) { console.log(err); reject(err); }
            client.query("SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_NAME = 'agent_profiles'")
            .then(res => {
                const dbColumns = _.keyBy(res.rows, row => row.column_name);
                const full_name = params.first_name && params.last_name ? params.first_name + ' ' + params.last_name : null
                var query = `INSERT INTO agent_profiles(id, ${columns}, ${full_name ? 'name, ' : ''}created_at) VALUES( (SELECT coalesce(max(id), 0) + 1 from agent_profiles), `
                query += generateValues(columns, dbColumns, params)
                query += `${full_name ? `, $$${full_name}$$` : ''}, $$${dateString}$$::timestamp without time zone);`
    
                return client.query(query);
            }).then(res => {
                client.close();
    
                callback(null, {
                    statusCode: 201,
                    body: '',
                    headers: {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    }
                });
            }).catch(error => {
                client.close();
                console.log("ERROR", error);
                callback(null, {
                    statusCode: 500,
                    body: JSON.stringify(error),
                    headers: {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    }
                });
            });
            
            
        });
        
    });
}

exports.create_agents = (event, context, callback) => {
    console.log("::::: Create function invoked");
    var agents = []
    var params = JSON.parse(event.body);
    if (Array.isArray(params)) {
        agents = params
    } else {
        agents.push(params)
    }
    let date = new Date();
    let dateString = date.getFullYear() + "-" + ("0" + (date.getMonth() + 1)).slice(-2) + "-" + ("0" + (date.getDate())).slice(-2) + " " + ("0" + (date.getHours())).slice(-2) + ":" + ("0" + (date.getMinutes())).slice(-2) + ":" + ("0" + (date.getSeconds())).slice(-2);

    let counter = 0;
    const columns = [
        'id', 'active', 'agent_id', 'account_id', 'branch', 'branch_id',
        'channel', 'name', 'rec_type', 'recommend',
        'role', 'first_name', 'last_name', 'name_velocify', 'employer_count',
        'account_start_date', 'account_end_date', 'branch_group',
        'nmls_id', 'rec_start_date', 'role_group',
        'sales_manager', 'tier', 'title', 'veteran', 'assistant', 'gender',
        'transfers_to', 'eligible_branch', 'email', 'duplicate', 'created_at'
    ]
    let insertStatement = `INSERT INTO agent_profiles (`;
    insertStatement += `${_.map(columns, (a) => `${a}`)} ) VALUES`

    getRedshiftClient().then(client => {
        client.connect( err=> {
            if (err) {console.log(err)}
            let queryId = "SELECT coalesce(max(id), 0) as id from agent_profiles;"
            client.query(queryId)
            .then(res => {
                let maxId = parseInt(res.rows[0].id);
                insertStatement += _.map(agents, a => {
                    let row = `(`;
                    counter++;
                    row += columns.map(function (c) {
                        let value;
                        if (c === 'id') {
                            value = maxId + counter
                        } else if (c.includes('timestamp_without_timezone')) {
                            value = (typeof a[c] == 'undefined' || a[c] == null) ? null : `'${moment(a[c], 'MM/DD/YYYY')}'`
                        } else if (c === 'created_at' || c === 'updated_at') {
                            value = `'${dateString}'`;
                        } else if (c === 'name') {
                            value = `$$${a.first_name} ${a.last_name}$$`
                        } else if (typeof a[c] == 'undefined' || a[c] == null) {
                            value = null
                        } else {
                            value = a[c] === null || typeof a[c] === 'boolean' ? a[c] : `$$${a[c]}$$`
                        }
            
                        return `${value}`;
                    });
                    row += ')'
                    return row;
                });
                insertStatement += ';';

                client.query(insertStatement)
                .then(res => {
                    client.close();
    
                callback(null, {
                    statusCode: 201,
                    body: '',
                    headers: {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    }
                });
            }).catch(error => {
                client.close();
                console.log("ERROR", error);
                callback(null, {
                    statusCode: 500,
                    body: JSON.stringify(error),
                    headers: {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    }
                });
            });
            })
        })
    });
}

exports.update = (event, context, callback) => {
    console.log("::::: Update function invoked");

    const params = JSON.parse(event.body);
    let agentId = event['pathParameters']['agentId']

    let client;
    let dateString = moment().format('YYYY-MM-DD HH:mm:ss')
    let columns = Object.keys(params)
    const excludeColumns = ['id', 'created_at', 'updated_at']

    columns = _.filter(columns, n => { return !excludeColumns.includes(n) })

    getRedshiftClient().then(client => {

        client.connect(err => {
            if (err) { console.log(err); reject(err); }
            client.query("SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_NAME = 'agent_profiles'")
            .then(res => {
                const dbColumns = _.keyBy(res.rows, row => row.column_name);
                var query = `UPDATE agent_profiles SET `
                params.name = params.first_name && params.last_name ? params.first_name + ' ' + params.last_name : null
                for (let i in excludeColumns) {
                    delete dbColumns[excludeColumns[i]]
                }
                query += generateValues(columns, dbColumns, params, 'update')
                query += `, updated_at=$$${dateString}$$::timestamp without time zone`
                query += ` WHERE id=${agentId};`
        
                return client.query(query);
            }).then(res => {
                client.close();
        
                callback(null, {
                        statusCode: 201,
                        body: '',
                        headers: {
                            'Content-Type': 'application/json',
                            'Access-Control-Allow-Origin': '*'
                        }
                });
            }).catch(error => {
                client.close();
                console.log("ERROR", error);
                callback(null, {
                        statusCode: 500,
                        body: JSON.stringify(error),
                        headers: {
                            'Content-Type': 'application/json',
                            'Access-Control-Allow-Origin': '*'
                        }
                });
            });
        });
    });
}

async function getColumns() {
    return new Promise((resolve, reject) => {
        getRedshiftClient().then(client => {
            client.connect(err => {
                if (err) { console.log(err); reject(err); }
                client.query("SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_NAME = 'agent_profiles'")
                .then(res => {
                    resolve(res)
                })
                .catch(err => {
                    reject(err)
                })
            })
        });
})
}

async function updateAgents(body){
    let promise = await new Promise(async (resolve, reject) => {
        const params = body;
        // let agentId = body['id']
        let dateString = moment().format('YYYY-MM-DD HH:mm:ss')
        let columns = Object.keys(params[0])
        const excludeColumns = ['id', 'created_at', 'updated_at']
        columns = _.filter(columns, n => { return !excludeColumns.includes(n) })
            try {
            const conn = await getRedshiftClient();
            const dbColumns = await getColumns();
            for (let i in excludeColumns) {
                delete dbColumns[excludeColumns[i]]
            }
            let columnsDB = []
            dbColumns.rows.forEach(col => {
                columnsDB.push(col.column_name)
            })
            conn.connect(async err => {
                if (err) { console.log(err); reject(err); }
                try {
                  await insertToDB(params, 'agent_profiles', conn, columnsDB)
                  conn.close()
                  resolve({
                    statusCode: 200,
                    message: "Ok"
            })
                } catch(e) {
                    conn.close();
                    console.log("ERROR", e);
                    reject({
                        statusCode: 500,
                        message: JSON.stringify(e)
                });
                }
              })
            }
        catch(e) {
            console.log("ERROR", e);
                    reject({
                        statusCode: 500,
                        message: JSON.stringify(e)
            })
        }
    })
    return promise
}

function removeDuplicates(records, uniqueColumns) {
	var hash = Object.create(null),
    result = [];
	for (let i = 0; i < records.length; i++) {
		var key = [..._.map(uniqueColumns, a => records[i][a] ? records[i][a] : 'null' )].join('.')
		if (!hash[key]) {
			hash[key] = true;
			result.push(records[i]);
		}
	}
	return result
}

function sleep(ms) {
	return new Promise(resolve => setTimeout(resolve, ms));
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
                return true
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
    let columnData = null
    let columnDataTable = null
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
	let columns = {}
    
    columnData.forEach(line => {
        columns[line.column] = { name: line.column, type: line.type }
	})

	//Query start
	let query = `INSERT INTO ${stageName}(${Object.keys(columns).join(', ')})`
	//This variable will store the insert query for the event data
	let values = `VALUES `
	//This variable will store the update query for existing events
	let updateQuery = `update ${tableName} set `

	let updateValues = []
	let insertValues = []

	Object.keys(columns).forEach(col => {
        insertValues.push(col)
        if (!['created_at', 'agent_id', 'name_velocify'].includes(col)) {
            updateValues.push(`${col} = ${stageName}.${col}`)
        }
	})
    
	updateQuery += `${updateValues.join(', ')}`
	updateQuery += ` from ${stageName} where `
    updateQuery += `${tableName}.agent_id = ${stageName}.agent_id and ${tableName}.account_id = ${stageName}.account_id and ${tableName}.id = ${stageName}.id;`

	records.forEach((record, i) => {
        record.name = record.first_name + " " + record.last_name 
		let recordValues = []
		const date = moment().format('YYYY-MM-DD HH:mm:ss')

		Object.keys(columns).forEach((key) => {
			let value = typeof record[key] !== "undefined" || record[key] !== '' ? record[key] : null
			const dataType = columns[key].type

			if (dataType.includes('integer') && value) {
				const maxInt = 2147483647;
				value = parseInt(value, 10) > maxInt ? maxInt : value
			} else if (dataType.includes('bigint') && value) {
				const maxInt = 9223372036854775800;
				value = parseInt(value, 10) > maxInt ? maxInt : value
			}
            let dateString = moment().format('YYYY-MM-DD HH:mm:ss')
			if (['created_at'].includes(key)) {
				value = `$$${date}$$`
            }
            else if (['updated_at'].includes(key)) {
                value = `$$${dateString}$$`
            }
            else if (!value || value === '') {
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
		query += values + '; ' + updateQuery
	} else {
		query += values + ';'
	}

	return query
}


exports.update_agents = async (event, context, callback) => {
    console.log("::::: Update function invoked");
    var agents = []
    try {
        var params = JSON.parse(event.body);
        if (Array.isArray(params)) {
            agents = params
        } else {
            agents.push(params)
        }
        let dateString = moment().format('YYYY-MM-DD HH:mm:ss')

        console.log(`::::: UPDATING ${agents.length} AGENTS`)

        try {
            let res = await updateAgents(agents);
            if (res.StatusCode == 200) {
                callback(null, {
                    statusCode: 201,
                    body: '',
                    headers: {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    }
                })
            }
            else {
                callback(null, {
                    statusCode: 500,
                    body: 'Upsert Error',
                    headers: {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    }
                })
            }
            
        } catch (error) {
            secureLog(error)
            callback(null, {
                            statusCode: 500,
                            body: JSON.stringify(error),
                            headers: {
                                'Content-Type': 'application/json',
                                'Access-Control-Allow-Origin': '*'
                            }
                        });
        }
    } catch (error) {
        secureLog(error)
        callback(null, {
            statusCode: 500,
            body: JSON.stringify(error),
            headers: {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        });
    }
    
    
}

