'use strict';
var AWS = require('aws-sdk');
AWS.config.update({region: 'us-west-1'});

const queryString = require('query-string');
let pg = require('pg');
let ssm = new AWS.SSM({apiVersion: '2014-11-06'});
const Pool = require('pg-pool')
var pool;

const { getRedshiftClient } = require('pplibs/dbUtils')



exports.index = (event, context, callback) => {
    //deploy
    console.log("::::: Index function invoked");
    let client;

    getRedshiftClient().then(client => {
        client.connect(err => {
            if (err) { console.log(err) }
                client.query(`SELECT * FROM branches;`)
                .then(res => {
                    client.close();
                    callback(null,{
                        statusCode: 200,
                        body: JSON.stringify( res.rows ),
                        headers: {
                            'Content-Type': 'application/json',
                            'Access-Control-Allow-Origin': '*'
                        }
                    });
                }).catch(error => {
                    console.log("ERROR", error);
                    callback(null,{
                        statusCode: 500,
                        body: error,
                        headers: {
                            'Content-Type': 'application/json',
                            'Access-Control-Allow-Origin': '*'
                        }
                    });
                });
        });
    })
}

exports.show = (event, context, callback) => {
    console.log("::::: Show function invoked");

    let branchId = event['pathParameters']['branchId']

    let client;
    getRedshiftClient().then(client => {
        client.connect(err => {
            if (err) { console.log(err) }
                client.query("SELECT *  FROM branches WHERE id = "+branchId+";")
                .then(res => {
                client.close();

                if(res.rows.length > 0){

                    callback(null,{
                        statusCode: 200,
                        body: JSON.stringify( res.rows[0] ),
                        headers: {
                            'Content-Type': 'application/json',
                            'Access-Control-Allow-Origin': '*'
                        }
                    });

                }else{
                    callback(null,{
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
                callback(null,{
                    statusCode: 500,
                    body: error,
                    headers: {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    }
                });
            });
        });
    })
}

exports.create = (event, context, callback) => {
    console.log("::::: Create function invoked");

    const params = JSON.parse(event.body);

    let client;
    let date = new Date();
    let dateString = date.getFullYear() + "-" + ("0" + (date.getMonth() + 1)).slice(-2) + "-" + ("0" + (date.getDate())).slice(-2) + " " + ("0" + (date.getHours())).slice(-2) + ":" + ("0" + (date.getMinutes())).slice(-2) + ":" + ("0" + (date.getSeconds())).slice(-2);

    getRedshiftClient().then(client => {
        client.connect(err => {
            if (err) { console.log(err) }
            let query = `INSERT INTO branches(id, name, branch_group_id, time_zone, created_at, updated_at)
                            VALUES( (SELECT coalesce(max(id), 0) + 1 from branches), '${params.name}', ${params.branch_group_id}, '${params.time_zone}', '${dateString}', '${dateString}')`;
            client.query(query)
            .then(res => {
                client.close();

                callback(null,{
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
                callback(null,{
                    statusCode: 500,
                    body: JSON.stringify(error),
                    headers: {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    }
                });
            });
        });
    })
}

exports.update = (event, context, callback) => {
    console.log("::::: Update function invoked");
    let branchId = event['pathParameters']['branchId']
    const params = JSON.parse(event.body);

    let client;
    let date = new Date();
    let dateString = date.getFullYear() + "-" + ("0" + (date.getMonth() + 1)).slice(-2) + "-" + ("0" + (date.getDate())).slice(-2) + " " + ("0" + (date.getHours())).slice(-2) + ":" + ("0" + (date.getMinutes())).slice(-2) + ":" + ("0" + (date.getSeconds())).slice(-2);

    getRedshiftClient().then(client => {
        client.connect(err => {
            if (err) { console.log(err) }
            let query = `UPDATE branches SET name='${params.name}', branch_group_id=${params.branch_group_id}, time_zone='${params.time_zone}', updated_at='${dateString}' WHERE id = ${branchId}`;
            client.query(query)
            .then(res => {
                client.close();

                callback(null,{
                    statusCode: 200,
                    body: '',
                    headers: {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    }
                });
            }).catch(error => {
                client.close();
                console.log("ERROR", error);
                callback(null,{
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
