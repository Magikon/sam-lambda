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
    getRedshiftClient().then(client => {
        client.connect(err => {
            if (err) { console.log(err) }
            client.query(`SELECT * FROM branch_groups`)
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
                client.close();
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
    });
}

exports.show = (event, context, callback) => {
    console.log("::::: Show function invoked");

    let branchGroupId = event['pathParameters']['branchGroupId']

    getRedshiftClient().then(client => {
        client.connect(err => {
            if (err) { console.log(err) }
            client.query("SELECT *  FROM branch_groups WHERE id = "+branchGroupId+";")
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
                    client.close()
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
    });
}

exports.create = (event, context, callback) => {
    console.log("::::: Create function invoked");

    const params = JSON.parse(event.body);

    let date = new Date();
    let dateString = date.getFullYear() + "-" + ("0" + (date.getMonth() + 1)).slice(-2) + "-" + ("0" + (date.getDate())).slice(-2) + " " + ("0" + (date.getHours())).slice(-2) + ":" + ("0" + (date.getMinutes())).slice(-2) + ":" + ("0" + (date.getSeconds())).slice(-2);

    getRedshiftClient().then(client => {
        client.connect(err => {
            if (err) { console.log(err) }
            let query = `INSERT INTO branch_groups(id, name, account_id, created_at, updated_at)
                            VALUES( (SELECT coalesce(max(id), 0) + 1 from branch_groups), '${params.name}', ${params.account_id}, '${dateString}', '${dateString}')`;
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
    });
}

exports.update = (event, context, callback) => {
    console.log("::::: Update function invoked");
    let branchGroupId = event['pathParameters']['branchGroupId']
    const params = JSON.parse(event.body);

    let date = new Date();
    let dateString = date.getFullYear() + "-" + ("0" + (date.getMonth() + 1)).slice(-2) + "-" + ("0" + (date.getDate())).slice(-2) + " " + ("0" + (date.getHours())).slice(-2) + ":" + ("0" + (date.getMinutes())).slice(-2) + ":" + ("0" + (date.getSeconds())).slice(-2);

    getRedshiftClient().then(client => {
        client.connect(err => {
            if (err) { console.log(err) }
            let query = `UPDATE branch_groups SET name='${params.name}', account_id=${params.account_id}, updated_at='${dateString}' WHERE id = ${branchGroupId}`;
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
