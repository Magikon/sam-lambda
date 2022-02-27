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
            client.query(`SELECT * FROM accounts;`)
            .then(res => {
                client.close();
                callback(null,{
                    statusCode: 200,
                    body: JSON.stringify( res.rows ),
                    headers: {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    }
                })
            
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
    })
}
    

exports.show = (event, context, callback) => {
    console.log("::::: Show function invoked");

    let customerId = event['pathParameters']['accountId']

    let client;
    getRedshiftClient().then(client => {
        client.connect(err => {
            if (err) { console.log(err) }
            client.query(`SELECT * FROM accounts WHERE id = ${customerId};`)
            .then(res => {
                if(res.rows.length > 0){

                    if(event.queryStringParameters && event.queryStringParameters.include_agents){
                        client.query(`SELECT id, agent_id, account_id, name, rec_type, active, recommend, channel, source_exclusions, created_at FROM agent_profiles WHERE account_id=${customerId};`)
                        .then(res2 => {
                            client.close();
                            let account = res.rows[0]
                            account["agents"] = res2.rows
                            callback(null,{
                                statusCode: 200,
                                body: JSON.stringify( account ),
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
                    }else{
                        client.close();
                        callback(null,{
                            statusCode: 200,
                            body: JSON.stringify( res.rows[0] ),
                            headers: {
                                'Content-Type': 'application/json',
                                'Access-Control-Allow-Origin': '*'
                            }
                        });
                    }

                }else{
                    client.close();
                    callback(null,{
                        statusCode: 404,
                        body: "",
                        headers: {
                            'Content-Type': 'application/json',
                            'Access-Control-Allow-Origin': '*'
                        }
                    });dateString
                }

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
        })
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
            if(err) { console.log(err) }
            let query = `INSERT INTO accounts(
              id, name, token, propair_agent_id, agent_availability_report_id,
              velocify_username, velocify_password, velocify_rec_field_1_id, velocify_branch_rec_field_1_id, velocify_group_field_id,
              ab_algo_count, ab_recommendation_threshold, ab_algo1_suffix, ab_algo2_suffix,
              recommendation_threshold, recommendation_threshold_weekend, recommendation_threshold_branch,
              expand_rec_after_seconds, expand_rec_by_percentage, branch_specific_second_rec,
              created_at, updated_at)
              VALUES( (SELECT max(id) + 1 from accounts),
              '${params.name}', '${params.token}', ${params.propair_agent_id}, ${params.agent_availability_report_id},
              '${params.velocify_username}', '${params.velocify_password}',
              ${params.velocify_rec_field_1_id}, ${params.velocify_branch_rec_field_1_id},
              ${params.velocify_group_field_id}, ${params.ab_algo_count}, ${params.ab_recommendation_threshold},
              ${params.ab_algo1_suffix}, ${params.ab_algo2_suffix}, ${params.recommendation_threshold},
              ${params.recommendation_threshold_weekend}, ${params.recommendation_threshold_branch},
              ${params.expand_rec_after_seconds}, ${params.expand_rec_by_percentage}, ${params.branch_specific_second_rec},
              '${dateString}', '${dateString}')`;
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
    let accountId = event['pathParameters']['accountId']
    const params = JSON.parse(event.body);

    let client;
    let date = new Date();
    let dateString = date.getFullYear() + "-" + ("0" + (date.getMonth() + 1)).slice(-2) + "-" + ("0" + (date.getDate())).slice(-2) + " " + ("0" + (date.getHours())).slice(-2) + ":" + ("0" + (date.getMinutes())).slice(-2) + ":" + ("0" + (date.getSeconds())).slice(-2);

    getRedshiftClient().then(client => {
        client.connect(err => {
            if(err) { console.log(err) }
            
            let query = `UPDATE accounts SET
            name='${params.name}', token='${params.token}', propair_agent_id=${params.propair_agent_id}, agent_availability_report_id=${params.agent_availability_report_id},
            velocify_username='${params.velocify_username}', velocify_password='${params.velocify_password}',velocify_rec_field_1_id=${params.velocify_rec_field_1_id},
            velocify_branch_rec_field_1_id=${params.velocify_branch_rec_field_1_id}, velocify_group_field_id=${params.velocify_group_field_id},
            ab_algo_count=${params.ab_algo_count}, ab_recommendation_threshold=${params.ab_recommendation_threshold}, ab_algo1_suffix=${params.ab_algo1_suffix},
            ab_algo2_suffix=${params.ab_algo2_suffix}, recommendation_threshold=${params.recommendation_threshold},
            recommendation_threshold_weekend=${params.recommendation_threshold_weekend}, recommendation_threshold_branch=${params.recommendation_threshold_branch},
            expand_rec_after_seconds=${params.expand_rec_after_seconds}, expand_rec_by_percentage=${params.expand_rec_by_percentage},
            branch_specific_second_rec=${params.branch_specific_second_rec},
            updated_at='${dateString}' WHERE id = ${accountId}`;
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
