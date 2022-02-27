'use strict';
var AWS = require('aws-sdk');
AWS.config.update({region: 'us-west-1'});
let athena = new AWS.Athena({region: 'us-west-2'});
let s3 = new AWS.S3({ region: 'us-west-2' });

const myBucket = 'pro-pair-athena-queries'
const signedUrlExpireSeconds = 60 * 5

function startQueryExecution(){
  return new Promise((resolve, reject) => {
    let yesterday = new Date(new Date().getTime() - (24 * 60 * 60 * 1000));
    let tomorrow  = new Date(new Date().getTime() + (24 * 60 * 60 * 1000));

    let yesterdayPartition = yesterday.getFullYear() + "-" + ("0" + (yesterday.getMonth() + 1)).slice(-2) + "-" + ("0" + (yesterday.getDate())).slice(-2);
    let tomorrowPartition  = tomorrow.getFullYear() + "-" + ("0" + (tomorrow.getMonth() + 1)).slice(-2) + "-" + ("0" + (tomorrow.getDate())).slice(-2);


    var params = {
        QueryString: `SELECT *
                        FROM 
                            (SELECT *,
                                RANK()
                                OVER (PARTITION BY account_id, log_id, log_subtype_id
                            ORDER BY  modified_at DESC) AS rank
                            FROM 
                                (SELECT log_user_name,
                                account_id,
                                log_user_id,
                                log_id,
                                log_date,
                                modified_at,
                                log_subtype_id,
                                log_type,
                                log_note,
                                log_subtype_name,
                                account_lead_id,
                                log_user_email,
                                dt,
                                group_name,
                                group_id,
                                milestone_id,
                                milestone_name,
                                imported
                                FROM external_events
                                WHERE dt >= '${yesterdayPartition}'
                                AND dt <= '${tomorrowPartition}'
                                AND created_at >= date_add('minute', -65, now())
                                GROUP BY  log_user_name, account_id, log_user_id, log_id, log_date, modified_at, log_subtype_id, log_type, log_note, log_subtype_name, account_lead_id, log_user_email, dt, group_name, group_id, milestone_id, milestone_name, imported ) )
                            WHERE rank = 1`,
        ResultConfiguration: { /* required */
          OutputLocation: 's3://pro-pair-athena-queries/queries', /* required */
          EncryptionConfiguration: {
            EncryptionOption: 'SSE_S3', /* required
            KmsKey: 'STRING_VALUE'*/
          }
        },
        //ClientRequestToken: 'STRING_VALUE',
        QueryExecutionContext: {
          Database: 'propair_production_logs'
        }
      };
      athena.startQueryExecution(params, function(err, data) {
        if (err){
          reject(err);
        }else{
          resolve(data);
        }
    });
  });
}



function getQueryResult(executionId, callback){
  console.log("loop")

    var params = {
        QueryExecutionId: executionId
    };
    athena.getQueryExecution(params, function(err, data) {
        if (err){
            console.log(err, err.stack);
            reject(err);
        }else{
            if(data['QueryExecution']['Status']['State'] === 'SUCCEEDED' || data['QueryExecution']['Status']['State'] === 'CANCELLED' || data['QueryExecution']['Status']['State'] === 'FAILED'){
                console.log(data);
                let location = data['QueryExecution']['ResultConfiguration']['OutputLocation'];
                let locationParts = location.split("/")
                let myKey = "queries/" + locationParts[locationParts.length - 1]

                const url = s3.getSignedUrl('getObject', {
                    Bucket: myBucket,
                    Key: myKey,
                    Expires: signedUrlExpireSeconds
                })

                console.log(url)

                var response = {
                    statusCode: 302,
                    headers: {
                        "Location" : url
                    },
                    body: null
                };
                callback(null, response);
            }else{
                setTimeout(function(){ return getQueryResult(executionId, callback) }, 500); //Sleep for 500ms before checking again if report isn't ready
            }
        }
    });
}


exports.handler = (event, context, callback) => {
  console.log("::::: Events Reporting Service Starting")

  if(event.headers.Authorization.replace('Bearer ', '') != "wxVa4kEzFxGcA5eaICZGRqtyVPAPoDNo"){
    var response = {
      statusCode: 401,
      body: null
    };
    callback(null, response);
  }else{
    startQueryExecution().then(function(result){
      console.log(result);
      getQueryResult(result['QueryExecutionId'], callback);
    });
  }
}
