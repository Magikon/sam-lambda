'use strict';
var AWS = require('aws-sdk');
AWS.config.update({ region: 'us-west-1' });
var dynamodb = new AWS.DynamoDB.DocumentClient({ region: 'us-west-1' });
let athena = new AWS.Athena({ region: 'us-west-2' });
let s3 = new AWS.S3({ region: 'us-west-2' });

const myBucket = 'pro-pair-athena-queries'
const signedUrlExpireSeconds = 60 * 5

function startQueryExecution() {
  return new Promise((resolve, reject) => {
    let yesterday = new Date(new Date().getTime() - (24 * 60 * 60 * 1000));
    let tomorrow = new Date(new Date().getTime() + (24 * 60 * 60 * 1000));

    let yesterdayPartition = yesterday.getFullYear() + "-" + ("0" + (yesterday.getMonth() + 1)).slice(-2) + "-" + ("0" + (yesterday.getDate())).slice(-2);
    let tomorrowPartition = tomorrow.getFullYear() + "-" + ("0" + (tomorrow.getMonth() + 1)).slice(-2) + "-" + ("0" + (tomorrow.getDate())).slice(-2);


    var params = {
      QueryString: "SELECT * FROM summaries WHERE dt >= '" + yesterdayPartition + "' AND dt <= '" + tomorrowPartition + "' AND created_at >= date_add('hour', -120, now());",
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
    athena.startQueryExecution(params, function (err, data) {
      if (err) {
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
}



function getQueryResult(executionId, callback) {
  console.log("loop")
  return new Promise((resolve, reject) => {
    var params = {
      QueryExecutionId: executionId
    };
    athena.getQueryExecution(params, function (err, data) {
      if (err) {
        console.log(err, err.stack);
        reject(err);
      } else {
        if (data['QueryExecution']['Status']['State'] === 'SUCCEEDED' || data['QueryExecution']['Status']['State'] === 'CANCELLED' || data['QueryExecution']['Status']['State'] === 'FAILED') {
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
              "Location": url
            },
            body: null
          };
          callback(null, response);
        } else {
          setTimeout(function () { resolve(getQueryResult(executionId, callback)) }, 500); //Sleep for 500ms before checking again if report isn't ready
        }
      }
    });
  });
}

function getToken() {
  return new Promise((resolve, reject) => {

    // if account configuration does not exist or a refresh as been invoked. 
    // Retrieve the latest configurations for all accounts from dynamoDB
    // and return the account configuration specified by id,
    // else, return the pre existing cached configuration.

    console.log("::::: Retrieving Account Configurations");
    var params = {
      Key: {
        "id": 1
      },
      TableName: 'customer-configuration'
    };
    dynamodb.get(params, function (err, data) {
      if (err) {
        console.log(err, err.stack); // an error occurred
        reject(err);
      } else {

        resolve(data['Item']['summaries_token'])
      };
    });

  });
}

exports.handler = async (event, context, callback) => {
  console.log("::::: Summaries Reporting Service Starting")

  const token = await getToken();

  if (event.headers.Authorization.replace('Bearer ', '') != token) {
    var response = {
      statusCode: 401,
      body: null
    };
    callback(null, response);
  } else {
    const result = await startQueryExecution()
    console.log(result);
    await getQueryResult(result['QueryExecutionId'], callback);
  }
}
