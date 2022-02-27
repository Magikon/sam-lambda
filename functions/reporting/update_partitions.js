'use strict';
var AWS = require('aws-sdk');
AWS.config.update({region: 'us-west-1'});
let athena = new AWS.Athena({region: 'us-west-2'});
let s3 = new AWS.S3({ region: 'us-west-2' });
let CSV = require('csv-string');

const myBucket = 'pro-pair-athena-queries'
const signedUrlExpireSeconds = 60 * 5

function repairTable(table){
  return new Promise((resolve, reject) => {
    var params = {
        QueryString: "MSCK REPAIR TABLE " + table + ";",
        ResultConfiguration: { 
          OutputLocation: 's3://pro-pair-athena-queries/queries',
          EncryptionConfiguration: {
            EncryptionOption: 'SSE_S3'
          }
        },
        QueryExecutionContext: {
          Database: process.env.ATHENA_DB_NAME
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

function getExecutionId() {
  console.log("::::: Invoking Query Execution For Existing Tables")
  return new Promise((resolve, reject) => {
    var params = {
      QueryString: "SHOW TABLES;",
      ResultConfiguration: { 
        OutputLocation: 's3://pro-pair-athena-queries/queries',
        EncryptionConfiguration: {
          EncryptionOption: 'SSE_S3'
        }
      },
      QueryExecutionContext: {
        Database: process.env.ATHENA_DB_NAME
      }
    };
    athena.startQueryExecution(params, function(err, data) {
      if (err){ 
        reject(err);
      }else{
        console.log("ID", data)
        resolve(data['QueryExecutionId']);
      }
    })
  })
}

function getTables(id) {
  console.log("::::: Fetching Existing Tables")
  return new Promise((resolve, reject) => {
    var params = {
      QueryExecutionId: id
    };
    athena.getQueryExecution(params, function(err, data) {
      if (err){
        reject(err)
      } else {
        if(data['QueryExecution']['Status']['State'] === 'SUCCEEDED' || data['QueryExecution']['Status']['State'] === 'CANCELLED' || data['QueryExecution']['Status']['State'] === 'FAILED'){
          //console.log(data);
          let location = data['QueryExecution']['ResultConfiguration']['OutputLocation'];
          let locationParts = location.split("/")
          let reportKey = "queries/" + locationParts[locationParts.length - 1]

          let s3Params = {
              Bucket: process.env.BUCKET,
              Key: reportKey
          };

          s3.getObject(s3Params, function(err, data) {
            if (err) {
              reject(err)
            } else {
              let objectData = data.Body.toString('utf-8');
              let entries = CSV.parse(objectData);
              resolve(entries)
            }
          });
        } else {
          console.log("::::: Query not ready. Trying again")
          setTimeout(function(){ resolve(getTables(id)) }, 3000);
        }
      }
    });
  });
}

async function asyncForEach(array, callback) {
  for (let index = 0; index < array.length; index++) {
    await callback(array[index], index, array);
  }
}

exports.handler = async (event, context, callback) => {

  var partitionedTables = [
    'recommendations',
    'summaries',
  ]

  console.log("::::: Table Partition Repair Service Starting")

  try {
    var queryExecutionId = await getExecutionId();
    var tables = await getTables(queryExecutionId);
    console.log("::::: TABLES:", tables)
    if (tables && tables.toString().length > 0) {
      console.log(":::: Successfully Fetched Existing Tables")
      await asyncForEach(tables, async (table) => {
        var tableName = table.toString();
        if (partitionedTables.includes(tableName)) {
          console.log("::::: REPAIRING TABLE PARTITIONS FOR", tableName)
          await repairTable(tableName);
        } else {
          console.log("::::: SKIPPING NON PARTITIONED TABLE", tableName)

        }
      })
  
      callback(null,{
        statusCode: 200,
        headers: {},
        body: { status: 'ok'}
      })
    } else {
      throw "No Tables Found"
    }

  } catch (e) {
    console.log("::::: ERROR Repairing Tables:", e)
    callback(null,{
      statusCode: 500,
      headers: {},
      body: { status: 'error', error: e}
    })
  }
}