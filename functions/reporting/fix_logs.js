'use strict';
var AWS = require('aws-sdk');
var DOC = require("dynamodb-doc");
AWS.config.update({region: 'us-west-1'});
let s3 = new AWS.S3({ region: 'us-west-1' });
let ddb = new AWS.DynamoDB({apiVersion: '2012-10-08'});
let docClient = new DOC.DynamoDB(ddb);
let sns = new AWS.SNS();



function runScan(lCallback, fCallback){
    console.log("::::: Reading Leads from DDB")
    let params = {
        TableName: process.env.LEADS_TABLE
    };

    let allUnprocessedComments = []

    ddb.scan(params, function(err, data) {
        if (err) {
            lCallback(err)
        } else {
            allUnprocessedComments = allUnprocessedComments.concat(data.Items);

            if(data.LastEvaluatedKey || allUnprocessedComments.length > 5000) {
                params.ExclusiveStartKey = data.LastEvaluatedKey;
                runScan(fCallback);
            } else {
                fCallback(err,allUnprocessedComments);
            }
        }
    });
}


var allKeys = [];
function listAllKeys(prefix, token, cb)
{
  var opts = { Bucket: "pro-pair-serverless", Prefix: prefix };
  if(token) opts.ContinuationToken = token;

  s3.listObjectsV2(opts, function(err, data){
    allKeys = allKeys.concat(data.Contents);

    if(data.IsTruncated)
      listAllKeys(prefix, data.NextContinuationToken, cb);
    else
      cb();
  });
}

function getLeadContent(key){
    return new Promise((resolve, reject) => {
        var params = { Bucket: "pro-pair-serverless",  Key: key };
        s3.getObject(params, function(err, data) {
            if (err){
                console.log(err, err.stack); // an error occurred
            }else{
                let objectData = data.Body.toString('utf-8').split("\n").map(x => JSON.parse(x));
                resolve(objectData)
            }
        });
    });
}

function validateAgainstDynamo(tableName, lead, s3_key){
    return new Promise((resolve, reject) => {
        let sortedEntries = lead.sort(function(a,b) {return (a.agent_id > b.agent_id) ? 1 : ((b.agent_id > a.agent_id) ? -1 : 0);} ); 
        let firstEntry = sortedEntries[0];

        var params = {
            TableName : 'pro-pair-serverless-LeadsTable-1COUF4ZKGU428',//tableName,
            KeyConditionExpression: "#id = :ids",
            ExpressionAttributeNames:{
                "#id": "id"
            },
            ExpressionAttributeValues: {
                ":ids": firstEntry.account_lead_id.toString()
            }
        };

        ddb.query(params, function(err, data) {
            if (err || data.Items.length == 0) {
                if(err){console.log('Error: ' + err)}else{console.log("-----> LEAD NOT FOUND INDB: " + firstEntry.account_lead_id.toString())}
            } else {
                //console.log(JSON.stringify(data))
                //resolve( data.Item.name.S );
                let databaseLead    = data.Items[0]
                let originalPayload = JSON.parse(databaseLead.payload)
                let originalAgent   = null

                if(firstEntry.reassignment == 0){// || typeof originalPayload.branchRecommendation == 'undefined'){
                    let originalRec = originalPayload.recommendation.split(":")
                    originalAgent = originalRec[1].match(/\d+/) ? originalRec[1].match(/\d+/)[0] : null
                }else if(originalPayload.recommendationForBranch){
                    let originalRec = originalPayload.recommendationForBranch.split(":")
                    originalAgent = originalRec[1].match(/\d+/) ? originalRec[1].match(/\d+/)[0] : null
                }else{
                    console.log("Error: Lead " + firstEntry.account_lead_id + " does not have a recommendation for reassignment " + firstEntry.reassignment + "  in ddb.")
                }

                if(firstEntry.agent_id == originalAgent){
                    //console.log("Lead " + firstEntry.account_lead_id + " is good, " + firstEntry.agent_id + " and " + originalAgent + " match, skipping.")
                    resolve({ status: 'ok' })
                }else{
                    //console.log("Lead " + firstEntry.account_lead_id + " NEEDS FIXING, " + firstEntry.agent_id + " and " + originalAgent + " DO NOT match.")
                    resolve({ status: 'bad', lead: lead,  originalPayload: originalPayload })
                }
            }
        });
    });
}

// function findAvailableOfficers(customer, leadParams, lead){
//     console.log("Trying to find officer for customer: " + customer + " and ID: " + leadParams.firstAssignmentDistributionUser)
//     return new Promise((resolve, reject) => {
//         let key = 'lo-profiles/' + customer.toLowerCase() + '/loan_officers.yml';
//         console.log("Trying to download file " + key)
//         let params = {Bucket: 'pro-pair-serverless', Key: key};
//         s3.getObject(params, function (error, data) {
//             if(error) {
//                 reject({code: 404, message: "not found"});
//             } else {
//                 let allOfficers = YAML.parse(data.Body.toString('utf8'));
//                 let assignedOfficer = null;
//                 let branchOfficers = {}

//                 Object.keys(allOfficers).forEach(function(key,index) {
//                     let officer = allOfficers[key];
//                     if(officer.name_velocify.toUpperCase() === leadParams.firstAssignmentDistributionUser.toUpperCase()){
//                         assignedOfficer = officer;
//                         assignedOfficer['agent_id'] = key;
//                     }
//                 });

//                 if(assignedOfficer === null){
//                     console.log("Officer " + leadParams.firstAssignmentDistributionUser + "Not found, exiting.")
//                     reject({code: 404, message: "not found"});
//                 }else{
//                     console.log(assignedOfficer)
//                     Object.keys(allOfficers).forEach(function(key,index) {
//                         let officer = allOfficers[key];
//                         if( officer.branch ==  assignedOfficer.branch) {
//                             branchOfficers[key] = officer;
//                         }
//                     });
    
//                     resolve({customer: customer, leadParams: leadParams, lead: lead, allOfficers: allOfficers, assignedOfficer: assignedOfficer, branchOfficers: branchOfficers })
//                 }
//             }
//         });
//     });
// }

function fixLeadLog(lead, originalPayload, s3Key){
    console.log("Uploading fix to lead log located at " + s3Key);
    return new Promise((resolve, reject) => {
        let type = lead[0].reassignment == 0 ? "NEWLEAD" : "REASSIGNMENT"
        let leadParams = null

        if(type == "NEWLEAD"){
            leadParams = {
                Message: JSON.stringify( { lead: {lead: originalPayload, customer: lead[0].account }, reassignment: false, s3Key: s3Key, originalDateString: lead[0].created_at } ),
                Subject: "Log Lead",
                TopicArn: process.env.TOPIC_LOG_LEAD
            };
        }else{
            leadParams = {
                Message: JSON.stringify( { lead: {lead: { payload: originalPayload }, customer: lead[0].account }, reassignment: true, s3Key: s3Key, originalDateString: lead[0].created_at, previousRecommendation: originalPayload.trimmedRecommendation } ),
                Subject: "Log Lead",
                TopicArn: process.env.TOPIC_LOG_LEAD
            };
        }

        //console.log(leadParams)
        
        //resolve();

        if(leadParams && lead[0].account_lead_id != 1){
            sns.publish(leadParams, function(err, data) {
                if (err) {
                    console.log(err)
                    console.log(JSON.stringify(err))
                    reject({code: 500, message: "server error"});
                }else{
                    resolve();
                }
            });
        }else{
            console.log("Ignoring for now, this is a reassignment.")
            resolve();
        }
    });
}


exports.handler = (event, context, callback) => {
    console.log("::::: Log Fixing Service Starting")

    let goodLeads = 0
    let badLeads = 0

    let prefix = "logs/recommendations/dt=2018-06-19"
    listAllKeys(prefix, null, function(){
        console.log(allKeys.length + " leads found under " + prefix);

        for(let i in allKeys){
        //for(let i=0; i<3; i++){
            let key = allKeys[i].Key;
            getLeadContent(key).then(function(lead){
                return validateAgainstDynamo(process.env.LEADS_TABLE, lead, key);
            }).then(function(result){
                if(result.status == 'bad'){
                    badLeads++
                    return fixLeadLog(result.lead, result.originalPayload, key);
                }else{
                    goodLeads++
                    return new Promise((resolve, reject) => {
                        resolve()
                    }); 
                }
            }).then(function(){
                console.log(goodLeads + " Good Leads")
                console.log(badLeads + " Bad Leads")
            }).catch(function(error){
                console.log(JSON.stringify(error));
            });
        }
    })

    // runScan(function(error){
    //     callback(null,{
    //         statusCode: 500,
    //         headers: {},
    //         body: JSON.stringify( { error: error.stack } )
    //     });
    // }, function(err, items){
    //     if(err){
    //         callback(null,{
    //           statusCode: 500,
    //           headers: {},
    //           body: JSON.stringify( err )
    //         });
    //     }else{
    //         console.log(items.length)
    //     }
    // });
  
}
