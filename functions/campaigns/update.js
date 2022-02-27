var AWS = require('aws-sdk');
AWS.config.update({ region: 'us-west-1' });
let ssm = new AWS.SSM({ apiVersion: '2014-11-06' });
let sns = new AWS.SNS();
const { reportToRollbar, ProPairError } = require('pplibs/customErrors')

var rp = require('request-promise');
var XLSX = require('xlsx')
var fs = require('fs');
let pg = require('pg');
let moment = require('moment');
const Pool = require('pg-pool')
var pool;


var workdocs = new AWS.WorkDocs({
  apiVersion: '2016-05-01',
  region: 'us-west-2'
});

const _ = require('lodash');
var workDocFiles = require(`./workdocs-${process.env.ENV}.json`)
var workDocParser = require('./workdocParser');
var accountsFile = require('./account');


// Utilities
const { getRedshiftClient, executeQueryOnRedshiftRawConn } = require('pplibs/dbUtils')
const { secureLog, getConfigurations } = require('pplibs/logUtils')

var accountIds;

function getFile(file) {
  return new Promise((resolve, reject) => {
    let documentId = file.id
    var params = {
      DocumentId: documentId
    };
    workdocs.getDocument(params, function (err, data) {
      if (err) {
        console.log(err);
        reject(err)
      } else {
        console.log("::::: Fetched Latest File Version ")
        resolve({ documentId: documentId, versionId: data['Metadata']['LatestVersionMetadata']['Id'] })
      }
    });
  });
}

function getLatestVersion(documentId, versionId, file) {
  return new Promise((resolve, reject) => {
    var params = {
      DocumentId: documentId,
      VersionId: versionId,
      Fields: 'SOURCE'
    };
    workdocs.getDocumentVersion(params, function (err, data) {
      if (err) {
        console.log(err)
        reject(err)
      } else {
        console.log("::::: Fetched Latest File Version URL ")
        //downloadLatestVersion(data['Metadata']['Source']['ORIGINAL'], file)
        resolve(data['Metadata']['Source']['ORIGINAL'])
      }
    });
  });
}

function downloadLatestVersion(url, file) {
  return new Promise((resolve, reject) => {
    const options = {
      url: url,
      encoding: null
    };
    try {
      rp.get(options).then(function (res) {
        const buffer = Buffer.from(res, 'utf8');
        const path = `/tmp/${file['name']}`
        fs.writeFileSync(path, buffer);
        console.log("::::: Downloaded Latest File Version ")
        resolve(path)
      });
    } catch (e) {
      console.log(e);
    }
  });
}

function insertToDB(rows, file, currentVer) {
  return new Promise((resolve, reject) => {
    getRedshiftClient().then(client => {
      client.connect(async function (err) {
        if (err) { console.log(err); reject(err); }
        try {
          if (rows.length == 0) {
            console.log("::::: There isn't any row to insert");
            resolve("::::: There isn't any row to insert");
          }
  
          let newVer = file.updateTable + '_' + moment().format('YYYY_MM_DD_HHmmss')
          console.log(`::::: Inserting updated data into new Table Version: ${newVer}`);
  
          // Begin transaction and lock current table version
          // Then create new table version
          let sql =`
            begin read write;
            lock ${currentVer.table};
            create table "${newVer}" (like ${currentVer.table});
          `
  
          // Insert data into new table
          sql += getInsertStatement(rows, newVer, file.updateColumns, file.isGlobal);
  
          // Replace view to point to new table
          sql += `create or replace view ${file.updateTable} as select * from public.${newVer} with no schema binding;`
  
          // Drop older version
          sql += `drop table ${currentVer.table};`
  
          sql += 'end transaction;'
  
          await client.query(sql);
          client.close()
          resolve("::::: Done with DB updates")
        } catch(e) {
          reject(e)
        }
      })
    }).catch(error => {
      console.log("::::: ERROR inserting lookups")
      reject(error)
    });
  });

}

function getlatestTableVer(table) {
  return new Promise(async (resolve, reject) => {
    let result = await executeQueryOnRedshiftRawConn(`select tablename from pg_tables where tablename like '%${table}%' order by tablename;`)
    if (result.rows.length > 0) {

      let tables = result.rows.map(i => {
        let date = i.tablename.match(/\d{4}_(0[1-9]|1[012])_(0[1-9]|[12][0-9]|3[01])_(0[1-9]|1[0-9]|2[0-3])_?(0[1-9]|[12345][0-9])_?(0[1-9]|[12345][0-9])/)
        if (date) {
          date = moment(date[0], 'YYYY_MM_DD_HHmmss')
          if (!date.isValid()) date = null
        } else {
          date = i.tablename.match(/\d{4}_(0[1-9]|1[012])_(0[1-9]|[12][0-9]|3[01])/)
          if (date) {
            date = moment(date[0], 'YYYY_MM_DD_HHmmss')
            if (!date.isValid()) date = null
          }
        }
        return {table: i.tablename, date}
      })

      tables = tables.filter(t => t.date ).sort((a,b) => a.date - b.date)

      let latestVer = tables[tables.length - 1]

      resolve(latestVer)

    } else {
      reject(`Table: ${table} does not exist!`)
    }
  });

}

function getAccountIds(configs) {
  //if (process.env.ENV === 'staging') return accountsFile;
  return new Promise((resolve, reject) => {
    getRedshiftClient().then(client => {
      client.connect(err => {
        if (err) { console.log(err); reject(err); }
        client.query(`SELECT id, name FROM accounts;`)
          .then(res => {
            client.close()
            var accounts = []

            res.rows.forEach(r => {
              var aConfig = configs[r.id]
              if (aConfig) {
                let trigger = typeof aConfig['use_lookups'] !== 'undefined' ? aConfig['use_lookups'] : true
                if (trigger) {
                  accounts.push({ id: parseInt(r.id, 10), name: r.name })
                } else {
                  console.log(`::::: Account: ${r.name} --> use_lookups: false`)
                }
              }
            })

            resolve(accounts);
          }).catch(error => {
            console.log("ERROR", error);
            reject(error)
          })
      });

    });
  });
}

function getInsertStatement(rows, table, columns, isGlobal = false) {

  // Build query based on parsed data
  // In order to populate the temporary table

  let insertStatement = `INSERT INTO ${table} (`;
  if (!isGlobal) insertStatement += `"account_id",`;
  insertStatement += `${_.map(columns, (a) => `"${a.name}"`)} ) VALUES`
  insertStatement += _.map(rows, a => {
    let row = `(`;
    if (!isGlobal) row += `${a.account_id},`;
    row += columns.map(function (c) {
      let value = a[c.name] === null || typeof a[c.name] !== "string" ? a[c.name] : `$$${a[c.name].replace(/\$/gi, '')}$$`
      return `${value}`;
    });
    row += ')'
    return row;
  });
  insertStatement += ';';

  return insertStatement;

}

function validateRows(rows, file, type = 'insert') {
  var requiredValues;
  if (type === 'insert') {
    console.log(`:::::::: Validating rows for insert from file ${file.fileName}`);
    requiredValues = file.updateColumns.filter(a => a.isRequired == true);
  } else {
    console.log(`:::::::: Validating rows for update from file ${file.fileName}`);
    requiredValues = file.columns
  }
  if (requiredValues.length > 0) {
    requiredValues.forEach(r => {
      let removed = _.remove(rows, row => {
        if (typeof row[r.name] !== 'undefined') {
          return row[r.name] == null;
        }
      });
      if (removed.length > 0) console.log(`:::::::: Removed ${removed.length} rows since having null values in a required field`)
    });
  }

  return rows;
}

function getNewCombinations(existingCombinations, file) {
  return new Promise((resolve, reject) => {
    console.log("::::: Fetching New Unique Combinations")
    let insertStatement = getInsertStatement(existingCombinations, file.table, file.columns, file.isGlobal);
    let truncateStatement = `TRUNCATE TABLE ${file.table};`
    let functionStatement = `SELECT * FROM ${file.function};`;


    // Clear table and insert parsed data from the lookup
    // Afterwards, retrieve new combinations that are non existant in the lookup table
    // Finally, clear table again and return combinations

    try {
      getRedshiftClient().then(client => {
        client.connect(async function (err) {
          if (err) { console.log(err); reject(err); }
          console.log('::::: Truncating Temporary table');
          await client.query(truncateStatement);
          if (existingCombinations && existingCombinations.length) {
            console.log('::::: Inserting Existing Combinations Into Temporary table');
            const resInsert = await client.query(insertStatement);
          }
          console.log('::::: Running Comparison Function');
          const resCombinations = await client.query(functionStatement);
          console.log('::::: Truncating Temporary table');
          await client.query(truncateStatement);
          client.close()
          resolve(resCombinations.rows);

        });
      });
    } catch (e) {
      console.log("ERROR", e);
      reject(e)
    }
  });


}


function initiateUpload(file) {
  return new Promise((resolve, reject) => {
    // Initiate code goes HERE
    var params = {
      ParentFolderId: file.parentFolderId,
      Id: file.id,
      Name: file.fileName,
      ContentType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    };
    workdocs.initiateDocumentVersionUpload(params, function (err, data) {
      if (err) {
        console.log(err)
        reject(err)
      } else {
        console.log("::::: Initiated Filed Upload ")
        resolve({ url: data['UploadMetadata']['UploadUrl'], version: data['Metadata']['LatestVersionMetadata']['Id'] })
      }
    });
  });
}

function uploadFile(url, path) {
  return new Promise((resolve, reject) => {
    rp({
      method: 'PUT',
      uri: url,
      headers: {
        'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'x-amz-server-side-encryption': 'AES256'
      },
      body: fs.readFileSync(path)
    }).then((r) => {
      resolve(r);
    }).catch((err) => {
      reject(err);
      console.log('err', err)
    })
  });
}

function activateFile(file, version) {
  return new Promise((resolve, reject) => {
    console.log(":::: Activating File")
    var params = {
      DocumentId: file.id,
      VersionId: version,
      VersionStatus: 'ACTIVE'
    };
    workdocs.updateDocumentVersion(params, function (err, data) {
      if (err) {
        reject(err)
      } else {
        resolve(data)
      }
    });
  });
}

/**
 * Get all the files from a folder from workdocs
 */
function getFolderContent(folderId) {
  return new Promise((resolve, reject) => {
    var params = {
      FolderId: folderId
    };

    workdocs.describeFolderContents(params, function (err, data) {
      if (err) {
        console.log(err, err.stack)
        reject(err);
      } else {
        resolve(data.Documents);
      }
    });
  });
}

function calculateCredit(score) {
  var creditScore = String(score).replace(/[^0-9]/gi, "")
  //If credit score is above 1000, calculate the average 
  creditScore = parseInt(creditScore, 10) < 1000 ? parseInt(creditScore, 10) : (parseInt(creditScore.slice(0, 3), 10) + parseInt(creditScore.slice(3, 6), 10)) / 2
  var creditProfile;
  if (creditScore > 0) creditProfile = 'Poor'
  if (creditScore >= 620) creditProfile = 'Fair'
  if (creditScore >= 660) creditProfile = 'Good'
  if (creditScore >= 720) creditProfile = 'Excellent'
  if (creditScore == 0) creditProfile = null
  return creditProfile;
}

function inferCreditProfileStated(records) {
  for (let i in records) {
    let credit_profile = records[i].credit_profile_stated
    credit_profile = String(credit_profile)
    try {
      if (typeof records[i].BIN_credit_profile_stated === "undefined" || records[i].BIN_credit_profile_stated == null || records[i].BIN_credit_profile_stated == "") {
        if (typeof credit_profile !== "undefined" && credit_profile != null && credit_profile != "") {
          let score = credit_profile.replace(/\D/g, '');
          if (score.length < 4 && score.length != 0) {
            let res = calculateCredit(score)
            if (res != null) records[i].BIN_credit_profile_stated = res
          }
          else if (score.length >= 4) {
            let range = score
            let splitIndex = range.length / 2
            let firstLimit = parseInt(range.substring(0, splitIndex), 10)
            let secondLimit = parseInt(range.substring(splitIndex, range.length))
            let finalScore = Math.round((firstLimit + secondLimit) / 2)
            let res = calculateCredit(finalScore.toString())
            if (res != null) records[i].BIN_credit_profile_stated = res
          }
          else {
            let score_name = credit_profile.toLowerCase()

            score_name = score_name.replace(/[^A-Za-z]+/g, '');

            if (score_name == "good") {
              records[i].BIN_credit_profile_stated = 'Good'
            }
            else if (score_name == "fair") {
              records[i].BIN_credit_profile_stated = 'Fair'
            }
            else if (score_name == "poor") {
              records[i].BIN_credit_profile_stated = 'Poor'
            }
            else if (score_name == "excellent") {
              records[i].BIN_credit_profile_stated = 'Excellent'
            }
            else {
              continue
            }
          }
        }
      }

    } catch (error) {
      console.log(error)
    }
  }
  return records
}
exports.update_database = async function (event, context, callback) {
  try {
    const forceUpdate = event.force_update
    context.callbackWaitsForEmptyEventLoop = false;

    const configs = await getConfigurations()
    let account_ids = await getAccountIds(configs);


    let folderContent = await getFolderContent(workDocFiles.folderId);
    if (forceUpdate) console.log(`:::::: Force Update triggered`)
    for (i in folderContent) {
      let file = _.find(workDocFiles.workdocs, a => a.fileName == folderContent[i].LatestVersionMetadata.Name);
      if (!file) continue;
      let tableVersion = await getlatestTableVer(file.updateTable)
      if (folderContent[i]['ModifiedTimestamp'] > tableVersion.date || forceUpdate) {
        file['id'] = folderContent[i]['Id'];

        console.log(`:::::::::::::::::: Detected Update! Updating - ${file.fileName}. `);
        console.log(`:::::::::::::::::: Current Version: ${tableVersion.date}`)
        console.log(`:::::::::::::::::: Last Modification: ${folderContent[i]['ModifiedTimestamp']}`)

        let result = await getFile(file)
        let url = await getLatestVersion(result.documentId, result.versionId, file)
        let path = await downloadLatestVersion(url, file)
        let dbRows = await workDocParser.read(path, file.updateColumns, account_ids, file.isGlobal);
        dbRows = validateRows(dbRows, file, 'insert');
        await insertToDB(dbRows, file, tableVersion)
      } else {
        console.log(`:::::::::::::::::: ${file.fileName} - Nothing to update. Current Version: ${tableVersion.date}. Last Modification: ${folderContent[i]['ModifiedTimestamp']}`)
      }
    }
    console.log(`:::::::::::::::::: Done with DB Updates`)
  } catch (e) {
    console.log(":::::::::: GOT AN ERROR TO REPORT :::::::::::::::")
    console.log(e)
    let error = new ProPairError(e , "UpdateCampaigns")
    await reportToRollbar(error, process.env.TOPIC_ROLLBAR, "UpdateCampaigns")
    
    callback(null, {
      statusCode: e.code || 500,
      headers: {},
      body: JSON.stringify({ status: e.message || e})
    });
  }

}


exports.update_lookups = async function (event, context, callback) {

  console.log("::::: Invoking UpdateLookupsForCampaigns Function");
  try {
    context.callbackWaitsForEmptyEventLoop = false;

    const configs = await getConfigurations()
    let account_ids = await getAccountIds(configs);

    // Iterate through each file.
    // Fetch the file's last version id.
    // Use the version id to retrieve the latest download url.
    // Download the file and parse it.
    // Use parsed information to compare and retrieve new combinations from the database.
    // Insert new combinations into the workdoc.
    // Initiate the upload via creating a new version object and retrieving the upload url.
    // Upload the updated file and activate the document.

    let folderContent = await getFolderContent(workDocFiles.folderId);
    for (i in folderContent) {

      let file = _.find(workDocFiles.workdocs, a => a.fileName == folderContent[i].LatestVersionMetadata.Name);
      if (!file) continue;
      if (file.isGlobal) continue;
      if (file.name == 'global.xlsx') continue;
      file['id'] = folderContent[i]['Id'];
      file['parentFolderId'] = workDocFiles.folderId;


      console.log(":::::::::::::::::::::::::: Updating", file.fileName);

      let result = await getFile(file);
      let url = await getLatestVersion(result.documentId, result.versionId);
      let path = await downloadLatestVersion(url, file);
      let data = await workDocParser.read(path, file.columns, account_ids);
      let rows = validateRows(data, file, 'update');
      let combinations = await getNewCombinations(rows, file);
      if (!combinations) continue;
      if (combinations.length > 0) {
        console.log(`:::::: ${combinations.length} New Combinations Successfully Retrieved`)
        if (file.name == 'credit_profile.xlsx') {
          combinations = inferCreditProfileStated(combinations)
          await workDocParser.write(path, combinations, account_ids, file.updateColumns, file.updateColumns);
        }
        else {
          await workDocParser.write(path, combinations, account_ids, file.columns, file.updateColumns);
        }
        let upload = await initiateUpload(file);
        await uploadFile(upload.url, path);
        await activateFile(file, upload.version);
        console.log("::::: Successfully Updated ", file.fileName);
      } else {
        console.log("::::: No new unique combinations were found for", file.fileName);
      }

    }

    console.log(":::::::::::::::::::::::::: Finished Updating Lookups");
  } catch (e) {
    console.log(":::::::::: GOT AN ERROR TO REPORT :::::::::::::::")
    console.log(e)
    let error = new ProPairError(e , "UpdateLookupsForCampaigns")
    await reportToRollbar(error, process.env.TOPIC_ROLLBAR, "UpdateLookupsForCampaigns")
  }

}

function updateWorkDocFile(production, staging, fileName) {

  if (fileName === "source_detail.xlsx") {
    let result = Array.from(production)
    _.forEach(staging, element => {
      const found = result.some(el => el.source === element.source && el.source_detail === element.source_detail &&
        el.campaign_group === element.campaign_group && el.account_id === element.account_id);
      if (!found) {
        result.push(element)
      }
    })
    return result
  }
  else {
    let result = new Set()

    _.forEach(production, element => {
      result.add(JSON.stringify(element))
    })
    _.forEach(staging, element => {
      result.add(JSON.stringify(element))
    })

    let updated = Array.from(result)
    updated = updated.map(JSON.parse)
    return updated
  }
}

exports.update_staging_folder = async function (event, context, callback) {

  // Iterate through each file.
  // Fetch the file's last versions for both staging and production.
  // Use parsed information to compare and retrieve unique combinations from both files.
  // Insert combinations into the workdoc.
  // Initiate the upload via creating a new version object and retrieving the upload url.
  // Upload the updated file and activate the document.
  console.log("::::: Invoking Update Staging WorkDocs Folder Function");
  if (process.env.ENV === "staging") {
    let productionFolder = require(`./workdocs-production.json`)
    try {
      context.callbackWaitsForEmptyEventLoop = false;

      const configs = await getConfigurations()
      let account_ids = await getAccountIds(configs);

      let folderContentStaging = await getFolderContent(workDocFiles.folderId);
      let folderContentProduction = await getFolderContent(productionFolder.folderId);
      for (let i in folderContentProduction) {
        let productionFile = _.find(productionFolder.workdocs, a => a.fileName == folderContentProduction[i].LatestVersionMetadata.Name);
        if (!productionFile) continue;
        if (productionFile.isGlobal) continue;
        if (productionFile.name == 'global.xlsx') continue;
        productionFile['id'] = folderContentProduction[i]['Id'];
        productionFile['parentFolderId'] = productionFolder.folderId;


        console.log(":::::::::::::::::::::::::: Downloading production lookups", productionFile.fileName);

        let productionResult = await getFile(productionFile);
        let productionUrl = await getLatestVersion(productionResult.documentId, productionResult.versionId);
        let productionPath = await downloadLatestVersion(productionUrl, productionFile);
        let productionData = await workDocParser.read(productionPath, productionFile.updateColumns, account_ids);

        let stagingFile = _.find(workDocFiles.workdocs, a => a.fileName == productionFile.fileName);
        let stagingFileId = _.find(folderContentStaging, a => a.LatestVersionMetadata.Name == productionFile.fileName).Id;
        stagingFile['id'] = stagingFileId;
        stagingFile['parentFolderId'] = workDocFiles.folderId;

        console.log(":::::::::::::::::::::::::: Downloading staging lookup", stagingFile.fileName);

        let stagingResult = await getFile(stagingFile);
        let stagingUrl = await getLatestVersion(stagingResult.documentId, stagingResult.versionId)
        let stagingPath = await downloadLatestVersion(stagingUrl, stagingFile);
        let stagingData = await workDocParser.read(stagingPath, stagingFile.updateColumns, account_ids);
        let updatedData = updateWorkDocFile(productionData, stagingData, stagingFile.name)

        await workDocParser.write(stagingPath, updatedData, account_ids, stagingFile.updateColumns, true);
        let upload = await initiateUpload(stagingFile);
        await uploadFile(upload.url, stagingPath);
        await activateFile(stagingFile, upload.version);
        console.log("::::: Successfully Updated ", stagingFile.fileName);

      }
    }
    catch (err) {
      console.log(err)
    }

  }
  else {
    console.log("Production environment, No need to run")
  }
}