'use strict';
var AWS = require('aws-sdk');
AWS.config.update({ region: 'us-west-1' });
let sns = new AWS.SNS();
const moment = require('moment');
const _ = require('lodash')
const fs = require('fs');
const path = require('path');

var DBCache = null;
var mapResultColdStart = true;

// Utilities
const { getRedshiftClient, executeQueryOnRedshiftRawConn } = require('pplibs/dbUtils')
const { secureLog } = require('pplibs/logUtils')
const { retrieveCache } = require('pplibs/redisUtils')
const { MapError } = require('pplibs/customErrors')
const { fieldNameToYamlFieldname, fieldNameToYamlFilename, getBinCreditProfile } = require('pplibs/mapUtils');

function preloadDBCache(isQA) {
  secureLog("::::: Pre-warming DB cache")
  return new Promise(async (resolve, reject) => {
    try {
      if (isQA) {
        const cachePath = path.join(__dirname, 'db_cache.json')
        if (fs.existsSync(cachePath)) {
          DBCache = JSON.parse(fs.readFileSync(cachePath));
          resolve()
        } else {
          DBCache = await fetchCacheMap();
          fs.writeFileSync(cachePath, JSON.stringify(DBCache))
          resolve()
        }

      } else {
        let uniqueKey = "map_lookup"
        DBCache = await retrieveCache(uniqueKey, fetchCacheMap)

        console.log("::::: Cache Retrieved")
        resolve()
        return;
      }
    } catch (error) {
      reject(error)
    }
  });
}


function fetchCacheMap() {
  return new Promise((resolve, reject) => {
    getRedshiftClient().then(client => {
      client.connect(async function (err) {
        if (err) { console.log(err); reject(err); }
        try {

          /////////// FIELDS //////////////
          let fieldsQuery = `select a.name, g.* from global_attribute_lookup g inner join accounts a on g.account_id = a.id where g.table_name = 'xml_post'`
          let fres = await client.query(fieldsQuery)

          for (let i in fres.rows) {
            let row = fres.rows[i]
            if (DBCache == null) { DBCache = {} }
            if (typeof DBCache[`${row['name']}`] == 'undefined') { DBCache[`${row['name']}`] = {} }
            if (typeof DBCache[`${row['name']}`][`fields`] == 'undefined') { DBCache[`${row['name']}`][`fields`] = {} }

            DBCache[`${row['name']}`][`fields`][`${row['customer_field_name']}`] = {
              field: row['propair_field'],
              split: row['customer_split']
            }
          }



          /////////// PURPOSE DETAIL STATED //////////////
          // Restart //
          let purposeQuery = `select a.name, purpose_detail_stated, "bin_purpose_detail_stated" from purpose_detail_stated_lookup p inner join accounts a on p.account_id = a.id`
          let res = await client.query(purposeQuery)

          for (let i in res.rows) {
            let row = res.rows[i]
            if (DBCache == null) { DBCache = {} }
            if (typeof DBCache[`${row['name']}/loan_purpose`] == 'undefined') { DBCache[`${row['name']}/loan_purpose`] = {} }
            DBCache[`${row['name']}/loan_purpose`][row['purpose_detail_stated']] = row['bin_purpose_detail_stated']
          }


          /////////// PROPERTY TYPE STATED //////////////
          let typeQuery = `select a.name, property_type_stated, "bin_property_type_stated" from property_type_stated_lookup p inner join accounts a on p.account_id = a.id`
          let typeRes = await client.query(typeQuery)

          for (let i in typeRes.rows) {
            let row = typeRes.rows[i]
            if (DBCache == null) { DBCache = {} }
            if (typeof DBCache[`${row['name']}/property_type`] == 'undefined') { DBCache[`${row['name']}/property_type`] = {} }
            DBCache[`${row['name']}/property_type`][row['property_type_stated']] = row['bin_property_type_stated']
          }


          /////////// PROPERTY USE STATED //////////////
          let useQuery = `select a.name, property_use_stated, "bin_property_use_stated" from property_use_stated_lookup p inner join accounts a on p.account_id = a.id`
          let useRes = await client.query(useQuery)

          for (let i in useRes.rows) {
            let row = useRes.rows[i]
            if (DBCache == null) { DBCache = {} }
            if (typeof DBCache[`${row['name']}/property_use`] == 'undefined') { DBCache[`${row['name']}/property_use`] = {} }
            DBCache[`${row['name']}/property_use`][row['property_use_stated']] = row['bin_property_use_stated']
          }


          /////////// PROPERTY STATE STATED //////////////
          let stateQuery = `SELECT state_lookup.property_state_stated, hmda.region_set1, hmda.region_set1_name, hmda.region_set2, hmda.region_set2_name, state_lookup.hmda_state_id
            FROM hmda_region_lookup hmda INNER JOIN property_state_stated_lookup state_lookup ON state_lookup.hmda_state_id = hmda.hmda_state_id;`

          let stateRes = await client.query(stateQuery)
          for (let i in stateRes.rows) {
            let row = stateRes.rows[i]
            if (DBCache == null) { DBCache = {} }
            if (typeof DBCache[`property_state`] == 'undefined') { DBCache[`property_state`] = {} }

            if (typeof DBCache[`property_state`][row['property_state_stated']] === 'undefined') {
              DBCache[`property_state`][row['property_state_stated']] = {
                region_set1: row['region_set1'],
                region_set1_name: row['region_set1_name'],
                region_set2: row['region_set2'],
                region_set2_name: row['region_set2_name'],
                hmda_state_id: row['hmda_state_id']
              }
            }
          }


          /////////// CREDIT PROFILE STATED //////////////
          let creditQuery = `select a.name, source, credit_profile_stated, "bin_credit_profile_stated" from credit_profile_stated_lookup p inner join accounts a on p.account_id = a.id`
          let creditResult = await client.query(creditQuery)

          /////////// SOURCES //////////////
          let sourcesQuery = `select a.name, d.* from source_detail_lookup d inner join accounts a on d.account_id = a.id`
          let sourcesRes = await client.query(sourcesQuery)

          var creditProfiles = _.groupBy(creditResult.rows, row => row['name'])

          Object.keys(creditProfiles).forEach(account => {
            let set = new Set()
            let unique_lookups = {}
            creditProfiles[account].forEach(o => {
              var unique_key = `${o.credit_profile_stated}_${o.bin_credit_profile_stated}`
              if (!set.has(unique_key)) {
                set.add(unique_key)
                unique_lookups[o.credit_profile_stated] = o.bin_credit_profile_stated
              }
            })
            DBCache[`${account}/credit_profiles_no_source`] = unique_lookups

          })

          Object.keys(creditProfiles).forEach(key => {
            creditProfiles[key] = _.groupBy(creditProfiles[key], row => row['source'])
          })
          for (let i in sourcesRes.rows) {
            let row = sourcesRes.rows[i]

            let account = row['name']
            let source = row['source']

            if (DBCache == null) { DBCache = {} }
            if (typeof DBCache[`${account}/credit_profile_sources`] == 'undefined') { DBCache[`${account}/credit_profile_sources`] = {} }
            if (typeof DBCache[`${account}/credit_profile_sources/${fieldNameToYamlFilename(source)}`] == 'undefined') { DBCache[`${account}/credit_profile_sources/${fieldNameToYamlFilename(source)}`] = {} }
            if (typeof DBCache[`${account}/credit_profile_sources/${fieldNameToYamlFilename(source)}`][`source_details`] == 'undefined') { DBCache[`${account}/credit_profile_sources/${fieldNameToYamlFilename(source)}`][`source_details`] = {} }
            if (typeof DBCache[`${account}/credit_profile_sources/${fieldNameToYamlFilename(source)}`][`source_details`][`${fieldNameToYamlFieldname(row['source_detail'])}`] == 'undefined') { DBCache[`${account}/credit_profile_sources/${fieldNameToYamlFilename(source)}`][`source_details`][`${fieldNameToYamlFieldname(row['source_detail'])}`] = {} }

            let sourcesMap = {
              BIN_source: row['bin_source'],
              ialgo: row['ialgo_source'],
              transfer: row['transfer'],
              longform: row['longform'],
              cob: row['cob'],
              self_generated: row['self_generated'],
              source_type: row['source_type'],
              source_channel: row['source_channel'],
              mobile: row['mobile'],
              competition: row['competition'],
              va: row['va'],
              historical_thru201612: row['historical_thru201612'],
              historical_from201701: row['historical_from201701']
            }

            if (typeof DBCache[`${account}/credit_profile_sources/${fieldNameToYamlFilename(source)}`][`source_details`][`${fieldNameToYamlFieldname(row['source_detail'])}`][`${fieldNameToYamlFieldname(row['campaign_group'])}`] == 'undefined') { DBCache[`${account}/credit_profile_sources/${fieldNameToYamlFilename(source)}`][`source_details`][`${fieldNameToYamlFieldname(row['source_detail'])}`][`${fieldNameToYamlFieldname(row['campaign_group'])}`] = {} }
            DBCache[`${account}/credit_profile_sources/${fieldNameToYamlFilename(source)}`][`source_details`][`${fieldNameToYamlFieldname(row['source_detail'])}`][`${fieldNameToYamlFieldname(row['campaign_group'])}`] = sourcesMap

            try {
              if (typeof creditProfiles[account][source] !== 'undefined') {
                for (let j in creditProfiles[account][source]) {
                  let crow = creditProfiles[account][source][j]
                  DBCache[`${account}/credit_profile_sources/${fieldNameToYamlFilename(source)}`][crow['credit_profile_stated']] = crow['bin_credit_profile_stated']
                }
              }
            } catch (e) {
              secureLog(`::::: ERROR retrieving credit profile for Account, Source - ${account}, ${source}`)
            }
          }

          secureLog("::::: Done Generating DBcache")
          client.close()
        } catch (error) {
          secureLog("ERROR Running Query")
          DBCache = null
          secureLog(error)
          reject(error)
        } finally {
          secureLog("Resolving")
          resolve(DBCache)
        }
      })

    }).catch(e => {
      console.log(e)
      reject(e)
    })
  })
}

var propairMap = function (lead, customer, accountConfig, isActiveProspect) {
  let result = {}
  for (let key in lead) {
    if (key != 'recommendation' && key != 'probabilities' && key != 'features') {
      if (DBCache[customer.toLowerCase()]["fields"][key] == undefined) {
        secureLog("Variable " + key + " is missing.")
      } else {
        result[DBCache[customer.toLowerCase()]["fields"][key]['field']] = lead[key]
      }
    }
  }

  for (let key in DBCache[customer.toLowerCase()]["fields"]) {
    if (key.includes(':')) {
      var props = key.split(':');
      if (props[0] in lead) {
        var splitValues = lead[props[0]].split(DBCache[customer.toLowerCase()]["fields"][key]['split'])
        if (splitValues.length > parseInt(props[1], 10)) {
          result[DBCache[customer.toLowerCase()]["fields"][key]['field']] = splitValues[parseInt(props[1], 10)].trim()
        } else {
          result[DBCache[customer.toLowerCase()]["fields"][key]['field']] = 'MISSING';
        }
      }
    }
  }

  //secureLog("::::: PROPAIR LEAD:", result)
  //secureLog("::::: VELOCIFY LEAD:", lead)

  let finalResult = Object.assign(result, getExtraFields(result, lead, customer.toLowerCase(), accountConfig, isActiveProspect))
  return finalResult;
}

var getExtraFields = function (result, lead, account, accountConfig, isActiveProspect) {
  let extraFields = {}

  const customer = account.toUpperCase();
  const sourceDetailRequired = typeof accountConfig['source_detail_required'] !== 'undefined' ? accountConfig['source_detail_required'] : true
  const sourceRequired = typeof accountConfig['source_required'] !== 'undefined' ? accountConfig['source_required'] : true

  secureLog(`::::: MAPPING LOOKUPS FOR ${customer}`)

  //SET 3RDPARTY IF CAMPAGIN GROUP DOES NOT EXIST
  if (typeof result["campaign_group"] === 'undefined' || result["campaign_group"] === null || result["campaign_group"] === '') result["campaign_group"] = '3RDPARTY';

  // Get credit profile fields

  if (typeof result["source"] == 'undefined' || result["source"] == null || result["source"] == '') {
    if (!sourceRequired) {
      result["source"] = "MISSING"
      secureLog(`::::: Source Missing! source_required: ${sourceRequired}. Setting to: MISSING`)
    } else {
      secureLog(`::::: Source Missing! source_required: ${sourceRequired}`)
      throw new MapError({ lead: lead, customer: customer, type: 0, name: `${customer}–Source Missing`, message: `${customer} Source Missing!` })
    }
  }

  if (typeof result["source_detail"] == 'undefined' || result["source_detail"] == null || result["source_detail"] == '') {
    if (!sourceDetailRequired) {
      result["source_detail"] = "MISSING"
      secureLog(`::::: Source Detail Missing! source_detail_required: ${sourceDetailRequired}. Setting to: MISSING`)
    } else {
      secureLog(`::::: Source Detail Missing! source_detail_required: ${sourceDetailRequired}`)
      throw new MapError({ lead: lead, customer: customer, type: 1, name: `${customer}–Source Detail Missing`, message: `${customer} Source Detail Missing!` })
    }
  }

  if (result["source"] === "MISSING" && result["source_detail"] === "MISSING" && result["campaign_group"] === "3RDPARTY") {
    throw new MapError({ lead: lead, customer: customer, type: 2, name: `${customer}– All Sources are missing!`, message: `${customer} Source, Source Detail and Campaign Group are Missing!` })
  }

  let map = DBCache[`${account}/credit_profile_sources/` + fieldNameToYamlFilename(result["source"])];


  if (typeof map == 'undefined' || map == null) {
    secureLog(`::::: ${customer} Source File Not Found!`)
    throw new MapError({ lead: lead, customer: customer, type: 3, name: `${customer}–Source Not Found!`, message: `${customer} Source Not Found! – Source: ${fieldNameToYamlFilename(result["source"])}.` })
  }

  secureLog("::::: FOUND SOURCE")

  //:::: CREDIT PROFILE CALCULATION ::::://
  // First, a credit profile priority list is loaded based in the customer account configuration
  // If the value is credit profile stated and it's parsed as a number, calculate the credit
  // If not, try to find the value in the lookup
  // If another try to search for it within the result variable and use it as credit profile stated


  extraFields["BIN_credit_profile_stated"] = getBinCreditProfile(result, accountConfig["credit_profile_priority_list"],  DBCache[`${account}/credit_profiles_no_source`])

  secureLog(`::::: BIN_CREDIT_PROFILE_STATED IS: ${extraFields["BIN_credit_profile_stated"] ? extraFields["BIN_credit_profile_stated"] : "NULL"}`)


  // Look for Source Detail in the Source Lookup.
  // If Found, Look for Campaign Group in the Source Detail Lookup.
  // If Found, map source detail values to extraFields.

  let sourceDetailField = map["source_details"][fieldNameToYamlFieldname(result["source_detail"])];

  if (typeof sourceDetailField != 'undefined' && sourceDetailField) {
    secureLog("::::: FOUND SOURCE DETAIL FOR SOURCE")
    let campaignGroupField = sourceDetailField[fieldNameToYamlFieldname(result["campaign_group"])];

    if (typeof campaignGroupField != 'undefined' && campaignGroupField) {
      secureLog("::::: FOUND CAMPAIGN GROUP FOR SOURCE DETAIL AND SOURCE")
      secureLog("::::: MAPPING SOURCE DETAIL VALUES")
      secureLog('<<-----------::::::')
      for (let subkey in campaignGroupField) {
        secureLog(`::::: Setting ${subkey} to ${campaignGroupField[subkey]}`)
        extraFields[subkey] = campaignGroupField[subkey]
      }
      secureLog('::::::----------->>')
    } else {
      secureLog(`::::: ${customer} Campaign Group for Source Detail and Source Not Found!`)
      throw new MapError({ lead: lead, customer: customer, type: 4, name: `${customer}–Campaign Group for Source Detail and Source Not Found`, message: `${customer}-Campaign Group for Source Detail and Source Not Found! – Source: ${fieldNameToYamlFilename(result["source"])}, Source Detail: ${fieldNameToYamlFieldname(result["source_detail"])}, Campaign Group: ${fieldNameToYamlFieldname(result["campaign_group"])}.` })
    }

  } else {
    secureLog(`::::: ${customer} Source Detail for Source Not Found!`)
    throw new MapError({ lead: lead, customer: customer, type: 5, name: `${customer}–Source Detail for Source Not Found`, message: `${customer} Source Detail for Source Not Found! – Source: ${fieldNameToYamlFieldname(result["source"])}, Source Detail: ${fieldNameToYamlFieldname(result["source_detail"])}.` })
  }

  if (typeof extraFields["source_type"] === 'undefined')
    extraFields["source_type"] = null;

  if (typeof extraFields["source_channel"] === 'undefined')
    extraFields["source_channel"] = null;

  // Get property type fields
  if (typeof lead["propertyType"] != 'undefined') {
    let map = DBCache[`${account}/property_type`]
    let value = map[fieldNameToYamlFieldname(result["property_type_stated"])] ? map[fieldNameToYamlFieldname(result["property_type_stated"])] : null
    if (value === null) secureLog(`::::: COULD NOT FIND PROPERTY TYPE MAP FOR ${result["property_type_stated"]}`)

    secureLog(`::::: Setting BIN_property_type_stated to ${value}`)
    extraFields["BIN_property_type_stated"] = value
  }

  // Get property use fields
  if (typeof lead["intendedPropertyUse"] != 'undefined') {
    let map = DBCache[`${account}/property_use`]
    let value = map[fieldNameToYamlFieldname(result["property_use_stated"])] ? map[fieldNameToYamlFieldname(result["property_use_stated"])] : null
    if (value === null) secureLog(`::::: COULD NOT FIND PROPERTY USE MAP FOR ${result["property_use_stated"]}`)

    secureLog(`::::: Setting BIN_property_use_stated to ${value}`)
    extraFields["BIN_property_use_stated"] = value
  } else if (isActiveProspect) {
    //TODO: Find Better workaround for this
    secureLog(`::::: Could not find PROPERTY USE. Setting to PrimaryHome`)
    extraFields["property_use_stated"] = "PrimaryHome"
    extraFields["BIN_property_use_stated"] = "PrimaryHome"
  }

  // Get loan purpose fields
  if (typeof lead["loanPurpose"] != 'undefined') {
    let map = DBCache[`${account}/loan_purpose`]
    let value = map[fieldNameToYamlFieldname(result["purpose_detail_stated"])] ? map[fieldNameToYamlFieldname(result["purpose_detail_stated"])] : null
    if (value === null) secureLog(`::::: COULD NOT FIND PURPOSE DETAIL MAP FOR ${result["purpose_detail_stated"]}`)

    secureLog(`::::: Setting BIN_purpose_detail_stated to ${value}`)
    extraFields["BIN_purpose_detail_stated"] = value
  }

  // Get property state fields
  if (typeof lead["propertyState"] != 'undefined') {
    let map = DBCache[`property_state`]
    let fields = map[fieldNameToYamlFieldname(result["property_state_stated"])]
    if (typeof fields != 'undefined') {
      for (let key in fields) {
        secureLog(`::::: Setting ${key} to ${fields[key]}`)
        extraFields[key] = fields[key]
      }
    } else {
      secureLog(`::::: COULD NOT FIND PROPERTY STATE MAPS FOR ${result["property_state_stated"]}`)
    }
  }


  //secureLog(extraFields);
  return extraFields;
}


exports.handler = async function (event, context, callback) {
  // This will allow us to freeze open connections to a database
  context.callbackWaitsForEmptyEventLoop = false;



  if (event.refresh) {
    console.log(":::: Refreshing Cache.")
    try {
      await preloadDBCache();
      console.log(":::: Caches Successfully Refreshed")
      callback(null, {
        statusCode: 200,
        headers: {},
        body: { status: 'ok' }
      })
      return
    } catch (e) {
      console.log("Error while refreshing cache:", e)
      callback(null, {
        statusCode: 500,
        headers: {},
        body: JSON.stringify({ status: e.message })
      })
      return
    }
  }

  if (typeof event.lead === 'undefined' && typeof event.customer === 'undefined') {
    // Just a ping to keep the function warm, ignoring.
    console.log('PING');
    return;
  }

  if (typeof event.accountConfig === 'undefined' || event.accountConfig === null) {
    var e = { message: "No account configuration provided", name: "MapException" }
    console.log(e.message);
    callback(null, {
      statusCode: 500,
      headers: {},
      body: {},
      error: e.message
    })
    return
  }

  let isColdStart = mapResultColdStart;
  var resultMap = null;
  let availabilityMap;
  let errorMsg;
  const { lead, customer, availability } = event;

  const isQA = event.isQA ? event.isQA : false
  if (isQA) secureLog("::::: This is a QA event")

  if (mapResultColdStart) {
    mapResultColdStart = false
  }

  try {
    const isActiveProspect = event.lead['ActiveProspect'] ? true : false
    const leadID = event.lead.id && event.lead.id !== "" ? event.lead.id : event.pp_id
    secureLog("::::: Initializing Secure Logging", null, event.accountConfig, leadID)

    if (isActiveProspect) secureLog("::::: This is an ActiveProspect Lead")

    secureLog("::::: Mapping Service Starting")
    secureLog("::::: EVENT:", event)

    if (!DBCache) {
      secureLog("::::: Local Cache not initialized. Retrieving Cache");
      await preloadDBCache(isQA);
    }

    resultMap = propairMap(lead, customer, event.accountConfig, isActiveProspect);

    if (!resultMap['propair_id'] && event.pp_id) resultMap['propair_id'] = event.pp_id

    if (availability) {
      // availabilityMap = inContactAgentAvailabilityMap(customer, availability);
      availabilityMap = availability.map(a => {
        const { AgentID: velocifyAgentId, Available: available } = a;
        return { 'velocify_agent_id': velocifyAgentId, available };
      });
    }

  } catch (error) {
    secureLog(":::::::::: GOT AN ERROR TO REPORT :::::::::::::::")
    errorMsg = {
      "name": error.name,
      //Remove "\n" character  from string otherwise it produces an error occurs in the JSON.parse
      "stack": error.stack.replace(/(?:\r\n|\r|\n)/g, '\\n'),
      "message": error.message,
      "mappingErrorType": error.mappingErrorType

    }
    error.lead = event.lead
    error.account = event.customer
    secureLog(errorMsg ? errorMsg : error)

    let pError = { trace: event, stack: error }
    if (typeof error['name'] != 'undefined') { pError['name'] = error['name'] }
  } finally {

    if (resultMap) resultMap.coldstart = isColdStart


    callback(null, {
      statusCode: 200,
      headers: {},
      body: { map: resultMap, availability: availabilityMap },
      error: errorMsg
    })

    secureLog("ProPair Map generated:", resultMap)
  }

  secureLog("::::: Mapping Service Ended")
}
