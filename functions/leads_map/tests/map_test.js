require('dotenv').config();
const map = require("../map")


const events = [
   [
      {
         "lead":{
            "id":"5033378",
            "actionCount":"0",
            "campaign":"LT - HE",
            "campaignGroup":"RETIRED",
            "creditProfile":"VERYGOOD",
            "creditScore":"",
            "dateAdded":"6/1/2020 4:33 PM",
            "dayPhone":"U2FsdGVkX1+w6jOdaH/xi7VCsAlxGjscYyTWmaBfmUM=",
            "email":"U2FsdGVkX19jJ/w0uXpx/lhckRHeQXSegfg42NQPhVWTSnnSRkTE3ja+gdzIJ+kJ",
            "eveningPhone":"U2FsdGVkX18ubWjKUY1L4XSSo20SbtZ06Zs3yUU38fg=",
            "existingHomeValue":"$400,000.00",
            "firstName":"U2FsdGVkX19Lyr4FzVv1D/w14eqWePwB+LyF/tXBQy8=",
            "intendedPropertyUse":"Primary Residence",
            "lastDuplicate":"",
            "lastName":"U2FsdGVkX1/LrOZrlkuGuYGY2RME81mwp0YwG1PjmE4=",
            "loanAmount":"$95,000.00",
            "loanPurpose":"Home Equity",
            "propertyAddress":"U2FsdGVkX1+ffGezkMUBYk9PynUesSIirZSbtszp8QA=",
            "propertyState":"CA",
            "propertyType":"Single Family",
            "propertyZipCode":"95993",
            "purchasePrice":"",
            "LeadProviderName":"QUINSTREET",
            "IsMilitary":"No",
            "CIS":"",
            "FICO":"",
            "LeadiDConsumerVelocity12Hours":"",
            "LeadiDConsumerVelocity1Hour":"",
            "LeadiDConsumerVelocity5Min":"",
            "LeadiDConsumerVelocityPastDay":"",
            "LeadiDConsumerVelocityPastWeek":"",
            "LeadiDDataIntegrity":"",
            "LeadiDDeviceFrequency12Hours":"",
            "LeadiDDeviceFrequency1Hour":"",
            "LeadiDDeviceFrequency5Min":"",
            "LeadiDDeviceFrequencyPastDay":"",
            "LeadiDDeviceFrequencyPastWeek":"",
            "LeadiDAge":"",
            "LeadiDDuplicationEmailDupeCheck":"",
            "LeadiDDuplicationLeadiDDupeCheck":"",
            "LeadiDDuration":"",
            "LeadiDLeadLineageTotalEntities":"",
            "LeadiDLeadLineageTotalHops":"",
            "LeadiDLeadVelocity12Hours":"",
            "LeadiDLeadVelocity1Hour":"",
            "LeadiDLeadVelocity5Min":"",
            "LeadiDLeadVelocityPastDay":"",
            "LeadiDLeadVelocityPastWeek":""
         },
         "customer":"bbmc",
         "availability":null,
         "accountConfig":{
            "write_to_velocify":true,
            "CR_bins":{
               "REFI":[
                  "Array"
               ],
               "PUR":[
                  "Array"
               ]
            },
            "use_custom_rank":true,
            "data_incremental_update_leads":true,
            "CUSTOM_RANK_THRESHOLD_REFI":0.14,
            "AB_ALGO2_SUFFIX_NOLEADID":":000303a",
            "data_incremental_update_calls":false,
            "domain_tld_list":[
               "BADDOMAIN",
               "COM",
               "EDU ",
               "GOV",
               "HARDBOUNCE",
               "NET",
               "OPTOUT",
               "ORG",
               "OTHER",
               "US"
            ],
            "name":"bbmc",
            "extra_fields_to_write":[
               [
                  "Object"
               ]
            ],
            "AB_ALGO1_SUFFIX_NOLEADID":":000203a",
            "accepted_split_fields":[
               "purchase_price_stated",
               "home_value_stated",
               "loan_amount_stated",
               "down_payment",
               "cash_out_stated"
            ],
            "data_incremental_update_events":true,
            "RECOMMENDATION_THRESHOLD":0.5,
            "run_rec_expansion":false,
            "id":3,
            "pii_fields":[
               "firstname",
               "firstnamestated",
               "lastname",
               "lastnamestated",
               "email",
               "dayphone",
               "eveningphone",
               "emailstated",
               "velocify_password",
               "velocify_username",
               "propertyaddress"
            ],
            "run_recommendations":false,
            "AB_ALGO1_SUFFIX":":000402a",
            "AB_ALGO2_SUFFIX":":000402a",
            "custom_rank":{
               "triggers":[
                  "Object"
               ],
               "variables":[
                  "Object"
               ]
            },
            "RECOMMENDATION_THRESHOLD_BRANCH":0.7,
            "los":{
               "date_close_field":"milestone_date_docsigning",
               "match_type":"on_loan_id",
               "email_match":false
            },
            "event_data_fromNowMinutes":16,
            "CUSTOM_RANK_THRESHOLD_PUR":0.09,
            "rookie_rollout_months_timeframe":0,
            "rookie_rollout2":{
               "triggers":[
                  "Object"
               ],
               "variables":[
                  "Object"
               ]
            },
            "credit_profile_priority_list":[
               "credit_score_stated",
               "credit_score_range_stated",
               "credit_profile_stated"
            ],
            "RECOMMENDATION_THRESHOLD_WEEKEND":0.7,
            "AB_ALGO_COUNT":2,
            "use_prospect_matching":false,
            "rookie_rollout":false,
            "prospect_matching":{
               "triggers":[
                  "Object"
               ],
               "variables":[
                  "Object"
               ]
            },
            "domain_list":[
               "AOL",
               "ATT",
               "COMCAST",
               "COX",
               "GMAIL",
               "HOTMAIL",
               "MSN",
               "OPTOUT",
               "OTHER",
               "SBCGLOBAL",
               "YAHOO"
            ],
            "AB_RECOMMENDATION_THRESHOLD":0.25
         },
         "pp_id":null
       }, "Excellent"],
   [
      {
      "lead": {
          "CustomerId": 8,
          "firstName": "U2FsdGVkX18zcju5syoJ8SJYeXRVGeJuRjazHnJey/g=",
          "lastName": "U2FsdGVkX19swn7itO1ydWO6nxbntS0a7Akw0DzwtcI=",
          "email": "U2FsdGVkX19kTQi9NLafT0NnMSYsmWwFqa6iMk+uX1XsbzO61NkPxVG2GIYcXymB",
          "dayPhone": "U2FsdGVkX1+t6a2XyM5d6sPbKiG/V/mLSa0DL9o0Odw=",
          "eveningPhone": "U2FsdGVkX19mlunWV9x2JcJFU116A/aZJUPX5hBcCJo=",
          "purchasePrice": "$196,606.00",
          "loanAmount": "$81,621.00",
          "downPayment": "$114,985.00",
          "downPaymentStated": "$114,985.00",
          "existingHomeValue": "$504,268.00",
          "propertyAddress": "U2FsdGVkX19gyAyPNpmta/cMqLyJ+mtYhcQJ2ncLErY=",
          "propertyZipCode": "59191",
          "IsMilitary": "True",
          "propertyState": "TEXAS",
          "propertyType": "MODULAR",
          "intendedPropertyUse": "SECONDARYORVACATION",
          "loanPurpose": "HOMEREFINANCE30YEARIRRRL",
          "campaignId": "1047",
          "campaign": "SCD_CreditKarmaCPL_RT",
          "campaignGroup": "Sebonic 1 | Outbound",
          "creditProfile": "622",
          "FICO": 652,
          "bankruptcyInLastSevenYears": "True",
          "downPaymentAmount": null,
          "ActiveProspect": true,
          "dateAdded": "2020-07-10T21:54:21.752816",
          "id": null,
          "propair_id": null
      },
      "customer": "cardinal",
      "availability": [
          {
              "AgentID": "580",
              "Available": "1"
          },
          {
              "AgentID": "66",
              "Available": "1"
          },
          {
              "AgentID": "1970",
              "Available": "1"
          },
          {
              "AgentID": "2045",
              "Available": "1"
          },
          {
              "AgentID": "2118",
              "Available": "1"
          },
          {
              "AgentID": "2313",
              "Available": "1"
          },
          {
              "AgentID": "2334",
              "Available": "1"
          },
          {
              "AgentID": "2335",
              "Available": "1"
          },
          {
              "AgentID": "2403",
              "Available": "1"
          },
          {
              "AgentID": "2409",
              "Available": "1"
          }
      ],
      "accountConfig": {
          "CUSTOM_RANK_THRESHOLD_Inbound": 0,
          "write_to_velocify": true,
          "source_required": false,
          "data_incremental_update_calls": true,
          "expand_rec_by_percentage": 80,
          "domain_tld_list": [
              "BADDOMAIN",
              "COM",
              "EDU ",
              "GOV",
              "HARDBOUNCE",
              "NET",
              "OPTOUT",
              "ORG",
              "OTHER",
              "US"
          ],
          "extra_fields_to_write": [
              {
                  "leadField": "custom_rank_bin",
                  "accountField": "custom_rank_bin_id"
              }
          ],
          "accepted_split_fields": [
              "purchase_price_stated",
              "home_value_stated",
              "loan_amount_stated",
              "down_payment",
              "cash_out_stated"
          ],
          "agent_exclusion": false,
          "run_rec_expansion": false,
          "rookie_rollout_bins": [
              0.529,
              0.423
          ],
          "run_dynamic_rank": true,
          "AB_ALGO1_SUFFIX": ":000203a",
          "RECOMMENDATION_THRESHOLD_LICENSE": {
              "CRD": 0.7,
              "CCD": 0.7,
              "SCD": 0.7
          },
          "custom_rank": {
              "triggers": {
                  "use_custom_rank": true
              },
              "variables": {
                  "CUSTOM_RANK_THRESHOLD_ALL": 0.23,
                  "CR_bins": {
                      "ALL": [
                          0.581,
                          0.533,
                          0.479,
                          0.452,
                          0.412,
                          0.334
                      ],
                      "BAK_AP_ORIGINAL": [
                          0.571,
                          0.529,
                          0.436,
                          0.407,
                          0.39,
                          0.352
                      ],
                      "AP": [
                          0.571,
                          0.529,
                          0.436,
                          0.407,
                          0.39,
                          0.37
                      ]
                  },
                  "CUSTOM_RANK_THRESHOLD_Inbound": 0
              }
          },
          "AVAIL_START_HOUR": 6,
          "repost": true,
          "rookie_rollout_boundary_conditions": {
              "1": [
                  0.05,
                  0.2
              ],
              "2": [
                  0.1,
                  0.3
              ],
              "3": [
                  0.85,
                  0.5
              ]
          },
          "calls_software": "incontact",
          "los": {
              "date_close_field": "milestone_date_docsigning",
              "match_type": "on_lead_id"
          },
          "event_data_fromNowMinutes": 16,
          "AVAIL_AGENT_COUNT": 1,
          "credit_profile_priority_list": [
              "credit_score_stated",
              "credit_profile_stated"
          ],
          "AB_ALGO_COUNT": 2,
          "rookie_rollout": true,
          "RANDOMIZE_AGENT_ORDER": true,
          "repost_hours_from_now": 50,
          "prospect_matching": {
              "triggers": {
                  "RANDOMIZE_AGENT_ORDER": true,
                  "use_prospect_matching": true,
                  "agent_exclusion": false
              },
              "variables": {
                  "RECOMMENDATION_THRESHOLD": 0.5,
                  "RECOMMENDATION_THRESHOLD_LICENSE": {
                      "CRD": 0.7,
                      "CCD": 0.7,
                      "SCD": 0.7
                  },
                  "AB_ALGO2_SUFFIX": ":000203a",
                  "AVAIL_PAGE_SECONDS": 300,
                  "AB_ALGO_COUNT": 2,
                  "AVAIL_AGENT_COUNT": 1,
                  "RECOMMENDATION_LICENSE_LO": {
                      "CRD": 7,
                      "CCD": 10,
                      "SCD": 10
                  },
                  "RECOMMENDATION_THRESHOLD_WEEKEND": 0.75,
                  "AB_ALGO1_SUFFIX": ":000203a",
                  "AVAIL_START_HOUR": 6,
                  "AB_RECOMMENDATION_THRESHOLD": 0.25,
                  "AVAIL_END_HOUR": 23
              }
          },
          "repost_failure_threshold": 0.2,
          "rookie_rollout_timeframe_weeks": 6,
          "AB_RECOMMENDATION_THRESHOLD": 0.25,
          "RECOMMENDATION_LICENSE_LO": {
              "CRD": 7,
              "CCD": 10,
              "SCD": 10
          },
          "CUSTOM_RANK_THRESHOLD_ALL": 0.23,
          "use_custom_rank": true,
          "data_incremental_update_leads": true,
          "source_detail_required": false,
          "name": "cardinal",
          "data_incremental_update_events": true,
          "RECOMMENDATION_THRESHOLD": 0.5,
          "call_data_fromNowMinutes": 100,
          "id": 8,
          "expand_rec_after_seconds": 1209600,
          "pii_fields": [
              "firstname",
              "firstnamestated",
              "lastname",
              "lastnamestated",
              "email",
              "dayphone",
              "eveningphone",
              "emailstated",
              "velocify_password",
              "velocify_username",
              "propertyaddress"
          ],
          "AB_ALGO2_SUFFIX": ":000203a",
          "repost_sample_size": 50,
          "DR_bins": {
              "DR_14": [
                  0.511,
                  0.421,
                  0.386,
                  0.366,
                  0.35,
                  0.306
              ],
              "DR_28": [
                  0.493,
                  0.394,
                  0.357,
                  0.332,
                  0.313,
                  0.293
              ]
          },
          "AVAIL_PAGE_SECONDS": 300,
          "AVAIL_END_HOUR": 23,
          "rookie_rollout2": {
              "triggers": {
                  "use_rookie_rollout": true
              },
              "variables": {
                  "bins": [
                      0.529,
                      0.423
                  ],
                  "boundary_conditions": {
                      "1": [
                          0.05,
                          0.2
                      ],
                      "2": [
                          0.1,
                          0.3
                      ],
                      "3": [
                          0.85,
                          0.5
                      ]
                  },
                  "timeframe_weeks": 6
              }
          },
          "RECOMMENDATION_THRESHOLD_WEEKEND": 0.75,
          "bypass_leads": true,
          "domain_list": [
              "AOL",
              "ATT",
              "COMCAST",
              "COX",
              "GMAIL",
              "HOTMAIL",
              "MSN",
              "OPTOUT",
              "OTHER",
              "SBCGLOBAL",
              "YAHOO"
          ],
          "leads_chunk_limit": 5000
      },
      "pp_id": "b56fe52233d3e5c52da3"
       }, 200]
    
]

// const events2 = [
//     [{"account": "bbmc", "lead": {"credit_score_stated": "VeryGood", "credit_score_range_stated": "", "credit_profile_stated": "", "source": ""}}, "Excellent"]
// ]
// test("BinCreditProfile Unit Test", (event, expected) => {
//     map.preloadDBCache()
//     let mapValues = map.DBCache[`${account}/credit_profile_sources/` + map.fieldNameToYamlFilename(result["source"])];
//     let mapWithoutSources = [];
//     let regexItem = new RegExp(`${account}\/credit_profile_sources\/.*`, "gi");
//     let keys = Object.keys(DBCache);
//     for (const i in keys) {
//         const element = keys[i]
//         let key = element.match(regexItem);
//         if (key != null) {
//         mapWithoutSources.push(DBCache[key])
//         }

//     }

//     let result = getBinCreditProfile(result, ["credit_score_stated", "credit_score_range_stated", "credit_profile_stated"], mapWithoutSources, mapValues)
// })

test.each(events)("Map Unit Tests", (event, expected, done) => {
map.handler(event, {}, (discard, data) => {
   try {
      expect(data.body.map.bin_credit_profile_stated).toBe(expected)
      console.log("Test Completed")
      done();
  } catch (error) {
      done(error);
  }
})

})
