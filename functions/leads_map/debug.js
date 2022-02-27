const map = require("./map.js");

const event_cardinal = {
  lead:
  {
    id: "4432743",
    actionCount: "0",
    bankruptcyInLastSevenYears: "No",
    campaign: "CFD_LTLF_H1",
    campaignGroup: "CCD | LTLF COB",
    cashOutAmount: "",
    creditProfile: "test",
    dateAdded: "3/3/2019 7:00 PM",
    dayPhone: "(972) 355-8391",
    downPaymentAmount: "$30,000.00",
    email: "jcdata@verizon.net",
    eveningPhone: "(972) 355-8391",
    existingHomeValue: "$225,000.00",
    firstName: "Rachel",
    intendedPropertyUse: "Primary Residence",
    lastName: "Chesney",
    lastDuplicate: "",
    loanAmount: "$80,000.00",
    loanPurpose: "Home Equity",
    propertyAddress: "",
    propertyState: "TX",
    propertyType: "Single Family",
    propertyZipCode: "75028",
    purchasePrice: "$110,000.00",
    IsMilitary: "No",
    FICO: "669"

  },
  customer: "cardinal"
}


const event_pp = {
  lead: {
    id: '1411041',
    creditScore: '',
    actionCount: '0',
    campaign: 'Aged Campaigns',
    campaignGroup: 'Retired',
    creditProfile: 'FAIR',
    dateAdded: '6/8/2020 8:54 AM',
    dayPhone: 'U2FsdGVkX19rjDv4Olkgj4O5rjPjFFceXsvNhGVSr0A=',
    email: 'U2FsdGVkX19C79nfkLz7w+w74CuuWkXrWOmBkKH292hK1ALsUQpJWDpsIk5w+0QB',
    eveningPhone: 'U2FsdGVkX1+TnmpQUbpCzeaml8odREuDpv1Cy2eub/o=',
    existingHomeValue: '1',
    firstName: 'U2FsdGVkX19FjRtrpuXTDAdyIB6BjH+JgLAARymgGCk=',
    intendedPropertyUse: 'PRIMARYHOME',
    lastDuplicate: '',
    lastName: 'U2FsdGVkX1+hh6q6s3UpyUQ56rcomvnQus9FU4aP55w=',
    loanAmount: '68216.00',
    loanPurpose: 'HOMEEQUITY2NDMORTGAGE',
    propertyAddress: 'U2FsdGVkX18/mvpOPl+uZppquQioHqOZMNT8uTvbDd8=',
    propertyState: 'ME',
    propertyType: 'APO',
    propertyZipCode: '36313',
    purchasePrice: '396204.00',
    LeadProviderName: 'HTTPSMYPROPAIR-BANK.COMADJUSTABLERATEMORTGAGE',
    leadProviderName: 'HTTPSMYPROPAIR-BANK.COMADJUSTABLERATEMORTGAGE',
    IsMilitary: 'Yes',
    CIS: '0.8',
    FICO: '745',
    CustomerId: '',
    FilterID: '',
    downPayment: '',
    downPaymentAmount: '',
    bankruptcyInLastSevenYears: ''
  },
  customer: 'propair-bank',
  availability: null,
  accountConfig: {
    write_to_velocify: true,
    data_incremental_update_leads: true,
    source_required: true,
    data_incremental_update_calls: true,
    expand_rec_by_percentage: 80,
    source_detail_required: true,
    domain_tld_list: [
      'BADDOMAIN', 'COM',
      'EDU ', 'GOV',
      'HARDBOUNCE', 'NET',
      'OPTOUT', 'ORG',
      'OTHER', 'US'
    ],
    name: 'propair-bank',
    accepted_split_fields: [
      'purchase_price_stated',
      'home_value_stated',
      'loan_amount_stated',
      'down_payment'
    ],
    data_incremental_update_events: true,
    fake_calls_config: { velocify: [Object], incontact: [Object] },
    call_data_fromNowMinutes: 30,
    generate_leads_size: 3,
    leads_incremental_min_from: 30,
    run_dynamic_rank: false,
    los_email_match: true,
    id: 10,
    expand_rec_after_seconds: 1209600,
    rec_expansion: [[Object]],
    pii_fields: [
      'firstname',
      'firstnamestated',
      'lastname',
      'lastnamestated',
      'email',
      'dayphone',
      'eveningphone',
      'emailstated',
      'velocify_password',
      'velocify_username',
      'propertyaddress'
    ],
    DR_bins: { DR_14: [Array], DR_28: [Array] },
    repost_sample_size: 30,
    custom_rank: { triggers: [Object], variables: [Object] },
    repost: true,
    bad_data: { str: [Array], int: [Array] },
    calls_software: 'incontact',
    los: {
      date_close_field: 'milestone_date_docsigning',
      match_type: 'velocify'
    },
    event_data_fromNowMinutes: 1,
    rookie_rollout2: { triggers: [Object], variables: [Object] },
    credit_profile_priority_list: [
      'credit_score_stated',
      'credit_score_range_stated',
      'credit_profile_stated'
    ],
    repost_hours_from_now: 24,
    prospect_matching: { triggers: [Object], variables: [Object] },
    repost_failure_threshold: 0.2,
    domain_list: [
      'AOL', 'ATT',
      'COMCAST', 'COX',
      'GMAIL', 'HOTMAIL',
      'MSN', 'OPTOUT',
      'OTHER', 'SBCGLOBAL',
      'YAHOO'
    ],
    event_data_pull_backfill: false,
    recommendation_spectrums: { spectrums: [Array], calculate_rec_spectrum: true }
  },
  pp_id: null
}

const event_nbkc = {
  "lead":
  {
    "id": "746568",
    "actionCount": "0",
    "bankruptcyInLastSevenYears": "No",
    "campaign": "LendingTree - VA Cash Out Refi",
    "campaignGroup": "LendingTree",
    "cashOutAmount": "",
    "creditProfile": "",
    "creditScore": "749",
    "dateAdded": "5/7/2019 12:21 PM",
    "dayPhone": "",
    "downPayment": "",
    "email": "amikerad00@yahoo.com",
    "eveningPhone": "(251) 504-0039",
    "existingHomeValue": "$225,000.00",
    "firstLoanAmount": "",
    "firstName": "Amelia",
    "firstRate": "",
    "firstRateType": "",
    "intendedPropertyUse": "Primary Residence",
    "lastName": "Radford",
    "loanAmount": "$155,001.00",
    "loanPurpose": "Refinance",
    "mobilePhone": "",
    "propertyAddress": "",
    "propertyAddress2": "",
    "propertyState": "AL",
    "propertyType": "Single Family",
    "propertyZipCode": "36535",
    "purchasePrice": "$225,000.00",
    "secondLoanAmount": "",
    "secondRate": "",
    "secondRateType": "",
    "vaEligible": "Yes",
    "costcoExclusivity": ""
  },
  "customer": "nbkc",
  "availability": null
}

const event_map = {
  "lead": {
    "id": "7251366",
    "actionCount": "0",
    "bankruptcyInLastSevenYears": "",
    "campaign": "CFD_LTLF_COB_RT2",
    "campaignGroup": "CCD | LTLF COB",
    "cashOutAmount": "",
    "creditProfile": "",
    "dateAdded": "10/23/2020 9:20 AM",
    "dayPhone": "U2FsdGVkX1+xfd9MbHxniRFE+nj1eL90/WHUAan98nk=",
    "downPaymentAmount": "$0.00",
    "email": "U2FsdGVkX1/IPTveK//JiBdffhFA5mfYMtjWuUnsH4g=",
    "eveningPhone": "U2FsdGVkX19FgRju1E25J8pOAUJPFEUlcc1pGP/r8XY=",
    "existingHomeValue": "",
    "firstName": "U2FsdGVkX19ucQt+0SaszC5Zxa0AA3P5Z6rF3xtFHLk=",
    "intendedPropertyUse": "",
    "lastName": "U2FsdGVkX1/A4NfdtP1RNJI1THQtini/UeUjcFgJmcU=",
    "lastDuplicate": "",
    "loanAmount": "",
    "loanPurpose": "",
    "propertyAddress": "U2FsdGVkX1+4VqYFtOcaIZVDRcg0jVkxvdrvZgNhmIw=",
    "propertyState": "TN",
    "propertyType": "",
    "propertyZipCode": "",
    "purchasePrice": "",
    "IsMilitary": "",
    "FICO": "",
    "ProPairId": "",
    "propair_id": ""
  },
  "customer": "cardinal",
  "availability": null,
  "accountConfig": {
    "active_prospect": {
      "use_custom_rank": true,
      "use_prospect_matching": false
    },
    "calls_producer": {
      "triggers": {
        "run_producer": true
      },
      "variables": {
        "calls_software": "incontact",
        "fromNowMinutes": 1440
      }
    },
    "credit_profile_priority_list": [
      "credit_score_stated",
      "credit_profile_stated"
    ],
    "custom_rank": {
      "triggers": {
        "use_custom_rank": true
      },
      "variables": {
        "CR_bins": {
          "ALL": [
            0.581,
            0.533,
            0.479,
            0.452,
            0.412,
            0.334
          ],
          "AP": [
            0.571,
            0.529,
            0.436,
            0.407,
            0.39,
            0.37
          ],
          "BAK_AP_ORIGINAL": [
            0.571,
            0.529,
            0.436,
            0.407,
            0.39,
            0.352
          ]
        },
        "CUSTOM_RANK_THRESHOLD_ALL": 0.23,
        "CUSTOM_RANK_THRESHOLD_Inbound": 0
      }
    },
    "domains": {
      "list": [
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
      "tld_list": [
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
      ]
    },
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
    "events_producer": {
      "triggers": {
        "run_producer": true
      },
      "variables": {
        "fromNowMinutes": 16
      }
    },
    "id": 8,
    "leads_producer": {
      "triggers": {
        "run_producer": true
      },
      "variables": {
        "accepted_split_fields": [
          "purchase_price_stated",
          "home_value_stated",
          "loan_amount_stated",
          "down_payment",
          "cash_out_stated"
        ]
      }
    },
    "los": {
      "date_close_field": "milestone_date_docsigning",
      "match_type": "on_lead_id"
    },
    "name": "cardinal",
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
    "prospect_matching": {
      "triggers": {
        "agent_exclusion": false,
        "RANDOMIZE_AGENT_ORDER": true,
        "use_prospect_matching": true
      },
      "variables": {
        "AB_ALGO1_SUFFIX": ":000203a",
        "AB_ALGO2_SUFFIX": ":000203a",
        "AB_ALGO_COUNT": 2,
        "AB_RECOMMENDATION_THRESHOLD": 0.25,
        "AVAIL_AGENT_COUNT": 1,
        "AVAIL_END_HOUR": 23,
        "AVAIL_PAGE_SECONDS": 300,
        "AVAIL_START_HOUR": 6,
        "RECOMMENDATION_LICENSE_LO": {
          "CCD": 10,
          "CRD": 7,
          "SCD": 10
        },
        "RECOMMENDATION_THRESHOLD": 0.5,
        "RECOMMENDATION_THRESHOLD_LICENSE": {
          "CCD": 0.7,
          "CRD": 0.7,
          "SCD": 0.7
        },
        "RECOMMENDATION_THRESHOLD_WEEKEND": 0.75
      }
    },
    "rec_expansion": [
      {
        "expand_rec_after_seconds": 1209600,
        "expand_rec_by_percentage": 80,
        "run_rec_expansion": false
      }
    ],
    "repost": {
      "triggers": {
        "use_repost": true
      },
      "variables": {
        "failure_threshold": 0.2,
        "hours_from_now": 50,
        "sample_size": 50
      }
    },
    "rookie_rollout": {
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
    "run_dynamic_rank": true,
    "source_detail_required": false,
    "source_required": false,
    "write_to_velocify": true
  },
  "pp_id": null
}

map.handler(event_map, {}, (a, b) => {
  console.log(a)
  console.log(b)
})