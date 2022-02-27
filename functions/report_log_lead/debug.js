const log_lead_meta = require('./log_lead_meta.js');

var event = {
  "Records": [
    {
      "Sns": {
        "Message": `{
          "lead": {
            "id": "1390010", "actionCount": "0", "bankruptcyInLastSevenYears": "", "campaignId": "1051",
            "campaign": "CD_Zillow_RT", "cashOutAmount": "", "creditProfile": "Excellent", "dateAdded": "06/02/2020 15:38:31",
            "dayPhone": "U2FsdGVkX18rwLKjxf3+yR0/RYvnIqi7u2S+phKoK4o=", "downPaymentAmount": "",
            "email": "U2FsdGVkX19CyO94mEFiWylxzrTgbAfqcq6/A/FLZCoXvDwHuln2jvaciKKPCOKx",
            "eveningPhone": "U2FsdGVkX19GICCwNv6V2XVM3WGy2PgDChwRv26fwAI=", "existingHomeValue": "270000",
            "firstName": "U2FsdGVkX1+di1lcnd+ZYs1iTaypvLZAytXEHEDOtI0=", "intendedPropertyUse": "",
            "lastName": "U2FsdGVkX19j3SPAkHVmS30nmhYf6bkvQPS+COt6NSM=", "leadSource": "", "lastDuplicate": "", "loanAmount": "230000", "loanPurpose": "Refinance", "propertyAddress": "U2FsdGVkX1+T2JEEoKVPPOUCqgstdDlgI6USfn8+CB0=", "propertyState": "NC", "propertyType": "Single Family Home", "propertyZipCode": "27705", "purchasePrice": "270000", "IsMilitary": "", "FICO": "740", "ActiveProspect": "true", "propair_id": "36db0517233113b72dd4", "campaignGroup": "Sebonic 1 | Outbound", "probabilities": null, "features": null, "reassignment": 0, "custom_rank": 0.5293953055511927, "custom_rank_bin": 2, "availableAgents": null, "availableAgentsThreshold": null, "agentsAdded": null, "agentOrderRecommendation": null, "agentOrderProbabilities": null, "trimmedRecommendation": null, "recommendation": null, "originalRecommendation": null
          },
          "reassignment": true,
          "active_prospect": true,
          "account": {
            "id": 8, "name": "cardinal",
            "token": "U2FsdGVkX19IewefuQK8Lit7GIua/yhNaP7yWlswh6MnX7kBY/9pvQ/npny1b9pD6z32MS29WB1LKBJ9/8dgcw==",
            "velocify_username": "velocifysandbox@pro-pair.com", "velocify_password": "df*ejsh#w9kjnDsQm!!akqi", "velocify_rec_field_1_id": 166, "velocify_branch_rec_field_1_id": null, "created_at": "2019-03-01T02:05:29.392Z", "updated_at": "2019-03-01T02:05:29.392Z", "velocify_group_field_id": null, "ab_algo_count": null, "ab_recommendation_threshold": null, "ab_algo1_suffix": null, "ab_algo2_suffix": null, "recommendation_threshold": null, "recommendation_threshold_weekend": null, "recommendation_threshold_branch": null, "agent_availability_report_id": 53, "propair_agent_id": null, "expand_rec_after_seconds": 1209600, "expand_rec_by_percentage": 80, "branch_specific_second_rec": null, "agent_page_views_report_id": 51, "assignments_report_id": null, "custom_rank_bin_id": 189
          },
          "accountConfig": {
            "CUSTOM_RANK_THRESHOLD_Inbound": 0, "write_to_velocify": true, "source_required": false,
            "data_incremental_update_calls": true, "expand_rec_by_percentage": 80,
            "domain_tld_list": ["BADDOMAIN", "COM", "EDU ", "GOV", "HARDBOUNCE", "NET", "OPTOUT", "ORG", "OTHER", "US"],
            "extra_fields_to_write": [{ "leadField": "custom_rank_bin", "accountField": "custom_rank_bin_id" }],
            "accepted_split_fields": ["purchase_price_stated", "home_value_stated", "loan_amount_stated", "down_payment", "cash_out_stated"],
            "agent_exclusion": false, "run_rec_expansion": false, "rookie_rollout_bins": [0.529, 0.423], "run_dynamic_rank": true, "AB_ALGO1_SUFFIX": ":000203a", "RECOMMENDATION_THRESHOLD_LICENSE": { "CRD": 0.7, "CCD": 0.7, "SCD": 0.7 }, "custom_rank": { "triggers": { "use_custom_rank": true }, "variables": { "CUSTOM_RANK_THRESHOLD_ALL": 0.23, "CR_bins": { "ALL": [0.581, 0.533, 0.479, 0.452, 0.412, 0.334], "AP": [0.571, 0.529, 0.436, 0.407, 0.39, 0.352] }, "CUSTOM_RANK_THRESHOLD_Inbound": 0 } }, "AVAIL_START_HOUR": 6, "repost": true, "rookie_rollout_boundary_conditions": { "1": [0.05, 0.2], "2": [0.1, 0.3], "3": [0.85, 0.5] }, "calls_software": "incontact", "los": { "date_close_field": "milestone_date_docsigning", "match_type": "on_lead_id" }, "event_data_fromNowMinutes": 16, "AVAIL_AGENT_COUNT": 1, "credit_profile_priority_list": ["credit_score_stated", "credit_profile_stated"], "AB_ALGO_COUNT": 2, "rookie_rollout": true, "RANDOMIZE_AGENT_ORDER": true, "repost_hours_from_now": 50, "prospect_matching": { "triggers": { "RANDOMIZE_AGENT_ORDER": true, "use_prospect_matching": true, "agent_exclusion": false }, "variables": { "RECOMMENDATION_THRESHOLD": 0.5, "RECOMMENDATION_THRESHOLD_LICENSE": { "CRD": 0.7, "CCD": 0.7, "SCD": 0.7 }, "AB_ALGO2_SUFFIX": ":000203a", "AVAIL_PAGE_SECONDS": 300, "AB_ALGO_COUNT": 2, "AVAIL_AGENT_COUNT": 1, "RECOMMENDATION_LICENSE_LO": { "CRD": 7, "CCD": 10, "SCD": 10 }, "RECOMMENDATION_THRESHOLD_WEEKEND": 0.75, "AB_ALGO1_SUFFIX": ":000203a", "AVAIL_START_HOUR": 6, "AB_RECOMMENDATION_THRESHOLD": 0.25, "AVAIL_END_HOUR": 23 } }, "repost_failure_threshold": 0.2, "rookie_rollout_timeframe_weeks": 6, "AB_RECOMMENDATION_THRESHOLD": 0.25, "RECOMMENDATION_LICENSE_LO": { "CRD": 7, "CCD": 10, "SCD": 10 }, "CUSTOM_RANK_THRESHOLD_ALL": 0.23, "use_custom_rank": true, "data_incremental_update_leads": true, "source_detail_required": false, "name": "cardinal", "data_incremental_update_events": true, "RECOMMENDATION_THRESHOLD": 0.5, "call_data_fromNowMinutes": 100, "id": 8, "expand_rec_after_seconds": 1209600, "pii_fields": ["firstname", "firstnamestated", "lastname", "lastnamestated", "email", "dayphone", "eveningphone", "emailstated", "velocify_password", "velocify_username", "propertyaddress"], "AB_ALGO2_SUFFIX": ":000203a", "repost_sample_size": 50, "DR_bins": { "DR_14": [0.511, 0.421, 0.386, 0.366, 0.35, 0.306], "DR_28": [0.493, 0.394, 0.357, 0.332, 0.313, 0.293] }, "AVAIL_PAGE_SECONDS": 300, "AVAIL_END_HOUR": 23, "rookie_rollout2": { "triggers": { "use_rookie_rollout": true }, "variables": { "bins": [0.529, 0.423], "boundary_conditions": { "1": [0.05, 0.2], "2": [0.1, 0.3], "3": [0.85, 0.5] }, "timeframe_weeks": 6 } }, "RECOMMENDATION_THRESHOLD_WEEKEND": 0.75, "bypass_leads": true, "domain_list": ["AOL", "ATT", "COMCAST", "COX", "GMAIL", "HOTMAIL", "MSN", "OPTOUT", "OTHER", "SBCGLOBAL", "YAHOO"],
            "leads_chunk_limit": 5000
          }
        }`
      }
    }
  ]
}


log_lead_meta.handler(event, {}, (a, b) => {

})