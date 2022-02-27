const api = require('./api.js');

const event = {
    pathParameters: {accountId:10},
    queryStringParameters : {include_agents: false},
    body: `
        {
            "id": 10, 
            "agent_id": 3,
            "account_id": 10,
            "first_name": "Pip", 
            "last_name": "Rudd",
            "name_velocify": "Rudd, Pip"
        }
    `
}

const event_create_update = {
    pathParameters: {accountId:16},
    queryStringParameters : {include_agents: false},
    body:
`{
    "id":16,
    "name":"samsara-bank",
    "token":"samsara123",
    "velocify_username": null,
    "velocify_password": null,
    "velocify_rec_field_1_id":null,
    "velocify_branch_rec_field_1_id":null,
    "velocify_group_field_id":null,
    "ab_algo_count":null,
    "ab_recommendation_threshold":null,
    "ab_algo1_suffix":null,
    "ab_algo2_suffix":null,
    "recommendation_threshold":null,
    "recommendation_threshold_weekend":null,
    "recommendation_threshold_branch":null,
    "agent_availability_report_id":null,
    "propair_agent_id":null,
    "expand_rec_after_seconds":null,
    "expand_rec_by_percentage":null,
    "branch_specific_second_rec":null,
    "agent_page_views_report_id":null,
    "assignments_report_id":null,
    "custom_rank_bin_id":null
 }`
}


api.index(event_create_update, {}, (a,b) => {})
