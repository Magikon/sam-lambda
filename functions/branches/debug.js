const api = require('./api.js');

const event = {
    pathParameters: {branchId:10},
    queryStringParameters : {include_agents: false},
    body: `
        {
            "name": "New Branch name", 
            "branch_group_id": 3,
            "time_zone": "Time-zone"
        }
    `
}


api.index(event, {}, (a,b) => {})
