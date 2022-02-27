const api = require('./api.js');

const event = {
    pathParameters: {branchGroupId:4},
    queryStringParameters : {include_agents: false},
    body: `
        {
            "name": "new branch group name", 
            "account_id": 3
        }
    `
}


api.index(event, {}, (a,b) => {})
