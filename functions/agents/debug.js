const api = require('./api.js');

const event = {
    pathParameters: {agentId:83785},
    body: `
        {
            "id": 83785, 
            "agent_id": 3,
            "account_id": 10,
            "first_name": "Pip", 
            "last_name": "Rudd",
            "name_velocify": "Rudd, Pip"
        }
    `
}

api.index(event, {}, (a,b) => {})
