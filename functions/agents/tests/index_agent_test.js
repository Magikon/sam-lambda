const api = require('../api.js');
require('dotenv').config();

let event = {
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

test("Index agents test", (done) => {

    function catchResponse(discard, data) {
        try {
            expect(data.statusCode).toBe(200)
            console.log("Indexing agents [TEST COMPLETED]")
            done();
        } catch (error) {
            done(error);
        }
    }

    api.index(event, {}, catchResponse)
    
})



test("Update agents test", (done) => {
    function catchResponse(discard, data) {
        try {
            expect(data.statusCode).toBe(200)
            console.log("Updating agents [TEST COMPLETED]")
            done();
        } catch (error) {
            done(error);
        }
    }

    api.update_agents(event, {}, catchResponse)
})

test("Update agent test", (done) => {
    function catchResponse(discard, data) {
        try {
            expect(data.statusCode).toBe(201)
            console.log("Update agent [TEST COMPLETED]")
            done();
        } catch (error) {
            done(error);
        }
    }

    api.update(event, {}, catchResponse)
    
})

test("Show agent test", (done) => {
    function catchResponse(discard, data) {
        try {
            expect(data.statusCode).toBe(200)
            console.log(data.body)
            console.log("Show agent [TEST COMPLETED]")
            done();
        } catch (error) {
            done(error);
        }
    }

    api.show(event, {}, catchResponse)
    
})

test("Create agent test", (done) => {
    function catchResponse(discard, data) {
        try {
            expect(data.statusCode).toBe(201)
            console.log(data.body)
            console.log("Create agent [TEST COMPLETED]")
            done();
        } catch (error) {
            done(error);
        }
    }

    api.create(event, {}, catchResponse)
    
})

test("Create agent test", (done) => {
    function catchResponse(discard, data) {
        try {
            expect(data.statusCode).toBe(201)
            console.log(data.body)
            console.log("Create agent [TEST COMPLETED]")
            done();
        } catch (error) {
            done(error);
        }
    }

    api.create_agents(event, {}, catchResponse)
    
})
