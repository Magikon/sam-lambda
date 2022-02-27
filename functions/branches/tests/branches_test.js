const api = require('../api.js');
require('dotenv').config();

const testTimeout = 60000
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

const event_create_update = {
    pathParameters: {branchId:35},
    queryStringParameters: {include_agents: false},
    body:
    `
    {
        "id":"35",
        "name":"Default jest_test",
        "branch_group_id":4,
        "time_zone":null
     }
    `
}

test.skip("Create branch test", (done) => {
    function catchResponse(discard, data) {
        try {
            expect(data.statusCode).toBe(201)
            console.log("Create branch [TEST COMPLETED]")
            done();
        } catch (error) {
            done(error);
        }
    }

    api.create(event_create_update, {}, catchResponse)
    
}, testTimeout)

test("Index branch test", (done) => {

    function catchResponse(discard, data) {
        try {
            expect(data.statusCode).toBe(200)
            console.log("Indexing branches [TEST COMPLETED]")
            done();
        } catch (error) {
            done(error);
        }
    }

    api.index(event, {}, catchResponse)
    
}, testTimeout)

test("Show branch test", (done) => {
    
    function catchResponse(discard, data) {
        try {
            expect(data.statusCode).toBe(200)
            console.log(data.body)
            console.log("Show branch [TEST COMPLETED]")
            done();
        } catch (error) {
            done(error);
        }
    }

    api.show(event, {}, catchResponse)
    
}, testTimeout)

test.skip("Update branch test", (done) => {
    function catchResponse(discard, data) {
        try {
            expect(data.statusCode).toBe(200)
            console.log("Update branch [TEST COMPLETED]")
            done();
        } catch (error) {
            done(error);
        }
    }

    api.update(event_create_update, {}, catchResponse)
    
}, testTimeout)


