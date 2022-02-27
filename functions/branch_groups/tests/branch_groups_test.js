const api = require('../api.js');
require('dotenv').config();

const testTimeout = 60000
const event = {
    pathParameters: {branchGroupId:4},
    queryStringParameters : {include_agents: false},
    body: ``
}

const event_create_update = {
    pathParameters: {branchGroupId:7},
    queryStringParameters: {include_agents: false},
    body:
    `
    {
        "id":"7",
        "name":"BranchGroupJestTest",
        "account_id":3
     }
    `
}

test.skip("Create branch_group test", (done) => {
    function catchResponse(discard, data) {
        try {
            expect(data.statusCode).toBe(201)
            console.log("Create branch group [TEST COMPLETED]")
            done();
        } catch (error) {
            done(error);
        }
    }

    api.create(event_create_update, {}, catchResponse)
    
}, testTimeout)

test("Index branch_group test", (done) => {

    function catchResponse(discard, data) {
        try {
            expect(data.statusCode).toBe(200)
            console.log("Indexing branch group [TEST COMPLETED]")
            done();
        } catch (error) {
            done(error);
        }
    }

    api.index(event, {}, catchResponse)
    
}, testTimeout)

test("Show branch group test", (done) => {
    
    function catchResponse(discard, data) {
        try {
            expect(data.statusCode).toBe(200)
            console.log(data.body)
            console.log("Show branch group [TEST COMPLETED]")
            done();
        } catch (error) {
            done(error);
        }
    }

    api.show(event, {}, catchResponse)
    
}, testTimeout)

test.skip("Update branch_group test", (done) => {
    function catchResponse(discard, data) {
        try {
            expect(data.statusCode).toBe(200)
            console.log("Update branch group [TEST COMPLETED]")
            done();
        } catch (error) {
            done(error);
        }
    }

    api.update(event_create_update, {}, catchResponse)
    
}, testTimeout)


