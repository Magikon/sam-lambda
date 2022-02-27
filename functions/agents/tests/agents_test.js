const api = require('../api.js');
require('dotenv').config();

const testTimeout = 60000

const event = {
    pathParameters: {agentId:44485},
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

const event_update = {
    pathParameters: {agentId:44543},
    body:
    `
    {
        "account_end_date":null,
        "account_id":10,
        "active":null,
        "age":null,
        "agent_id":3,
        "agent_status_id":null,
        "assistant":null,
        "avg_tenure":null,
        "branch":null,
        "branch_group":null,
        "branch_id":null,
        "brokers_outpost_profile":null,
        "channel":null,
        "college":null,
        "days_account":null,
        "days_account_lead":null,
        "duplicate":null,
        "eligible_branch":null,
        "email":null,
        "employer_count":null,
        "encompass_name":null,
        "encompass_name2":null,
        "fb_profile":null,
        "first_name":"Pip",
        "gender":null,
        "graduation_year":null,
        "home_city":null,
        "home_country":null,
        "home_state":null,
        "last_name":"Rudd",
        "license_count":null,
        "license_list":null,
        "license_status":null,
        "license_type":null,
        "linkedin_profile":null,
        "lt_profile":null,
        "lt_profile_count":null,
        "name_velocify":"Rudd, Pip",
        "nmls_id":null,
        "primary_phone":null,
        "propair_id":null,
        "rec_type":null,
        "recent_review":null,
        "recommend":null,
        "regulatory_actions":null,
        "role":null,
        "role_group":null,
        "role_id":null,
        "rookie":null,
        "sales_manager":null,
        "secondary_phone":null,
        "source_exclusions":null,
        "tier":null,
        "title":null,
        "transfers_to":null,
        "veteran":null,
        "years_account":null,
        "years_account_lead":null,
        "years_industry":null,
        "zillow_profile":null,
        "zillow_profile_count":null,
        "zodiac_sign":null
     }
    `
}

const event_create = {
    pathParameters: {accountId:16},
    queryStringParameters : {include_agents: false},
    body:
    `
    {
        "account_end_date":null,
        "account_id":10,
        "active":null,
        "age":null,
        "agent_id":3,
        "agent_status_id":null,
        "assistant":null,
        "avg_tenure":null,
        "branch":null,
        "branch_group":null,
        "branch_id":null,
        "brokers_outpost_profile":null,
        "channel":null,
        "college":null,
        "days_account":null,
        "days_account_lead":null,
        "duplicate":null,
        "eligible_branch":null,
        "email":null,
        "employer_count":null,
        "encompass_name":null,
        "encompass_name2":null,
        "fb_profile":null,
        "first_name":"Pip",
        "gender":null,
        "graduation_year":null,
        "home_city":null,
        "home_country":null,
        "home_state":null,
        "last_name":"Rudd",
        "license_count":null,
        "license_list":null,
        "license_status":null,
        "license_type":null,
        "linkedin_profile":null,
        "lt_profile":null,
        "lt_profile_count":null,
        "name_velocify":"Rudd, Pip",
        "nmls_id":null,
        "primary_phone":null,
        "propair_id":null,
        "rec_type":null,
        "recent_review":null,
        "recommend":null,
        "regulatory_actions":null,
        "role":null,
        "role_group":null,
        "role_id":null,
        "rookie":null,
        "sales_manager":null,
        "secondary_phone":null,
        "source_exclusions":null,
        "tier":null,
        "title":null,
        "transfers_to":null,
        "veteran":null,
        "years_account":null,
        "years_account_lead":null,
        "years_industry":null,
        "zillow_profile":null,
        "zillow_profile_count":null,
        "zodiac_sign":null
     }
    `
}

const event_create_multiple = {
    body: `[
        
        {
            "account_end_date":null,
            "account_id":10,
            "active":null,
            "age":null,
            "agent_id":3,
            "agent_status_id":null,
            "assistant":null,
            "avg_tenure":null,
            "branch":null,
            "branch_group":null,
            "branch_id":null,
            "brokers_outpost_profile":null,
            "channel":null,
            "college":null,
            "days_account":null,
            "days_account_lead":null,
            "duplicate":null,
            "eligible_branch":null,
            "email":null,
            "employer_count":null,
            "encompass_name":null,
            "encompass_name2":null,
            "fb_profile":null,
            "first_name":"Pip",
            "gender":null,
            "graduation_year":null,
            "home_city":null,
            "home_country":null,
            "home_state":null,
            "last_name":"Rudd",
            "license_count":null,
            "license_list":null,
            "license_status":null,
            "license_type":null,
            "linkedin_profile":null,
            "lt_profile":null,
            "lt_profile_count":null,
            "name_velocify":"Rudd, Pip",
            "nmls_id":null,
            "primary_phone":null,
            "propair_id":null,
            "rec_type":null,
            "recent_review":null,
            "recommend":null,
            "regulatory_actions":null,
            "role":null,
            "role_group":null,
            "role_id":null,
            "rookie":null,
            "sales_manager":null,
            "secondary_phone":null,
            "source_exclusions":null,
            "tier":null,
            "title":null,
            "transfers_to":null,
            "veteran":null,
            "years_account":null,
            "years_account_lead":null,
            "years_industry":null,
            "zillow_profile":null,
            "zillow_profile_count":null,
            "zodiac_sign":null
            }
            ,
            {
            "account_end_date":null,
            "account_id":10,
            "active":null,
            "age":null,
            "agent_id":3,
            "agent_status_id":null,
            "assistant":null,
            "avg_tenure":null,
            "branch":null,
            "branch_group":null,
            "branch_id":null,
            "brokers_outpost_profile":null,
            "channel":null,
            "college":null,
            "days_account":null,
            "days_account_lead":null,
            "duplicate":null,
            "eligible_branch":null,
            "email":null,
            "employer_count":null,
            "encompass_name":null,
            "encompass_name2":null,
            "fb_profile":null,
            "first_name":"Pip2",
            "gender":null,
            "graduation_year":null,
            "home_city":null,
            "home_country":null,
            "home_state":null,
            "last_name":"Rudd2",
            "license_count":null,
            "license_list":null,
            "license_status":null,
            "license_type":null,
            "linkedin_profile":null,
            "lt_profile":null,
            "lt_profile_count":null,
            "name_velocify":"Rudd2, Pip2",
            "nmls_id":null,
            "primary_phone":null,
            "propair_id":null,
            "rec_type":null,
            "recent_review":null,
            "recommend":null,
            "regulatory_actions":null,
            "role":null,
            "role_group":null,
            "role_id":null,
            "rookie":null,
            "sales_manager":null,
            "secondary_phone":null,
            "source_exclusions":null,
            "tier":null,
            "title":null,
            "transfers_to":null,
            "veteran":null,
            "years_account":null,
            "years_account_lead":null,
            "years_industry":null,
            "zillow_profile":null,
            "zillow_profile_count":null,
            "zodiac_sign":null
         }
        ]`
    
    
}

const event_update_multiple = {
    body: `[
        
        {
            "id": 44543,
            "account_end_date":null,
            "account_id":10,
            "active":null,
            "age":null,
            "agent_id":3,
            "agent_status_id":null,
            "assistant":null,
            "avg_tenure":null,
            "branch":null,
            "branch_group":null,
            "branch_id":null,
            "brokers_outpost_profile":null,
            "channel":null,
            "college":null,
            "days_account":null,
            "days_account_lead":null,
            "duplicate":null,
            "eligible_branch":null,
            "email":null,
            "employer_count":null,
            "encompass_name":null,
            "encompass_name2":null,
            "fb_profile":null,
            "first_name":"Pip3",
            "gender":null,
            "graduation_year":null,
            "home_city":null,
            "home_country":null,
            "home_state":null,
            "last_name":"Rudd3",
            "license_count":null,
            "license_list":null,
            "license_status":null,
            "license_type":null,
            "linkedin_profile":null,
            "lt_profile":null,
            "lt_profile_count":null,
            "name_velocify":"Rudd, Pip",
            "nmls_id":null,
            "primary_phone":null,
            "propair_id":null,
            "rec_type":null,
            "recent_review":null,
            "recommend":null,
            "regulatory_actions":null,
            "role":null,
            "role_group":null,
            "role_id":null,
            "rookie":null,
            "sales_manager":null,
            "secondary_phone":null,
            "source_exclusions":null,
            "tier":null,
            "title":null,
            "transfers_to":null,
            "veteran":null,
            "years_account":null,
            "years_account_lead":null,
            "years_industry":null,
            "zillow_profile":null,
            "zillow_profile_count":null,
            "zodiac_sign":null
            }
            ,
            {
            "id": 44544,
            "account_end_date":null,
            "account_id":10,
            "active":null,
            "age":null,
            "agent_id":3,
            "agent_status_id":null,
            "assistant":null,
            "avg_tenure":null,
            "branch":null,
            "branch_group":null,
            "branch_id":null,
            "brokers_outpost_profile":null,
            "channel":null,
            "college":null,
            "days_account":null,
            "days_account_lead":null,
            "duplicate":null,
            "eligible_branch":null,
            "email":null,
            "employer_count":null,
            "encompass_name":null,
            "encompass_name2":null,
            "fb_profile":null,
            "first_name":"Pip4",
            "gender":null,
            "graduation_year":null,
            "home_city":null,
            "home_country":null,
            "home_state":null,
            "last_name":"Rudd4",
            "license_count":null,
            "license_list":null,
            "license_status":null,
            "license_type":null,
            "linkedin_profile":null,
            "lt_profile":null,
            "lt_profile_count":null,
            "name_velocify":"Rudd2, Pip2",
            "nmls_id":null,
            "primary_phone":null,
            "propair_id":null,
            "rec_type":null,
            "recent_review":null,
            "recommend":null,
            "regulatory_actions":null,
            "role":null,
            "role_group":null,
            "role_id":null,
            "rookie":null,
            "sales_manager":null,
            "secondary_phone":null,
            "source_exclusions":null,
            "tier":null,
            "title":null,
            "transfers_to":null,
            "veteran":null,
            "years_account":null,
            "years_account_lead":null,
            "years_industry":null,
            "zillow_profile":null,
            "zillow_profile_count":null,
            "zodiac_sign":null
         }
        ]`
    
    
}

test.skip("Create agent test", (done) => {
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

    api.create(event_create, {}, catchResponse)
    
}, testTimeout)

test.skip("Create multiple agents test", (done) => {
    function catchResponse(discard, data) {
        try {
            expect(data.statusCode).toBe(201)
            console.log(data.body)
            console.log("Create multiple agents [TEST COMPLETED]")
            done();
        } catch (error) {
            done(error);
        }
    }

    api.create_agents(event_create_multiple, {}, catchResponse)
    
}, testTimeout)

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
    
}, testTimeout)

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
    
}, testTimeout)

test.skip("Update agent test", (done) => {
    function catchResponse(discard, data) {
        try {
            expect(data.statusCode).toBe(201)
            console.log("Update agent [TEST COMPLETED]")
            done();
        } catch (error) {
            done(error);
        }
    }

    api.update(event_update, {}, catchResponse)
    
}, testTimeout)

test.skip("Update multiple agents test", (done) => {
    function catchResponse(discard, data) {
        try {
            expect(data.statusCode).toBe(200)
            console.log("Updating agents [TEST COMPLETED]")
            done();
        } catch (error) {
            done(error);
        }
    }

    api.update_agents(event_update_multiple, {}, catchResponse)
}, testTimeout)



