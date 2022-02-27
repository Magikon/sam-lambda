const create = require("./create.js");

const event_cardinal = {
    headers: {
        Authorization: ""
    },
    body: {
        isQa: true,
        lead:
        {
            payload:
            {
                id: "4766216",
                actionCount: "1",
                bankruptcyInLastSevenYears: "",
                campaign: "CFD_LTLF_H1",
                campaignGroup: "CCD | LTLF COB",
                cashOutAmount: "",
                creditProfile: "",
                dateAdded: "06/05/2019 21:01:53",
                dayPhone: "8438936548",
                downPaymentAmount: "",
                email: "michaelchesh@yahoo.com",
                eveningPhone: "8438936548",
                existingHomeValue: "350000.00",
                firstName: "michael", intendedPropertyUse: "",
                lastName: "chshire", leadSource: "",
                lastDuplicate: "", loanAmount: "75000.00",
                loanPurpose: "Home Equity",
                propertyAddress: "",
                propertyState: "SC",
                propertyType: "Single Family",
                propertyZipCode: "29940",
                purchasePrice: "",
                IsMilitary: "False",
                FICO: "669"
            }
        },
        availability:
        {
            payload:
                [
                    { AgentID: "1877", Available: "1" },
                    { AgentID: "1948", Available: "1" },
                    { AgentID: "580", Available: "1" },
                    { AgentID: "452", Available: "1" },
                    { AgentID: "1684", Available: "1" },
                    { AgentID: "1137", Available: "1" },
                    { AgentID: "1493", Available: "1" },
                    { AgentID: "1925", Available: "1" },
                    { AgentID: "1139", Available: "1" },
                    { AgentID: "1775", Available: "1" },
                    { AgentID: "555", Available: "1" },
                    { AgentID: "778", Available: "1" },
                    { AgentID: "154", Available: "1" },
                    { AgentID: "319", Available: "1" },
                    { AgentID: "1596", Available: "1" },
                    { AgentID: "34", Available: "1" },
                    { AgentID: "1929", Available: "1" },
                    { AgentID: "1927", Available: "1" },
                    { AgentID: "1950", Available: "1" },
                    { AgentID: "2097", Available: "1" },
                    { AgentID: "2045", Available: "1" },
                    { AgentID: "2047", Available: "1" },
                    { AgentID: "2046", Available: "1" },
                    { AgentID: "2128", Available: "1" },
                    { AgentID: "1011", Available: "1" }
                ]
        }
    }
}


const event_pp = {
    headers: {
        Authorization: "Du30s49juI49eD320aOffzxs30oKj483eDt"
    },
    body: `{
        "availability": {
            "payload": [
                {
                  "AgentID":"580",
                  "Available":"1"
                },
                {
                  "AgentID":"66",
                  "Available":"1"
                },
                {
                  "AgentID":"1970",
                  "Available":"1"
                },
                {
                  "AgentID":"2045",
                  "Available":"1"
                },
                {
                  "AgentID":"2118",
                  "Available":"1"
                },
                {
                  "AgentID":"2313",
                  "Available":"1"
                },
                {
                  "AgentID":"2334",
                  "Available":"1"
                },
                {
                  "AgentID":"2335",
                  "Available":"1"
                },
                {
                  "AgentID":"2403",
                  "Available":"1"
                },
                {
                  "AgentID":"2409",
                  "Available":"1"
                }
            ]
        },
        "lead": {
            "payload": {
                "CustomerId":8,
                "firstName":"U2FsdGVkX18zcju5syoJ8SJYeXRVGeJuRjazHnJey/g=",
                "lastName":"U2FsdGVkX19swn7itO1ydWO6nxbntS0a7Akw0DzwtcI=",
                "email":"U2FsdGVkX19kTQi9NLafT0NnMSYsmWwFqa6iMk+uX1XsbzO61NkPxVG2GIYcXymB",
                "dayPhone":"U2FsdGVkX1+t6a2XyM5d6sPbKiG/V/mLSa0DL9o0Odw=",
                "eveningPhone":"U2FsdGVkX19mlunWV9x2JcJFU116A/aZJUPX5hBcCJo=",
                "purchasePrice":"$196,606.00",
                "loanAmount":"$81,621.00",
                "downPayment":"$114,985.00",
                "downPaymentStated":"$114,985.00",
                "existingHomeValue":"$504,268.00",
                "propertyAddress":"U2FsdGVkX19gyAyPNpmta/cMqLyJ+mtYhcQJ2ncLErY=",
                "propertyZipCode":"59191",
                "IsMilitary":"True",
                "propertyState":"TEXAS",
                "propertyType":"MODULAR",
                "intendedPropertyUse":"SECONDARYORVACATION",
                "loanPurpose":"HOMEREFINANCE30YEARIRRRL",
                "campaignId":"1047",
                "campaign":"SCD_CreditKarmaCPL_RT",
                "campaignGroup":"Sebonic 1 | Outbound",
                "creditProfile":"622",
                "FICO":652,
                "bankruptcyInLastSevenYears":"True",
                "downPaymentAmount":null,
                "ActiveProspect":true,
                "dateAdded":"2020-07-10T21:54:21.752816",
                "id":null,
                "propair_id":null
            }
            
        }
    }`
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

create.handler(event_pp, {}, (a, b) => {
    console.log(a)
    console.log(b)
})