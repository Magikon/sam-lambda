require('dotenv').config();
const create = require('../create');
const testTimeout = 60000

events = [
  [
    {
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
    }, 200
  ],
  [
    {
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
                  "campaign":"UnitTestcampaign",
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
    }, 500
  ],
  [
    {
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
                  "propertyState":"TEXASSSSSSSS",
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
    }, 500
  ]
]

test.each(events)('Unit Test Create', (event, expected, done) => {
  create.handler(event, {}, (context, response) => {
    console.log(response)
    expect(response.statusCode).toBe(expected)
    done()
    
  })
}, testTimeout);
