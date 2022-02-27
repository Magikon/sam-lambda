const stream = require('./update.js')

let oldLead = {
    "lead": {
        "payload": {
            "id": "2044059",
            "campaign": "Lending Tree Long Form RT",
            "campaignGroup": "Consumer Direct",
            "cashOutAmount": "",
            "FilterID": "896611",
            "creditProfile": "",
            "dateAdded": "4/13/2021 10:04:37 PM",
            "dayPhone": "U2FsdGVkX19Jg7fGcRcZxFLgZV7ov05Cz3i+jS1GnMc=",
            "downPaymentAmount": "",
            "email": "U2FsdGVkX18ttzmbvefe4j/AzVF6jhpZ3UjSgj4l2Sqd4v5C9NRhXGtF60okmZUf",
            "eveningPhone": "U2FsdGVkX1/D2IvtRQO5+hPFzp+r/DOLXa4+d+zU6tk=",
            "existingHomeValue": "800000.00",
            "firstName": "U2FsdGVkX19u1XfiVPrBFwH1CvI+mVcFj5a+cpyewnU=",
            "intendedPropertyUse": "Primary",
            "IsMilitary": "True",
            "lastName": "U2FsdGVkX183mVB1zJbmCsykjqBPtGsM2uTOeL2y7Xo=",
            "loanAmount": "205922.00",
            "loanPurpose": "Refinance - No Cash-Out",
            "propertyAddress": "U2FsdGVkX19YUM3/VXlT5OAPCl9bJUeQca5h1XoJINquUguYAMv8tczakUyP+aLh",
            "propertyState": "OR",
            "propertyType": "SFR - Detached",
            "propertyZipCode": "97008",
            "purchasePrice": "384900.00",
            "FICO": "769",
            "CurrentOwnerId": "17201",
            "CurrentOwnerName": "Dave Dembinski",
            "CurrentOwnerEmail": "ddembinski@laderalending.com"
        }
    }
}
let newLead = {
    "lead": {
        "payload": {
            "id": "2050019",
            "campaign": "LendGo-CashOut",
            "campaignGroup": "Consumer Direct",
            "cashOutAmount": "",
            "FilterID": "1000",
            "creditProfile": "",
            "dateAdded": "4/20/2021 2:14:21 PM",
            "dayPhone": "U2FsdGVkX1+zvIM2ummS6WYOz5uWF1jzIpJNOC3sDnU=",
            "downPaymentAmount": "",
            "email": "johndoe@etherpros.com.mx",
            "eveningPhone": "U2FsdGVkX1/OTahXu/lchT/TkB1W+T6Y5GbSvo0a008=",
            "existingHomeValue": "346000.00 to 395000.00",
            "firstName": "U2FsdGVkX1+2MpQxo8mhjMoqbwapui3rdDbUWXGyXYk=",
            "intendedPropertyUse": "Primary",
            "IsMilitary": "False",
            "lastName": "U2FsdGVkX1+xCB5Rma2oGKMEdknxnm0Y6SmW7BDAXWc=",
            "loanAmount": "12000-23500",
            "loanPurpose": "Refinance - Cash-Out",
            "propertyAddress": "U2FsdGVkX19lFq5O0fWmWYMd59jjhgnj+5V+IwnHpp+LDu6zhC+ZN3jP/8zp2CgD",
            "propertyState": "WA",
            "propertyType": "",
            "propertyZipCode": "98290",
            "purchasePrice": "",
            "FICO": "300-600",
            "OriginalOwnerId": "17283",
            "OriginalOwnerName": "Michael Davis",
            "OriginalOwnerEmail": "mdavis@laderalending.com"
        }
    }
}


const event = {
    headers: {
        Authorization: "Bearer 8dhej20MD9sowke2922kf0dfWODrP4d2sa"
    },
    body: `${JSON.stringify(newLead)}`
}

stream.handler(event, {}, (a,b) => {})
