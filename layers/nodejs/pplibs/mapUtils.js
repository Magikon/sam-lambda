
var { secureLog } = require('./logUtils');

exports.calculateCredit = (score, field) => {
    secureLog(`::::: Calculating credit - (field, value): ${field}, ${score}`)
    let creditProfile;
    let creditScore = parseInt(String(score).replace(/[^0-9]/gi, ""), 10)
    let creditScoreStr = String(score).replace(/[^a-zA-Z]/gi, "")
    if (!isNaN(creditScore)) {
        if (creditScore > 290 && creditScore < 1000000) {
            //If credit score is above 1000, calculate the average 
            creditScore = creditScore < 1000 ? creditScore : (parseInt(String(creditScore).slice(0, 3), 10) + parseInt(String(creditScore).slice(3, 6), 10)) / 2

            secureLog(`::::: ${field} parsed as score: ${creditScore}`)
            if (creditScore > 0) creditProfile = 'Poor'
            if (creditScore >= 620) creditProfile = 'Fair'
            if (creditScore >= 660) creditProfile = 'Good'
            if (creditScore >= 720) creditProfile = 'Excellent'

            return creditProfile
        } else {
            secureLog(`::::: Invalid credit score for ${field}: ${creditScore}`)
        }
    }

    if (creditScoreStr != "") {
        creditScoreStr = creditScoreStr.toUpperCase();

        secureLog(`::::: ${field} parsed as string: ${creditScoreStr}`)
        if (creditScoreStr == "POOR") creditProfile = 'Poor'
        if (creditScoreStr == "FAIR") creditProfile = 'Fair'
        if (creditScoreStr == "GOOD") creditProfile = 'Good'
        if (creditScoreStr == "EXCELLENT") creditProfile = 'Excellent'
    }

    return creditProfile;
}


exports.getBinCreditProfile = (lead, priority_list, mappingSources) => {
    let result = null
    if (typeof priority_list === "undefined" || !priority_list) {
        priority_list = [
            { field: "credit_score_stated", use_lookup: false },
            { field: "credit_score_range_stated", use_lookup: false },
            { field: "credit_profile_stated", use_lookup: true }
        ]
    }

    for (const element of priority_list) {

        let value = lead[element.field]

        if (typeof value !== "undefined" && value !== '') {
            result = this.calculateCredit(value, element.field)

            if (element.use_lookup && (!result || typeof result === 'undefined')) {
                secureLog(`::::: ${element.field.toUpperCase()} FAILED TO CALCULATE CREDIT; FETCHING FROM LOOKUP`)
                result = mappingSources[this.fieldNameToYamlFieldname(value)]
                if (result) secureLog(`::::: Lookup match: ${result}`)
            }

            if (!result || typeof result === 'undefined') {
                secureLog(`::::: COULD NOT PARSE ${element.field.toUpperCase()}`)
                result = null
                continue
            } else {
                break
            }

        } else {
            secureLog(`::::: MISSING VALUE FOR ${element.field.toUpperCase()}`)
            continue
        }
    }
    return result
}

exports.variableSanitizer = (variableName) => {
    try {
        return variableName.replace(/[^a-zA-Z0-9.+@]/gi, "")
    } catch (e) {
        secureLog("Warning! trying to sanitize null value:", variableName)
        return ''
    }
}

exports.fieldNameToYamlFieldname = (fieldName) => {
    if (typeof fieldName === "string") {
        return this.variableSanitizer(fieldName).toUpperCase();
    } else {
        return fieldName
    }
}

exports.fieldNameToYamlFilename = (fieldName) => {
    if (typeof fieldName === "string") {
        return this.variableSanitizer(fieldName).toLowerCase();
    } else {
        return fieldName
    }
}