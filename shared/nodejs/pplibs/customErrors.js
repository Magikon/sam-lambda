var AWS = require('aws-sdk');
AWS.config.update({ region: 'us-west-1' });
let sns = new AWS.SNS();


function reportToRollbar(message, rollbarTopic, referer = "NO_REFERER") {
    return new Promise((resolve, reject) => {

        if (typeof message === "string") {
            message = new ProPairError(message)
        }
        if (typeof message.referer === 'undefined' || message.referer == null) {
            message.referer = referer
            message.name = referer
        }
        message.stack = message.stack.replace(/\"/g, "")
        message.message = message.message.replace(/\"/g, "")
        var errorParams = {
            Message: JSON.stringify(message, replaceErrors),
            Subject: "Rollbar Error",
            TopicArn: rollbarTopic
        };


        sns.publish(errorParams, function (err, data) {
            if (err) {
                console.log("ERROR: Failed to report exception to Rollbar");
                console.log(err);
                console.log(e);
                reject(err)
            } else {
                console.log("ERROR POSTED TO ROLLBAR");
                resolve()
            }
        });
    })
}


class GeneralProPairError extends Error {
    constructor(message) {
        super(message);
        this.name = this.constructor.name;
    }
}

function replaceErrors(key, value) {
    if (value instanceof GeneralProPairError) {
        var error = {};

        Object.getOwnPropertyNames(value).forEach(function (key) {
            error[key] = value[key];
        });

        return error;
    }

    return value;
}

class rollbarError extends GeneralProPairError {
    constructor(payload) {
        super(payload.message);
        this.extra = {}
        for (const key of Object.keys(payload)) {
            this.extra[key] = payload[key]
        }
        this.stack = payload.stack
    }
}

class MapError extends GeneralProPairError {

    static get map_error_types() {
        let types = [
            "Source_Missing",
            "Source_Detail_Missing",
            "All_Sources_Missing",
            "Source_Not_Found",
            "Campaign_Group_for_Source_Detail_and_Source_Not_Found",
            "Source_Detail_for_Source_Not_Found",
            "Mapping_Failure"
        ];
        return types
    }

    constructor(payload) {
        super(payload.message);
        if (typeof payload.lead !== 'undefined' && payload.lead != null) {
            this.lead = payload.lead
            this.customer = payload.customer
            this.mappingErrorType = MapError.map_error_types[payload.type]
        }
    }
}

class ProPairError extends GeneralProPairError {
    constructor(payload, type = null) {
        if (typeof payload === "string") {
            super(payload)
        }
        else {
            super(String(payload))
        }
        this.stack = payload.stack || this.stack

        if (typeof type !== "undefined" || type != null) {
            this.name = type
            this.referer = type
        }
    }
}

exports.reportToRollbar = reportToRollbar
exports.rollbarError = rollbarError
exports.MapError = MapError
exports.ProPairError = ProPairError
exports.replaceErrors = replaceErrors