const { s3Helper } = require('pplibs/s3Helper');
const { reportToRollbar, ProPairError } = require('pplibs/customErrors')
const { getConfigurations } = require('pplibs/logUtils')
const moment = require('moment');

var AWS = require('aws-sdk');
AWS.config.update({ region: 'us-west-1' });

let sns = new AWS.SNS();

function sendSns(message, topic, subject = "") {
    const params = {
        Message: message,
        Subject: subject,
        TopicArn: topic
    }
    return new Promise( (resolve, reject) => {
        sns.publish(params, function (err, data) {
            if (err) {
                let miscError = new ProPairError("SNS Error " + err, "Misc")
                console.log(`::::: Error sending SNS Message: ${miscError}`)
                rejct(miscError)
            } else {
                console.log(`::::: Successfully sent Sns Message`)
                resolve()
            }
        });
    })
}

function filterFiles(keys, configs) {
    const conditions = ['MailSent', 'upload']
    const today = moment().startOf('day');

    return keys.filter(k => {
        // Filter by MailSent Files
        if(conditions.every(x => k.includes(x)) === false) return false 
        
        let paths = k.split('/');
        let account = paths[paths.length - 3]
        let file = paths[paths.length - 1]
        let config = configs[Object.keys(configs).find(x => configs[x].name === account)]
        
        // Filter by run_audit Dynamo trigger
        let runAudit;
        try {
            runAudit = config.direct_mail.run_audit
        } catch(e){}
        
        if (!runAudit) return false 
        
        // Filter by day_range
        let dayRange;
        try {
            dayRange = config.direct_mail.audit_days_range
        } catch(e){}
        
        if (!dayRange) dayRange = 7;

        var startDate = today.clone().subtract(dayRange, "days");
        var fileDate = moment(file.split('_')[0], 'YYYY-MMDD')
        
        if (!fileDate.isValid()) return false

        if (fileDate < startDate) return false

        return true

    })
}

exports.handler = async (event, context, callback) => {
    try {
        console.log("::::: Initializing MailSent Audit")
        let configs = await getConfigurations();
        let keys = await s3Helper.listAllKeys(process.env.BUCKET, 'sftp')

        let mailSentKeys = filterFiles(keys, configs);
        
        if (mailSentKeys.length > 0 ) {
            for (let index = 0; index < mailSentKeys.length; index++) {
                let key = mailSentKeys[index];
                console.log(`::::: Auditing File Path: ${key}`)
                await sendSns(JSON.stringify({ object: { key }, bucket: { name: process.env.BUCKET } }), process.env.TOPIC_DIRECT_MAIL, "MailSent Audit")
            }
    
            console.log("::::: All Done")
        } else {
            console.log("::::: No files met the required conditions for an audit")
        }
        return

    } catch (e) {
        console.log("::::: ERROR :::::")
        console.log(e)
        let error = new ProPairError(e , "MailSentAuditError")
        await reportToRollbar(error, process.env.TOPIC_ROLLBAR, "MailSentAudit")
            
        callback(null, {
          statusCode: e.code || 500,
          headers: {},
          body: JSON.stringify({ status: e.message || e})
        });
    }

}
