'use strict';
var AWS = require('aws-sdk');
AWS.config.update({region: 'us-west-1'});
let s3 = new AWS.S3({ region: 'us-west-1' });

var Rollbar = require("rollbar");

const { encryptData, parseMessageToCustomError, secureLog } = require('pplibs/logUtils')

exports.handler = async (event, context, callback) => {
  console.log("::::: Rollbar Reporting Service Starting")
  console.log(`Production Mode? ${process.env.ENV == 'production'}`)
  let messageEvent = event['Records'][0]['Sns']['Message'];
  if (messageEvent.indexOf("PythonException") == -1) {
    messageEvent = messageEvent.replace(/(?:\r\n|\r|\n)/g, '\\n')
    messageEvent = messageEvent.replace(/\"/g, "\"")
  }
  let encryptEvent = await encryptData(messageEvent)
  console.log(encryptEvent)
  let payload = JSON.parse(encryptEvent)
  console.log(payload);

  console.log(`[Referer]: ${payload.referer}`)

  let _rollbarConfig;
  _rollbarConfig = {
      accessToken: "3ef736a027044630bb6c49c86c91756e",
      captureUncaught: true,
      captureUnhandledRejections: true,
      payload: {
        environment: process.env.ENV
      }
    };
  
  var rollbar = new Rollbar( _rollbarConfig );

  let err = parseMessageToCustomError(payload);
  secureLog("The error sent to Rollbar is")
  secureLog(err)
  if (err.extra != null && typeof err.extra !== 'undefined') {
      rollbar.error(err, err.extra)
    }
    else {
      rollbar.error(err)
    }

  return

}
