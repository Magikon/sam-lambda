const AWS = require('aws-sdk');


exports.createMetric = async (metricName, dimensions, value, nameSpace, unit='Milliseconds') => {
  return new Promise((resolve, reject) => {
    const metric = {
        MetricData: [ /* required */
          {
            MetricName: metricName, /* required */
            Dimensions: dimensions,
            Timestamp: new Date(),
            Unit: unit,
            Value: value
          },
          /* more items */
        ],
        Namespace: nameSpace /* required */
      };
    
      
    const cloudwatch = new AWS.CloudWatch({region: 'us-west-1'});
    cloudwatch.putMetricData(metric, (err, data) => {
        if (err) {
            reject(err)
        } else {
            resolve(data)          // successful response
        }
    });
  });
    
} 

  