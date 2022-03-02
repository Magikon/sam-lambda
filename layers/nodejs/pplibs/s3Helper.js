const AWS = require('aws-sdk');
AWS.config.update({ region: 'us-west-1' });
let s3 = new AWS.S3({ region: 'us-west-1' });

exports.s3Helper = class s3Helper {

    static async listAllKeys(bucket, prefix = '') {
        var allKeys = [];
        const params = {
            Bucket: bucket,
            Prefix: prefix
        }

        try {

            function list(params,) {
                return new Promise((resolve, reject) => {
                    s3.listObjectsV2(params, async function (err, data) {
                        if (err) {
                            reject(err, err.stack); // an error occurred
                        } else {
                            var contents = data.Contents;
                            allKeys = allKeys.concat(contents.map(i => i.Key))

                            if (data.IsTruncated) {
                                params.ContinuationToken = data.NextContinuationToken;
                                console.log("get further list...");
                                console.log("Current Length", allKeys.length)
                                await list(params);
                            }
                            resolve();
                        }
                    });
                })
            }

            await list(params);
            if (allKeys.length > 0) {
                return allKeys
            } else {
                throw 'No keys found for specified path'
            }
        } catch (e) {
            console.log('List Objects Error!', e)
            console.log('Params:', params)
        }
    }
}
