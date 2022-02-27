from encompass import update

event = {
    "Records": [
        {
            "eventVersion": "2.1",
            "eventSource": "aws:s3",
            "awsRegion": "us-west-1",
            "eventTime": "2019-11-15T13:14:08.073Z",
            "eventName": "ObjectCreated:Put",
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "DirectMailPut",
                "bucket":
                {
                    "name": "pro-pair-serverless-staging",
                    "ownerIdentity": { "principalId": "A3KB0F0FZHR41M" },
                    "arn": "arn:aws:s3:::pro-pair-serverless-staging"
                },
                "object":
                {
                    "key": "los/upload/propair-bank/encompass_propair-bank.csv",
                    "size": 6358,
                    "eTag": "ced1f40824a7351eaf21bce839a6e8a8",
                    "sequencer": "005DCEA85A39048F09"
                }
            }
        }
    ]
}
update(event, {})