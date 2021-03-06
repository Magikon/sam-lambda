import psycopg2
from leads_producer import update

event = {
  "Records": [
    {
      "EventVersion": "1.0",
      "EventSubscriptionArn": "arn:aws:sns:us-west-1:450889472107:pro-pair-serverless-DataArchitectureLeadsTopic-F52C0JVUM93C:a757cfd4-00ce-43d2-b86c-4dfa0f9f0bb2",
      "EventSource": "aws:sns",
      "Sns": {
        "SignatureVersion": "1",
        "Timestamp": "2018-12-13T22:13:08.735Z",
        "Signature": "FyE5Ufyo4LTZxHyNYytWd8D29jNdkHTlRg2V1IE2GUgbfKZhgwyPzRSFmXWL8BGB++JEjx28j/gyuNPGFN6D1AlywW+icHLRrfQ7wEqMx7mZy9ScVeUKrUUlgFuAP3uC3Yazm9U4J1DFL8s7Evx5nJCKfW6W8zxJSAiEHOf0u4FgCa27Q06/BiTb5Si3ndm5650daAJy5ilMo2JxbWWQLvfwoSqjHSPZ/lfUgs/uF8Z3iyEcQDwbeiaq9sbdDIpI6GAm6BOkIYCkjex8iBAmm76s654LqQ/+f6iy4JRZ6+/7m/feULX1IKwUCmU2h4+MhAep79dtxp/sXdPptBI6Yw==",
        "SigningCertUrl": "https://sns.us-west-1.amazonaws.com/SimpleNotificationService-ac565b8b1a6c5d002d285f9598aa1d9b.pem",
        "MessageId": "1604d4c9-e57f-57d9-b590-80453ecb3a17",
        "Message": "{\"velocify_password\": \"df*ejsh#w9kjnDsQm!!akqi\",\"account\": \"bnc\",\"account_id\": 12,\"velocify_username\": \"velocifysandbox@pro-pair.com\"}",
        "MessageAttributes": {},
        "Type": "Notification",
        "UnsubscribeUrl": "https://sns.us-west-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-1:450889472107:pro-pair-serverless-DataArchitectureLeadsTopic-F52C0JVUM93C:a757cfd4-00ce-43d2-b86c-4dfa0f9f0bb2",
        "TopicArn": "arn:aws:sns:us-west-1:450889472107:pro-pair-serverless-DataArchitectureLeadsTopic-F52C0JVUM93C",
        "Subject": "Production Error in Recommend Lambda"
      }
    }
  ]
}

update(event, {})