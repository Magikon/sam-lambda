const log_lead_meta = require('./log_lead_in.js');

var event = {
  "Records": [
    {
      "Sns": {
        "Message": `{"lead":{
          "request_id": "94c26481a428851",
          "lead_id": "1471807",
          "propair_id": null,
          "account_id": 12,
          "account_name": "ladera",
          "sre_timestamps": { "isColdStart": true, "map": 609, "recommend": 3651 },
          "recommendation": ":34n:131n:271n:298n:305n:317n:320n:321n:340n:343n:365n:369n:370n:371n:372n:374n:378n:CRy:000102a",
          "custom_rank_bin": 6,
          "error": 0
        }, "account": {"id": 12}, "isActiveProspect": false}`
      }
    }
  ]
}


log_lead_meta.handler(event, {}, (a, b) => {

})