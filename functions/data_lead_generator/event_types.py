#! /usr/bin/env python

################################################
# Column list: Should be from 'propair_field'
################################################
# global_var_list = ['account_lead_id','created_at','modified_at','log_type',\
# 				{ 'field_name': 'log_id', 'field_type': 'INT'},{ 'field_name': 'log_date', 'field_type': 'TIMESTAMP'},{ 'field_name': 'log_subtype_id', 'field_type': 'INT'},{ 'field_name': 'log_subtype_name', 'field_type': 'VARCHAR'},\
# 				{ 'field_name': 'milestone_id', 'field_type': 'INT'},{ 'field_name': 'log_note', 'field_type': 'VARCHAR'},{ 'field_name': 'log_user_id', 'field_type': 'INT'},{ 'field_name': 'log_user_name', 'field_type': 'VARCHAR'},\
# 				{ 'field_name': 'log_user_email', 'field_type': 'VARCHAR'},{ 'field_name': 'group_id', 'field_type': 'INT'},{ 'field_name': 'group_name', 'field_type': 'VARCHAR'},{ 'field_name': 'imported', 'field_type': 'INT'}]

######################################
# Map all logs to global fields names
######################################
event_types = {}
#event_types['account_lead_id'] = 'Id'
#event_types['created_date'] = 'CreateDate'
#event_types['modified_date'] = 'ModifyDate'

#event_types['creationlog'] = { 
#    'LogId': { 'field_name': 'log_id', 'field_type': 'INT'},
#    'LogDate': { 'field_name': 'log_date', 'field_type': 'TIMESTAMP'},
#    'CreatedByAgentId': { 'field_name': 'log_user_id', 'field_type': 'INT'},
#    'CreatedByAgentName': { 'field_name': 'log_user_name', 'field_type': 'VARCHAR'},
#    'CreatedByAgentEmail': { 'field_name': 'log_user_email', 'field_type': 'VARCHAR'},
#    'CreatedByGroupId': { 'field_name': 'group_id', 'field_type': 'INT'},
#    'CreatedByGroupName': { 'field_name': 'group_name', 'field_type': 'VARCHAR'},
#    'Imported': { 'field_name': 'imported', 'field_type': 'INT'}
#}

#event_types['sync'] = { 
#    'LogId': { 'field_name': 'log_id', 'field_type': 'INT'},
#    'LogDate': { 'field_name': 'log_date', 'fiesld_type': 'TIMESTAMP'},
#    'Name': { 'field_name': 'log_subtype_name', 'field_type': 'VARCHAR'},
#    'Message': { 'field_name': 'log_note', 'field_type': 'VARCHAR'},
#    'AgentId': { 'field_name': 'log_user_id', 'field_type': 'INT'},
#    'AgentName': { 'field_name': 'log_user_name', 'field_type': 'VARCHAR'},
#    'AgentEmail': { 'field_name': 'log_user_email', 'field_type': 'VARCHAR'}
#}
event_types['email'] = {
    "subtypes": [
        {"id": 117, "name": "Newly Assigned Lead Notification", "milestone": None, "milestone_id": None},
        {"id": 119, "name": "Introduction Email: Generic", "milestone": None, "milestone_id": None}
    ]
}

event_types['export'] = {
    "subtypes": [
        {"id": -50, "name": "Failure", "milestone": None, "milestone_id": None}
    ]
}

event_types['status'] = {
    "subtypes": [
        {"id": 2, "name": "Contact Attempt", "milestone": None, "milestone_id": None}
    ],
    "log_note": True
}

event_types['distribution'] = {
    "subtypes": [
        {"id": 38, "name": "New Lead Distribution", "milestone": None, "milestone_id": None}
    ],
    "log_note": True
}

event_types['action'] = {
    "subtypes": [
        {"id": 68, "name": "Called: No Contact", "milestone": "None", "milestone_id": 0},
        {"id": 33, "name": "Called: Left Message", "milestone": "None", "milestone_id": 0},
        {"id": 116, "name": "Past Customer - Inquired about a New Loan", "milestone": "None", "milestone_id": 0},
        {"id": 92, "name": "On Phone With Client", "milestone": "Contact", "milestone_id": 1},
        {"id": 118, "name": "Went With Another Lender", "milestone": "Contact", "milestone_id": 1},
        {"id": 71, "name": "Called: Contacted/Call Back", "milestone": "Contact", "milestone_id": 1},
        {"id": 93, "name": "File Started", "milestone": "Contact", "milestone_id": 1},
        {"id": 106, "name": "Client Care - Existing Client", "milestone": "Contact", "milestone_id": 1},
        {"id": 76, "name": "Locked/Submitted", "milestone": "Qualification", "milestone_id": 2},
        {"id": 67, "name": "Not Interested", "milestone": "Qualification", "milestone_id": 2},
        {"id": 86, "name": "Rate Locked", "milestone": "Conversion", "milestone_id": 4},
        {"id": 50, "name": "Funded Loan", "milestone": "Conversion", "milestone_id": 4}
    ],
    "log_note": True
}



#event_types['assignment'] = { 
#    'LogId': { 'field_name': 'log_id', 'field_type': 'INT'},
#    'LogDate': { 'field_name': 'log_date', 'field_type': 'TIMESTAMP'},
#    'AssignedAgentId': { 'field_name': 'log_subtype_id', 'field_type': 'INT'},
#    'AssignedAgentName': { 'field_name': 'log_subtype_name', 'field_type': 'VARCHAR'},
#    'AssignedAgentEmail': { 'field_name': 'log_note', 'field_type': 'VARCHAR'},
#    'AssignedByAgentId': { 'field_name': 'log_user_id', 'field_type': 'INT'},
#    'AssignedByAgentName': { 'field_name': 'log_user_name', 'field_type': 'VARCHAR'},
#    'AssignedByAgentEmail': { 'field_name': 'log_user_email', 'field_type': 'VARCHAR'},
#    'AssignedGroupId': { 'field_name': 'group_id', 'field_type': 'INT'},
#    'AssignedGroupName': { 'field_name': 'group_name', 'field_type': 'VARCHAR'}
#}


action_seq = [
    [67, 92, 33, 68],
    [67, 68, 68, 71],
    [67, 71, 71],
    [33, 71, 71, 33, 80 ,118, 71, 71, 104, 106, 81, 81, 86, 33],
    [118, 79, 33, 108, 108, 81, 108, 71, 106, 106, 67, 106, 76, 108, 108, 120, 120, 120, 98, 106, 2, 33, 108, 116, 50],
    [71, 71, 81, 80, 98, 33, 93, 120, 77, 80, 93, 120, 76, 120, 116, 76, 50],
    [106, 81, 33, 106, 71, 81, 98, 86, 2, 71, 106, 118, 33, 120, 104, 33, 71, 120, 106, 81, 120],
    [33, 81, 120, 71, 71, 71, 71, 120, 120, 118]
]



action_log_subtypes = {
2: [
    {"log_subtype_id": 2, "log_subtype_name": "Application - Locked", "milestone_id": 2},
    {"log_subtype_id": 2, "log_subtype_name": "Application Received", "milestone_id": 2},
    {"log_subtype_id": 2, "log_subtype_name": "Sent to Processing", "milestone_id": 0}
],
33: [
    {"log_subtype_id": 33, "log_subtype_name": "Contact Attempt: Called/Left Message", "milestone_id": 0},
    {"log_subtype_id": 33, "log_subtype_name": "Called: Left Message", "milestone_id": 0}
],
43: [
    {"log_subtype_id": 43, "log_subtype_name": "Pitched Loan", "milestone_id": 1},
    {"log_subtype_id": 43, "log_subtype_name": "Working", "milestone_id": 1},
    {"log_subtype_id": 43, "log_subtype_name": "Loan Quote Requested", "milestone_id": 1}
],
50: [
    {"log_subtype_id": 50, "log_subtype_name": "Funded Loan", "milestone_id": 0},
    {"log_subtype_id": 50, "log_subtype_name": "Funded Loan", "milestone_id": 4}
],
52: [{"log_subtype_id": 52, "log_subtype_name": "Bad Lead: Return to Vendor", "milestone_id": 0}],
59: [
    {"log_subtype_id": 59, "log_subtype_name": "Application Withdrawn ", "milestone_id": 2},
    {"log_subtype_id": 59, "log_subtype_name": "Application Withdrawn ", "milestone_id": 0}
],
67: [
    {"log_subtype_id": 67, "log_subtype_name": "TUD - Not Interested", "milestone_id": 1},
    {"log_subtype_id": 67, "log_subtype_name": "Not Interested", "milestone_id": 1},
    {"log_subtype_id": 67, "log_subtype_name": "Nurture", "milestone_id": 0}
],
68: [
    {"log_subtype_id": 68, "log_subtype_name": "Called: No Contact", "milestone_id": 0},
    {"log_subtype_id": 68, "log_subtype_name": "Contact Attempt: Called/No Message ", "milestone_id": 0}
    ],
69: [
    {"log_subtype_id": 69, "log_subtype_name": "Does Not Qualify", "milestone_id": 1},
    {"log_subtype_id": 69, "log_subtype_name": "Does Not Qualify", "milestone_id": 0},
    {"log_subtype_id": 69, "log_subtype_name": "Lead:  No Program Available", "milestone_id": 1}
    ],
70: [{"log_subtype_id": 70, "log_subtype_name": "Bad Phone Number", "milestone_id": 0}],
71: [
    {"log_subtype_id": 71, "log_subtype_name": "Made Contact/Call Back", "milestone_id": 1},
    {"log_subtype_id": 71, "log_subtype_name": "Called: Contacted/Call Back", "milestone_id": 1}
    ],
73: [{"log_subtype_id": 73, "log_subtype_name": "Dead", "milestone_id": 0}],
75: [{"log_subtype_id": 75, "log_subtype_name": "Change to Nurture", "milestone_id": 0}],
76: [
    {"log_subtype_id": 76, "log_subtype_name": "Locked/Submitted", "milestone_id": 2},
    {"log_subtype_id": 76, "log_subtype_name": "Submitted", "milestone_id": 0}
    ],
77: [{"log_subtype_id": 77, "log_subtype_name": "General Comment", "milestone_id": 0}],
78: [{"log_subtype_id": 78, "log_subtype_name": "Get Zillow URL", "milestone_id": 0}],
79: [{"log_subtype_id": 79, "log_subtype_name": "Text Message - Undeliverable", "milestone_id": 0}],
80: [{"log_subtype_id": 80, "log_subtype_name": "Text Message - Received", "milestone_id": 0}],
81: [{"log_subtype_id": 81, "log_subtype_name": "Text Message - Delivered", "milestone_id": 0}],
83: [{"log_subtype_id": 83, "log_subtype_name": "Text Message - Opted Out", "milestone_id": 0}],
84: [{"log_subtype_id": 84, "log_subtype_name": "Inbound Email", "milestone_id": 1}],
85: [{"log_subtype_id": 85, "log_subtype_name": "Called: Abandoned Call", "milestone_id": 0}],
86: [
    {"log_subtype_id": 86, "log_subtype_name": "Send to Credit Repair", "milestone_id": 1},
    {"log_subtype_id": 86, "log_subtype_name": "Rate Locked", "milestone_id": 4},
    {"log_subtype_id": 86, "log_subtype_name": "Pre-Approved", "milestone_id": 2},
    {"log_subtype_id": 86, "log_subtype_name": "SoftVu - Get Mortech Rates", "milestone_id": 0}
    ],
87: [
    {"log_subtype_id": 87, "log_subtype_name": "WTD - Employment/Income", "milestone_id": 1},
    {"log_subtype_id": 87, "log_subtype_name": "Send to Mortech", "milestone_id": 0},
    {"log_subtype_id": 87, "log_subtype_name": "Lead: Not Interested", "milestone_id": 1}
    ],
88: [
    {"log_subtype_id": 88, "log_subtype_name": "WTD - Assets", "milestone_id": 1},
    {"log_subtype_id": 88, "log_subtype_name": "Spanish Speaker", "milestone_id": 1},
    {"log_subtype_id": 88, "log_subtype_name": "Outbound Email", "milestone_id": 0}
    ],
89: [
    {"log_subtype_id": 89, "log_subtype_name": "Application - Float", "milestone_id": 1},
    {"log_subtype_id": 89, "log_subtype_name": "Loan Pitched", "milestone_id": 1},
    {"log_subtype_id": 89, "log_subtype_name": "WTD - Equity/Value", "milestone_id": 1}
    ],
90: [
    {"log_subtype_id": 90, "log_subtype_name": "Rate Watch", "milestone_id": 1},
    {"log_subtype_id": 90, "log_subtype_name": "TUD - Selected Other Lender", "milestone_id": 1},
    {"log_subtype_id": 90, "log_subtype_name": "Hold For Time Zone", "milestone_id": 0}
    ],
91: [
    {"log_subtype_id": 91, "log_subtype_name": "Rate Trigger", "milestone_id": 1},
    {"log_subtype_id": 91, "log_subtype_name": "Unable to Reconnect with Lead", "milestone_id": 0},
    {"log_subtype_id": 91, "log_subtype_name": "Inbound Call", "milestone_id": 1}
    ],
92: [
    {"log_subtype_id": 92, "log_subtype_name": "On Phone With Client", "milestone_id": 1},
    {"log_subtype_id": 92, "log_subtype_name": "Made Contact/EMAIL ONLY", "milestone_id": 1},
    {"log_subtype_id": 92, "log_subtype_name": "Future Opportunity", "milestone_id": 1}
    ],
93: [
    {"log_subtype_id": 93, "log_subtype_name": "Application - Purchase Pre-Approval", "milestone_id": 1},
    {"log_subtype_id": 93, "log_subtype_name": "File Started in BBMC", "milestone_id": 6},
    {"log_subtype_id": 93, "log_subtype_name": "In Contract Negotiations", "milestone_id": 2},
    {"log_subtype_id": 93, "log_subtype_name": "File Started", "milestone_id": 6}
    ],
94: [
    {"log_subtype_id": 94, "log_subtype_name": "Change status to Contacted/Call Back", "milestone_id": 1},
    {"log_subtype_id": 94, "log_subtype_name": " 7 attempts - no contact (nurture)", "milestone_id": 0}
    ],
95: [
    {"log_subtype_id": 95, "log_subtype_name": "Agent Sold", "milestone_id": 1},
    {"log_subtype_id": 95, "log_subtype_name": "SoftVu Vu Notification Trigger", "milestone_id": 0}
    ],
96: [
    {"log_subtype_id": 96, "log_subtype_name": "HOT RESPONSE Text Trigger", "milestone_id": 0},
    {"log_subtype_id": 96, "log_subtype_name": "Agent Assigned", "milestone_id": 0}
    ],
97: [
    {"log_subtype_id": 97, "log_subtype_name": "Realtor: Inactivated by Cardinal", "milestone_id": 0},
    {"log_subtype_id": 97, "log_subtype_name": "Credit Watch (possible opportunity in near future)", "milestone_id": 1}
    ],
98: [
    {"log_subtype_id": 98, "log_subtype_name": "ReInquired Lead", "milestone_id": 0},
    {"log_subtype_id": 98, "log_subtype_name": "Realtor Cancelled CoMarketing", "milestone_id": 0}
    ],
99: [
    {"log_subtype_id": 99, "log_subtype_name": "Wait for Realtor Approval", "milestone_id": 0},
    {"log_subtype_id": 99, "log_subtype_name": "SoftVu OptOut", "milestone_id": 0},
    {"log_subtype_id": 99, "log_subtype_name": "SoftVu - Fire LN-Aged Drip Posts", "milestone_id": 0}
    ],
100: [
    {"log_subtype_id": 100, "log_subtype_name": "SoftVu refire  ", "milestone_id": 0},
    {"log_subtype_id": 100, "log_subtype_name": "Welcome Call Complete", "milestone_id": 1},
    {"log_subtype_id": 100, "log_subtype_name": "SoftVu - Fire Pitched Posts", "milestone_id": 0}
    ],
101: [
    {"log_subtype_id": 101, "log_subtype_name": "Updated Roostify Application Assignment", "milestone_id": 0},
    {"log_subtype_id": 101, "log_subtype_name": "Purchase Nurture", "milestone_id": 0}
    ],
102: [
    {"log_subtype_id": 102, "log_subtype_name": "RATE DROP Text Trigger", "milestone_id": 0},
    {"log_subtype_id": 102, "log_subtype_name": "SoftVu - Fire Application Posts", "milestone_id": 0}
    ],
103: [
    {"log_subtype_id": 103, "log_subtype_name": "Do Not Call List", "milestone_id": 0},
    {"log_subtype_id": 103, "log_subtype_name": "Past Customer - working w/ again on a New Loan", "milestone_id": 0}
    ],
104: [
    {"log_subtype_id": 104, "log_subtype_name": "ANY LUCK FINDING A HOME Text Trigger", "milestone_id": 0},
    {"log_subtype_id": 104, "log_subtype_name": "Reverse Mortgage Quote", "milestone_id": 1}
    ],
105: [
    {"log_subtype_id": 105, "log_subtype_name": "Qualification", "milestone_id": 2},
    {"log_subtype_id": 105, "log_subtype_name": "Started/Submitted Roostify Application", "milestone_id": 0},
    {"log_subtype_id": 105, "log_subtype_name": "Client Care - No Program", "milestone_id": 1}
    ],
106: [
    {"log_subtype_id": 106, "log_subtype_name": "SoftVu Send", "milestone_id": 0},
    {"log_subtype_id": 106, "log_subtype_name": "Client Care - Existing Client", "milestone_id": 1},
    {"log_subtype_id": 106, "log_subtype_name": "Resubmission", "milestone_id": 0}
    ],
107: [
    {"log_subtype_id": 107, "log_subtype_name": "SoftVu Open", "milestone_id": 0},
    {"log_subtype_id": 107, "log_subtype_name": "SoftVu Vu Notification Trigger", "milestone_id": 0},
    {"log_subtype_id": 107, "log_subtype_name": "Lead Sent to HomeAgentMatch", "milestone_id": 1}
    ],
108: [
    {"log_subtype_id": 108, "log_subtype_name": "SoftVu ClickVu", "milestone_id": 0},
    {"log_subtype_id": 108, "log_subtype_name": "Export to GCH", "milestone_id": 0},
    {"log_subtype_id": 108, "log_subtype_name": "Active LeadArk Agent", "milestone_id": 0}
    ],
109: [
    {"log_subtype_id": 109, "log_subtype_name": "Welcome Call Pending", "milestone_id": 1},
    {"log_subtype_id": 109, "log_subtype_name": "RATE DROP Email Trigger", "milestone_id": 0}
    ],
110:[ 
    {"log_subtype_id": 110, "log_subtype_name": "CP Transfer-Greg", "milestone_id": 1},
    {"log_subtype_id": 110, "log_subtype_name": "Trigger Roostify App Send (prefill)", "milestone_id": 0}
    ],
111: [
    {"log_subtype_id": 111, "log_subtype_name": "Bridgequal Sent", "milestone_id": 5},
    {"log_subtype_id": 111, "log_subtype_name": "Pre-Qualification Sent", "milestone_id": 5},
    {"log_subtype_id": 111, "log_subtype_name": "AE | Send Realtor Package", "milestone_id": 1}
    ],
112: [
    {"log_subtype_id": 112, "log_subtype_name": "Lead Transferred", "milestone_id": 1},
    {"log_subtype_id": 112, "log_subtype_name": "AE | Realtor Not Interested", "milestone_id": 1}
    ],
113: [
    {"log_subtype_id": 113, "log_subtype_name": "SoftVu OptOut", "milestone_id": 0},
    {"log_subtype_id": 113, "log_subtype_name": "Encompass Merge to Existing Lead", "milestone_id": 2},
    {"log_subtype_id": 113, "log_subtype_name": "Realtor Package Sent", "milestone_id": 0}
    ],
114: [
    {"log_subtype_id": 114, "log_subtype_name": "Email Only - Realtor", "milestone_id": 1},
    {"log_subtype_id": 114, "log_subtype_name": "Transfer-Nate", "milestone_id": 1},
    {"log_subtype_id": 114, "log_subtype_name": "LOAN SUMMARY FOLLOW UP text trigger", "milestone_id": 0}
    ],
115: [
    {"log_subtype_id": 115, "log_subtype_name": "LOAN SUMMARY/READY TO REFI text trigger  ", "milestone_id": 0},
    {"log_subtype_id": 115, "log_subtype_name": "AE | No Realtor Package Required", "milestone_id": 1},
    {"log_subtype_id": 115, "log_subtype_name": "Send to Homebird", "milestone_id": 1}
    ],
116: [
    {"log_subtype_id": 116, "log_subtype_name": "Past Customer - Inquired about a New Loan", "milestone_id": 0},
    {"log_subtype_id": 116, "log_subtype_name": "Realtor Nurture", "milestone_id": 0}
    ],
117: [
    {"log_subtype_id": 117, "log_subtype_name": "WTD - Credit (DO NOT forward to GCH)", "milestone_id": 1},
    {"log_subtype_id": 117, "log_subtype_name": "Send Home Captain Connection Email", "milestone_id": 0},
    {"log_subtype_id": 117, "log_subtype_name": "Do Not Call List", "milestone_id": 1}
    ],
118: [
    {"log_subtype_id": 118, "log_subtype_name": "Went With Another Lender", "milestone_id": 1},
    {"log_subtype_id": 118, "log_subtype_name": "ALO | Transfer - Manual", "milestone_id": 1},
    {"log_subtype_id": 118, "log_subtype_name": "App Form FIlled", "milestone_id": 0}
    ],
119: [
    {"log_subtype_id": 119, "log_subtype_name": "Beta SMS", "milestone_id": 0},
    {"log_subtype_id": 119, "log_subtype_name": "Called: Evening/Additional", "milestone_id": 0},
    {"log_subtype_id": 119, "log_subtype_name": "TM Warm Transfer", "milestone_id": 1}
    ],
120: [
    {"log_subtype_id": 120, "log_subtype_name": "Batch Email Send Trigger", "milestone_id": 0},
    {"log_subtype_id": 120, "log_subtype_name": "Credit Pulled", "milestone_id": 6},
    {"log_subtype_id": 120, "log_subtype_name": "SoftVu Vu Notification Trigger", "milestone_id": 0}
    ],
121: [
    {"log_subtype_id": 121, "log_subtype_name": "Send Sweepstakes Text Message", "milestone_id": 0},
    {"log_subtype_id": 121, "log_subtype_name": "SoftVu OptOut", "milestone_id": 0}
    ],
122: [
    {"log_subtype_id": 122, "log_subtype_name": "CCD - Encompass Application", "milestone_id": 2},
    {"log_subtype_id": 122, "log_subtype_name": "EE Test Action 0617", "milestone_id": 0},
    {"log_subtype_id": 122, "log_subtype_name": "Vets Sweeps Dup Merged", "milestone_id": 0}
    ],
123: [{"log_subtype_id": 123, "log_subtype_name": "SoftVu Send", "milestone_id": 0}],
124: [
    {"log_subtype_id": 124, "log_subtype_name": "SoftVu Open", "milestone_id": 0},
    {"log_subtype_id": 124, "log_subtype_name": "Lead: No Benefit", "milestone_id": 1}
    ],
125: [
    {"log_subtype_id": 125, "log_subtype_name": "Application - Purch Pre-Approval (Home Not Found)", "milestone_id": 1},
    {"log_subtype_id": 125, "log_subtype_name": "SoftVu ClickVu", "milestone_id": 0}
    ],
126: [{"log_subtype_id": 126, "log_subtype_name": "Application - Purch Pre-Approval (Home Found)", "milestone_id": 1}],
127: [
    {"log_subtype_id": 127, "log_subtype_name": "SoftVu - Fire Funded Drips", "milestone_id": 0},
    {"log_subtype_id": 127, "log_subtype_name": "Lead:  Sent to Cardinal Realtor Network", "milestone_id": 1}
    ],
128: [{"log_subtype_id": 128, "log_subtype_name": "Called: CCD - Contacted/Call Back", "milestone_id": 1}],
129: [
    {"log_subtype_id": 129, "log_subtype_name": "RD - Application", "milestone_id": 1},
    {"log_subtype_id"  : 129, "log_subtype_name": "BA BQ F/U", "milestone_id": 0}
    ],
130: [
    {"log_subtype_id": 130, "log_subtype_name": "BA F/U 2", "milestone_id": 0},
    {"log_subtype_id": 130, "log_subtype_name": "Application - Recapture", "milestone_id": 1}
    ],
131: [
    {"log_subtype_id": 131, "log_subtype_name": "BA Closed F/U", "milestone_id": 0},
    {"log_subtype_id": 131, "log_subtype_name": "Lead active on lead provider and/or Cardinal site", "milestone_id": 0}
    ],
132:[ 
    {"log_subtype_id": 132, "log_subtype_name": "BQ Confirmed", "milestone_id": 5},
    {"log_subtype_id": 132, "log_subtype_name": "LeadiD Test Action", "milestone_id": 0}
    ],
133: [
    {"log_subtype_id": 133, "log_subtype_name": "SoftVu Send", "milestone_id": 0},
    {"log_subtype_id": 133, "log_subtype_name": "BQ Not Confirmed", "milestone_id": 5}
    ],
134: [
    {"log_subtype_id": 134, "log_subtype_name": "SoftVu Open", "milestone_id": 0},
    {"log_subtype_id": 134, "log_subtype_name": "Trigger Post", "milestone_id": 0}
    ],
135: [{"log_subtype_id": 135, "log_subtype_name": "SoftVu ClickVu", "milestone_id": 0}],
136: [
    {"log_subtype_id": 136, "log_subtype_name": "Lead Sent to Home Captain", "milestone_id": 0},
    {"log_subtype_id": 136, "log_subtype_name": "Create RFL Contact ", "milestone_id": 0}
    ],
137: [{"log_subtype_id": 137, "log_subtype_name": "Spanish Speaker Requested", "milestone_id": 1}],
138: [{"log_subtype_id": 138, "log_subtype_name": "Application | Pre-App (Sent to Home Captain)", "milestone_id": 1}],
139: [
    {"log_subtype_id": 139, "log_subtype_name": "Export to BBMC Encompass", "milestone_id": 0},
    {"log_subtype_id": 139, "log_subtype_name": "Export to Encompass", "milestone_id": 0},
    {"log_subtype_id": 139, "log_subtype_name": "Application | Pre-App (No Agent | Reject Home Cap)", "milestone_id": 1}
    ],
140: [
    {"log_subtype_id": 140, "log_subtype_name": "Application | Pre-App (Has Real Estate Agent)", "milestone_id": 1},
    {"log_subtype_id": 140, "log_subtype_name": "Copy Lead - Different Address", "milestone_id": 0}
    ],
141: [
    {"log_subtype_id": 141, "log_subtype_name": "Copy Lead - Same Address", "milestone_id": 0},
    {"log_subtype_id": 141, "log_subtype_name": "On Call", "milestone_id": 0}
    ],
142: [
    {"log_subtype_id": 142, "log_subtype_name": "Spanish Speaking", "milestone_id": 1},
    {"log_subtype_id": 142, "log_subtype_name": "Realtor Update", "milestone_id": 0}
    ],
143: [{"log_subtype_id": 143, "log_subtype_name": "CCD - Octane Application", "milestone_id": 2}],
144: [
    {"log_subtype_id": 144, "log_subtype_name": "HB - Shopping", "milestone_id": 0},
    {"log_subtype_id": 144, "log_subtype_name": "Lead: Not Right Now", "milestone_id": 1}
    ],
145: [
    {"log_subtype_id": 145, "log_subtype_name": "HB - Offer In", "milestone_id": 0},
    {"log_subtype_id": 145, "log_subtype_name": "Mkto - Email Opened", "milestone_id": 0}
    ],
146: [
    {"log_subtype_id": 146, "log_subtype_name": "Mkto - Email Clicked", "milestone_id": 0},
    {"log_subtype_id": 146, "log_subtype_name": "HB - Offer Accepted", "milestone_id": 0}
    ],
147: [{"log_subtype_id": 147, "log_subtype_name": "Called: In_Market Notification Addressed", "milestone_id": 0}],
148: [
    {"log_subtype_id": 148, "log_subtype_name": "Mkto - Email Send", "milestone_id": 0},
    {"log_subtype_id": 148, "log_subtype_name": "Sweeps Entrant Verified", "milestone_id": 1}
    ],
149: [{"log_subtype_id": 149, "log_subtype_name": "Called: ALO Contacted | Future Transfer Scheduled", "milestone_id": 1}],
150: [
    {"log_subtype_id": 150, "log_subtype_name": "Post To Anomaly Squared", "milestone_id": 0},
    {"log_subtype_id": 150, "log_subtype_name": "Called: ALO Contacted | Consumer Call Back", "milestone_id": 1}
    ],
151: [
    {"log_subtype_id": 151, "log_subtype_name": "Lead: Not Interested | Consumer Hung Up", "milestone_id": 1},
    {"log_subtype_id": 151, "log_subtype_name": "SoftVu Survey", "milestone_id": 0}
    ],
152: [{"log_subtype_id": 152, "log_subtype_name": "Lead: Not Interested | Not Right Now", "milestone_id": 1}],
153: [
    {"log_subtype_id": 153, "log_subtype_name": "Call Back", "milestone_id": 1},
    {"log_subtype_id": 153, "log_subtype_name": "Lead: Not Interested | Rate Not Competitive", "milestone_id": 1}
    ],
154: [
    {"log_subtype_id": 154, "log_subtype_name": "TUD", "milestone_id": 1},
    {"log_subtype_id": 154, "log_subtype_name": "Emailed: ALO | Transfer | Email Contact", "milestone_id": 1}
    ],
155: [
    {"log_subtype_id": 155, "log_subtype_name": "Sent Text Message", "milestone_id": 0},
    {"log_subtype_id": 155, "log_subtype_name": "WTD", "milestone_id": 1}
    ],
156: [
    {"log_subtype_id": 156, "log_subtype_name": "Need Phone / Info Review", "milestone_id": 0},
    {"log_subtype_id": 156, "log_subtype_name": "Send Marketing Text - CFD", "milestone_id": 0}
    ],
157: [
    {"log_subtype_id": 157, "log_subtype_name": "Send Marketing Text - CD", "milestone_id": 0},
    {"log_subtype_id": 157, "log_subtype_name": "Verifed:  Bad Phone / Info", "milestone_id": 0}
    ],
160: [{"log_subtype_id": 160, "log_subtype_name": "VA Seminar - Confirmed", "milestone_id": 1}],
162: [{"log_subtype_id": 162, "log_subtype_name": "VA Seminar - Attended F/U", "milestone_id": 0}],
164: [{"log_subtype_id": 164, "log_subtype_name": "TD Encompass - Duplicate", "milestone_id": 0}],
166: [{"log_subtype_id": 166, "log_subtype_name": "Cloudvirga API Import (BBMC UAT)", "milestone_id": 0}],
170: [{"log_subtype_id": 170, "log_subtype_name": "Remove Funded Date", "milestone_id": 0}],
171: [{"log_subtype_id": 171, "log_subtype_name": "S.E. Uncontacted Post Action", "milestone_id": 0}],
172: [{"log_subtype_id": 172, "log_subtype_name": "AQ", "milestone_id": 0}],
173: [{"log_subtype_id": 173, "log_subtype_name": "Text Message - Uncontacted", "milestone_id": 0}],
174: [{"log_subtype_id": 174, "log_subtype_name": "S.E.Create Realtor Lead", "milestone_id": 0}],
175: [{"log_subtype_id": 175, "log_subtype_name": "S.E. RFL A track task done", "milestone_id": 0}],
176: [{"log_subtype_id": 176, "log_subtype_name": "S.E. RFL move to B track", "milestone_id": 0}],
177: [{"log_subtype_id": 177, "log_subtype_name": "SoftVu - Send LO Funded LT Review", "milestone_id": 0}],
178: [{"log_subtype_id": 178, "log_subtype_name": "SoftVu - Send LO Funded Zillow Review", "milestone_id": 0}],
179: [{"log_subtype_id": 179, "log_subtype_name": "Hung Up", "milestone_id": 0}],
180: [
    {"log_subtype_id": 180, "log_subtype_name": "File Started in MoOM", "milestone_id": 6},
    {"log_subtype_id": 180, "log_subtype_name": "File Started in MoOM", "milestone_id": 0}
    ],
181: [
    {"log_subtype_id": 181, "log_subtype_name": "Pre-Qualification Sent2", "milestone_id": 5},
    {"log_subtype_id": 181, "log_subtype_name": "Pre-Qualification Sent", "milestone_id": 5}
    ],
182: [{"log_subtype_id": 182, "log_subtype_name": "*KG CD Batch to Five9 ", "milestone_id": 0}],
184: [{"log_subtype_id": 184, "log_subtype_name": "Bank Deposits - Batch Email Trigger", "milestone_id": 0}],
186: [{"log_subtype_id": 186, "log_subtype_name": "Contact Attempt: Called/Left Message (Movers)", "milestone_id": 0}],
187: [{"log_subtype_id": 187, "log_subtype_name": "Contact Attempt: Called/No Message (Movers)", "milestone_id": 0}],
188: [{"log_subtype_id": 188, "log_subtype_name": "Send Credit Law Center Email", "milestone_id": 0}],
189: [{"log_subtype_id": 189, "log_subtype_name": "Contact Attempt: Called/Sent Email", "milestone_id": 0}],
190: [{"log_subtype_id": 190, "log_subtype_name": "Contact Attempt: Called/Sent Text", "milestone_id": 0}],
191: [{"log_subtype_id": 191, "log_subtype_name": "Contact Attempt: Called/Sent Blind Prop (autotext)", "milestone_id": 0}],
192: [{"log_subtype_id": 192, "log_subtype_name": "Contact Attempt: Called/Sent Email (Movers)", "milestone_id": 0}],
193: [{"log_subtype_id": 193, "log_subtype_name": "Contact Attempt: Called/Sent Text (Movers)", "milestone_id": 0}],
195: [{"log_subtype_id": 195, "log_subtype_name": "BLIND LOAN PROPOSAL sent Text Trigger", "milestone_id": 0}],
196: [{"log_subtype_id": 196, "log_subtype_name": "BLIND LOAN PROPOSAL FollowUp Text Trigger", "milestone_id": 0}],
198: [{"log_subtype_id": 198, "log_subtype_name": "SoftVu Survey", "milestone_id": 0}],
201: [{"log_subtype_id": 201, "log_subtype_name": "TEST Action", "milestone_id": 0}],
204: [{"log_subtype_id": 204, "log_subtype_name": "Pre-Movers Response", "milestone_id": 0}],
205: [{"log_subtype_id": 205, "log_subtype_name": "nestiny", "milestone_id": 0}],
206: [{"log_subtype_id": 206, "log_subtype_name": "Send to Home Captain Realty", "milestone_id": 0}],
207: [{"log_subtype_id": 207, "log_subtype_name": "Application Sent - asked borrower to complete ", "milestone_id": 1}],
1936: [{"log_subtype_id": 1936, "log_subtype_name": "Called: Left Message", "milestone_id":0}]
}