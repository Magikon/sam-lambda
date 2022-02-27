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

event_types['creationlog'] = { 
    'LogId': { 'field_name': 'log_id', 'field_type': 'BIGID'},
    'LogDate': { 'field_name': 'log_date', 'field_type': 'TIMESTAMP'},
    'CreatedByAgentId': { 'field_name': 'log_user_id', 'field_type': 'BIGID'},
    'CreatedByAgentName': { 'field_name': 'log_user_name', 'field_type': 'NAME'},
    'CreatedByAgentEmail': { 'field_name': 'log_user_email', 'field_type': 'EMAIL'},
    'CreatedByGroupId': { 'field_name': 'group_id', 'field_type': 'ID'},
    'CreatedByGroupName': { 'field_name': 'group_name', 'field_type': 'NAME'},
    'Imported': { 'field_name': 'imported', 'field_type': 'ID'}
}

event_types['sync'] = { 
    'LogId': { 'field_name': 'log_id', 'field_type': 'BIGID'},
    'LogDate': { 'field_name': 'log_date', 'field_type': 'TIMESTAMP'},
    'Name': { 'field_name': 'log_subtype_name', 'field_type': 'NAME'},
    'Message': { 'field_name': 'log_note', 'field_type': 'TEXT'},
    'AgentId': { 'field_name': 'log_user_id', 'field_type': 'BIGID'},
    'AgentName': { 'field_name': 'log_user_name', 'field_type': 'NAME'},
    'AgentEmail': { 'field_name': 'log_user_email', 'field_type': 'EMAIL'}
}

event_types['action'] = { 
    'LogId': { 'field_name': 'log_id', 'field_type': 'BIGID'},
    'ActionDate': { 'field_name': 'log_date', 'field_type': 'TIMESTAMP'},
    'ActionTypeId': { 'field_name': 'log_subtype_id', 'field_type': 'ID'},
    'ActionTypeName': { 'field_name': 'log_subtype_name', 'field_type': 'NAME'},
    'Message': { 'field_name': 'log_note', 'field_type': 'TEXT'},
    'AgentId': { 'field_name': 'log_user_id', 'field_type': 'BIGID'},
    'AgentName': { 'field_name': 'log_user_name', 'field_type': 'NAME'},
    'AgentEmail': { 'field_name': 'log_user_email', 'field_type': 'EMAIL'},
    'GroupId': { 'field_name': 'group_id', 'field_type': 'ID'},
    'GroupName': { 'field_name': 'group_name', 'field_type': 'NAME'},
    'MilestoneId': { 'field_name': 'milestone_id', 'field_type': 'ID'}
}

event_types['email'] = { 
    'LogId': { 'field_name': 'log_id', 'field_type': 'BIGID'},
    'SendDate': { 'field_name': 'log_date', 'field_type': 'TIMESTAMP'},
    'EmailTemplateId': { 'field_name': 'log_subtype_id', 'field_type': 'ID'},
    'EmailTemplateName': { 'field_name': 'log_subtype_name', 'field_type': 'NAME'},
    'AgentId': { 'field_name': 'log_user_id', 'field_type': 'BIGID'},
    'AgentName': { 'field_name': 'log_user_name', 'field_type': 'NAME'},
    'AgentEmail': { 'field_name': 'log_user_email', 'field_type': 'VARCHAR'},
    'GroupId': { 'field_name': 'group_id', 'field_type': 'ID'},
    'GroupName': { 'field_name': 'group_name', 'field_type': 'NAME'}
}

event_types['export'] = { 
    'LogId': { 'field_name': 'log_id', 'field_type': 'BIGID'},
    'LogDate': { 'field_name': 'log_date', 'field_type': 'TIMESTAMP'},
    'Result': { 'field_name': 'log_subtype_name', 'field_type': 'NAME'},
    'Message': { 'field_name': 'log_note', 'field_type': 'TEXT'},
    'AgentId': { 'field_name': 'log_user_id', 'field_type': 'BIGID'},
    'AgentName': { 'field_name': 'log_user_name', 'field_type': 'NAME'},
    'AgentEmail': { 'field_name': 'log_user_name', 'field_type': 'EMAIL'}
}

event_types['status'] = { 
    'LogId': { 'field_name': 'log_id', 'field_type': 'BIGID'},
    'LogDate': { 'field_name': 'log_date', 'field_type': 'TIMESTAMP'},
    'StatusId': { 'field_name': 'log_subtype_id', 'field_type': 'ID'},
    'StatusTitle': { 'field_name': 'log_subtype_name', 'field_type': 'NAME'},
    'Result': { 'field_name': 'log_note', 'field_type': 'TEXT'},
    'AgentId': { 'field_name': 'log_user_id', 'field_type': 'BIGID'},
    'AgentName': { 'field_name': 'log_user_name', 'field_type': 'NAME'},
    'AgentEmail': { 'field_name': 'log_user_email', 'field_type': 'EMAIL'},
    'GroupId': { 'field_name': 'group_id', 'field_type': 'ID'},
    'GroupName': { 'field_name': 'group_name', 'field_type': 'NAME'}
}

event_types['distribution'] = { 
    'LogId': { 'field_name': 'log_id', 'field_type': 'BIGID'},
    'LogDate': { 'field_name': 'log_date', 'field_type': 'TIMESTAMP'},
    'DistributionProgramId': { 'field_name': 'log_subtype_id', 'field_type': 'ID'},
    'DistributionProgramName': { 'field_name': 'log_subtype_name', 'field_type': 'NAME'},
    'Result': { 'field_name': 'log_note', 'field_type': 'TEXT'},
    'AssignedAgentId': { 'field_name': 'log_user_id', 'field_type': 'BIGID'},
    'AssignedAgentName': { 'field_name': 'log_user_name', 'field_type': 'NAME'},
    'AssignedAgentEmail': { 'field_name': 'log_user_email', 'field_type': 'EMAIL'},
    'AssignedGroupId': { 'field_name': 'group_id', 'field_type': 'ID'},
    'AssignedGroupName': { 'field_name': 'group_name', 'field_type': 'NAME'}
}

event_types['assignment'] = { 
    'LogId': { 'field_name': 'log_id', 'field_type': 'BIGID'},
    'LogDate': { 'field_name': 'log_date', 'field_type': 'TIMESTAMP'},
    'AssignedAgentId': { 'field_name': 'log_subtype_id', 'field_type': 'BIGID'},
    'AssignedAgentName': { 'field_name': 'log_subtype_name', 'field_type': 'NAME'},
    'AssignedAgentEmail': { 'field_name': 'log_note', 'field_type': 'EMAIL'},
    'AssignedByAgentId': { 'field_name': 'log_user_id', 'field_type': 'BIGID'},
    'AssignedByAgentName': { 'field_name': 'log_user_name', 'field_type': 'NAME'},
    'AssignedByAgentEmail': { 'field_name': 'log_user_email', 'field_type': 'EMAIL'},
    'AssignedGroupId': { 'field_name': 'group_id', 'field_type': 'ID'},
    'AssignedGroupName': { 'field_name': 'group_name', 'field_type': 'NAME'}
}