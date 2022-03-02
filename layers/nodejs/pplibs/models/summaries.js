'use strict';
var summaries = {
  'tableName': 'summaries',
  'tableProperties': {
    'account_lead_id': {
      'type': 'int'
    },
    'account': { 
      'type': 'string'
    },
    'reassignment': { 
      'type': 'int'
    },
    'error': { 
      'type': 'bool'
    },
    'rec_code': { 
      'type': 'string'
    },
    'custom_rank_code': { 
      'type': 'string'
    },
    'custom_rank_value': { 
      'type': 'string'
    },
    'created_at': { 
      'type': 'string'
    },
    'previous_assignment_agent_id': { 
      'type': 'int'
    },
    'previous_assignment_profile_id': { 
      'type': 'int'
    },
    'previous_assignment_match': { 
      'type': 'bool'
    },
    'available_agents': { 
      'type': 'int'
    },
    'available_agents_threshold': { 
      'type': 'int'
    },
    'agents_added': { 
      'type': 'int'
    },
    'expansion': { 
      'type': 'int'
    },
  }
};
module.exports = summaries;