'use strict';
var recommendations = {
  'tableName': 'recommendations',
  'tableProperties': {
    'account_lead_id': {
      'type': 'int'
    },
    'agent_id': { 
      'type': 'int'
    },
    'account': { 
      'type': 'string'
    },
    'probability': { 
      'type': 'float'
    },
    'rank': { 
      'type': 'int'
    },
    'recommendation': { 
      'type': 'string'
    },
    'ramdom': { 
      'type': 'bool'
    },
    'error': { 
      'type': 'bool'
    },
    'algo_sample': { 
      'type': 'string'
    },
    'reassignment': { 
      'type': 'int'
    },
    'created_at': { 
      'type': 'string'
    },
  }
};
module.exports = recommendations;