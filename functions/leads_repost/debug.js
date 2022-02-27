const repost = require('./repost_leads.js');
const scheduler = require('./scheduler.js');

const event = {
    start_date: '2020-06-15 00:00:00',
    end_date: '2020-06-16 00:00:00'
}
//repost.handler(event, {}, (a,b) => {})

scheduler.handler(event, {}, (a,b) => {})