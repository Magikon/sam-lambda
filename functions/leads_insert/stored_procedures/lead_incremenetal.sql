create or replace procedure sp_leads_increment()
as $$ 
BEGIN


update leads
set 
state = leads_upsert.state, 
payload = leads_upsert.payload, 
updated_at = getdate() 
from leads_upsert
where leads.customer_id = leads_upsert.customer_id 
and leads.customer_lead_id = leads_upsert.customer_lead_id; 

delete from leads_upsert 
using leads 
where leads.customer_id = leads_upsert.customer_id 
and leads.customer_lead_id = leads_upsert.customer_lead_id;  

insert into leads(
    customer_id,
    customer_lead_id,
    state,
    created_at,
    updated_at,
    payload
)
select
customer_id,
customer_lead_id,
state,
created_at,
updated_at,
payload
from leads_upsert;


END
$$ language plpgsql 
