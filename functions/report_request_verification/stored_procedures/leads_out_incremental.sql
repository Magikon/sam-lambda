create or replace procedure sp_leads_out_incremental()
as $$ 
BEGIN

delete from leads_out_upsert 
using leads_out 
where leads_out.lead_id = leads_out_upsert.lead_id 
and leads_out.request_id = leads_out_upsert.request_id
and leads_out.account_id = leads_out_upsert.account_id;  

insert into leads_out(
    request_id,
    lead_id,
    account_id,
    account_name,
    rec_type,
    success,
    result,
    created_at,
    rec_count
)
select
request_id,
lead_id,
account_id,
account_name,
rec_type,
success,
result,
created_at,
rec_count
from leads_out_upsert;

delete from leads_out_upsert;

END
$$ language plpgsql 
