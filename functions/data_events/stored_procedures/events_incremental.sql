create procedure sp_external_events_increment()
as $$
BEGIN

lock external_events, external_events_upsert;

update external_events
set
modified_at = external_events_upsert.modified_at,
log_type = external_events_upsert.log_type,
log_date = external_events_upsert.log_date,
log_subtype_name = external_events_upsert.log_subtype_name,
milestone_id = external_events_upsert.milestone_id,
milestone_name = external_events_upsert.milestone_name,
log_note = external_events_upsert.log_note,
log_user_id = external_events_upsert.log_user_id,
log_user_email = external_events_upsert.log_user_email,
log_user_name = external_events_upsert.log_user_name,
group_id = external_events_upsert.group_id,
group_name = external_events_upsert.group_name,
imported = external_events_upsert.imported,
updated_at = external_events_upsert.updated_at,
log_type_id = external_events_upsert.log_type_id
from external_events_upsert
where external_events.account_id = external_events_upsert.account_id
and external_events.account_lead_id = external_events_upsert.account_lead_id
and external_events.log_id = external_events_upsert.log_id
and external_events.log_subtype_id = external_events_upsert.log_subtype_id;

delete from external_events_upsert
using external_events
where external_events.account_id = external_events_upsert.account_id
and external_events.account_lead_id = external_events_upsert.account_lead_id
and external_events.log_id = external_events_upsert.log_id
and external_events.log_subtype_id = external_events_upsert.log_subtype_id;

insert into external_events(
created_at,
account_id,
account_lead_id,
modified_at,
log_type,
log_id,
log_date,
log_subtype_id,
log_subtype_name,
milestone_id,
milestone_name,
log_note,
log_user_id,
log_user_email,
log_user_name,
group_id,
group_name,
imported,
updated_at,
log_type_id
)
select
created_at,
account_id,
account_lead_id,
modified_at,
log_type,
log_id,
log_date,
log_subtype_id,
log_subtype_name,
milestone_id,
milestone_name,
log_note,
log_user_id,
log_user_email,
log_user_name,
group_id,
group_name,
imported,
updated_at,
log_type_id
from external_events_upsert;

delete from external_events_upsert;

END;
$$
LANGUAGE plpgsql;

