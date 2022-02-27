create or replace procedure sp_el_increment()
as $$ 
BEGIN
delete from external_leads 
using external_leads_stage 
where external_leads.account_id = external_leads_stage.account_id
and   external_leads.account_lead_id = external_leads_stage.account_lead_id ;

insert into external_leads( 
created_at,
 account_id,
 account_lead_id,
 source,
 campaign_group,
 credit_profile_stated,
 credit_score_stated,
 credit_score_range_stated,
 bankruptcy_stated,
 purchase_price_stated,
 home_value_stated,
 loan_amount_stated,
 cash_out_stated,
 purpose_detail_stated,
 property_address_stated,
 property_address_2_stated,
 property_city_stated,
 property_state_stated,
 hmda_state_id,
 property_zip_code_stated,
 property_type_stated,
 property_use_stated,
 day_phone,
 evening_phone,
 email_stated,
 first_name_stated,
 last_name_stated,
 va_eligible_stated,
 updated_at,
 gender_predicted             ) 
select 
created_at,
 account_id,
 account_lead_id,
 source,
 campaign_group,
 credit_profile_stated,
 credit_score_stated,
 credit_score_range_stated,
 bankruptcy_stated,
 purchase_price_stated,
 home_value_stated,
 loan_amount_stated,
 cash_out_stated,
 purpose_detail_stated,
 property_address_stated,
 property_address_2_stated,
 property_city_stated,
 property_state_stated,
 hmda_state_id,
 property_zip_code_stated,
 property_type_stated,
 property_use_stated,
 day_phone,
 evening_phone,
 email_stated,
 first_name_stated,
 last_name_stated,
 va_eligible_stated,
 updated_at,
 gender_predicted 
from external_leads_stage ;

delete from external_leads_stage;

END;
$$ language plpgsql 
