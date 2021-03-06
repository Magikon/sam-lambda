create or replace procedure sp_eld_increment()
as $$ 
BEGIN
delete from external_lead_details 
using external_lead_details_stage 
where external_lead_details.account_id = external_lead_details_stage.account_id
and   external_lead_details.account_lead_id = external_lead_details_stage.account_lead_id ;

insert into external_lead_details( 
                        account_lead_id, 
                        profile_id_loan_officer_default, 
                        profile_id_first_assignment_user, 
                        profile_id_user, 
                        profile_id_qualification, 
                        profile_id_processor, 
                        profile_id_sales_manager, 
                        profile_id_underwriter, 
                        profile_id_encompass, 
                        propair_lead_score, 
                        cur_loan_number, 
                        los_loan_number, 
                        borrower_city, 
                        borrower_state, 
                        borrower_zip, 
                        lead_datetime, 
                        lead_date, 
                        lead_distribution, 
                        lead_price, 
                        lead_score_account, 
                        lead_type, 
                        leadid_universal_lead_id, 
                        cis_score, 
                        leadid_consumervelocity_12hr, 
                        leadid_consumervelocity_1hr, 
                        leadid_consumervelocity_5mins, 
                        leadid_consumervelocity_pastdy, 
                        leadid_consumervelocity_pastwk, 
                        leadid_dataintegrity, 
                        leadid_devicefrequency_12hrs, 
                        leadid_devicefrequency_1hr, 
                        leadid_devicefrequency_5mins, 
                        leadid_devicefrequency_pastdy, 
                        leadid_devicefrequency_pastwk, 
                        leadid_intentscore_id, 
                        leadid_intentscore_outcome, 
                        leadid_leadage, 
                        leadid_leaddupe_email, 
                        leadid_leaddupe_leadid, 
                        leadid_leadduration, 
                        leadid_leadlineage_entities, 
                        leadid_leadlineage_hops, 
                        leadid_leadvelocity_12hr, 
                        leadid_leadvelocity_1hr, 
                        leadid_leadvelocity_5mins, 
                        leadid_leadvelocity_pastdy, 
                        leadid_leadvelocity_pastwk, 
                        action_count, 
                        email_linksclicked_count, 
                        email_sent_count, 
                        email_sent_lastdate, 
                        first_assignment_date, 
                        branch_group, 
                        first_contact_attempt_date, 
                        down_payment, 
                        flagged, 
                        ltv_stated, 
                        log_count, 
                        source_detail, 
                        source_description, 
                        source_division, 
                        source_division2, 
                        lead_status, 
                        time_zone, 
                        hhd_income, 
                        firstcontact_dow, 
                        firstcontact_mins_from_midnight, 
                        firstcontact_tod, 
                        second_rate_existing, 
                        second_rate_type_existing, 
                        trusted_form_id, 
                        current_loan_type, 
                        current_mortgage_rate, 
                        second_loan, 
                        domain, 
                        bin_domain, 
                        domain_tld, 
                        bin_domain_tld, 
                        contact_lead_date, 
                        lead_date_day, 
                        lead_date_hour, 
                        event_count, 
                        sec_to_first_contact, 
                        sec_to_first_attempt, 
                        sec_to_first_assignment, 
                        sec_to_qualification, 
                        lead_cluster1, 
                        lead_cluster1_cr, 
                        lead_cluster2, 
                        lead_cluster2_cr, 
                        lead_cluster3, 
                        lead_cluster3_cr, 
                        total_contact_attempts, 
                        exclusivity, 
                        date_last_refi, 
                        ltv_datamyx, 
                        mtg_balance_datamyx, 
                        ifha_va, 
                        iharp2, 
                        iloan_type, 
                        imortgage_balance, 
                        phone_code_telesign, 
                        phone_description_telesign, 
                        last_action_date, 
                        source_vendor_id, 
                        source_filter_id, 
                        loan_consultant, 
                        current_lender, 
                        loan_program, 
                        loan_close_date, 
                        current_servicer, 
                        source_reference, 
                        account_id, 
                        icontact, 
                        iqualification, 
                        milestone_date_contact, 
                        milestone_date_qualification, 
                        last_duplicate_found_by, 
                        created_at, 
                        updated_at 
            ) 
select 
                        account_lead_id, 
                        profile_id_loan_officer_default, 
                        profile_id_first_assignment_user, 
                        profile_id_user, 
                        profile_id_qualification, 
                        profile_id_processor, 
                        profile_id_sales_manager, 
                        profile_id_underwriter, 
                        profile_id_encompass, 
                        propair_lead_score, 
                        cur_loan_number, 
                        los_loan_number, 
                        borrower_city, 
                        borrower_state, 
                        borrower_zip, 
                        lead_datetime, 
                        lead_date, 
                        lead_distribution, 
                        lead_price, 
                        lead_score_account, 
                        lead_type, 
                        leadid_universal_lead_id, 
                        cis_score, 
                        leadid_consumervelocity_12hr, 
                        leadid_consumervelocity_1hr, 
                        leadid_consumervelocity_5mins, 
                        leadid_consumervelocity_pastdy, 
                        leadid_consumervelocity_pastwk, 
                        leadid_dataintegrity, 
                        leadid_devicefrequency_12hrs, 
                        leadid_devicefrequency_1hr, 
                        leadid_devicefrequency_5mins, 
                        leadid_devicefrequency_pastdy, 
                        leadid_devicefrequency_pastwk, 
                        leadid_intentscore_id, 
                        leadid_intentscore_outcome, 
                        leadid_leadage, 
                        leadid_leaddupe_email, 
                        leadid_leaddupe_leadid, 
                        leadid_leadduration, 
                        leadid_leadlineage_entities, 
                        leadid_leadlineage_hops, 
                        leadid_leadvelocity_12hr, 
                        leadid_leadvelocity_1hr, 
                        leadid_leadvelocity_5mins, 
                        leadid_leadvelocity_pastdy, 
                        leadid_leadvelocity_pastwk, 
                        action_count, 
                        email_linksclicked_count, 
                        email_sent_count, 
                        email_sent_lastdate, 
                        first_assignment_date, 
                        branch_group, 
                        first_contact_attempt_date, 
                        down_payment, 
                        flagged, 
                        ltv_stated, 
                        log_count, 
                        source_detail, 
                        source_description, 
                        source_division, 
                        source_division2, 
                        lead_status, 
                        time_zone, 
                        hhd_income, 
                        firstcontact_dow, 
                        firstcontact_mins_from_midnight, 
                        firstcontact_tod, 
                        second_rate_existing, 
                        second_rate_type_existing, 
                        trusted_form_id, 
                        current_loan_type, 
                        current_mortgage_rate, 
                        second_loan, 
                        domain, 
                        bin_domain, 
                        domain_tld, 
                        bin_domain_tld, 
                        contact_lead_date, 
                        lead_date_day, 
                        lead_date_hour, 
                        event_count, 
                        sec_to_first_contact, 
                        sec_to_first_attempt, 
                        sec_to_first_assignment, 
                        sec_to_qualification, 
                        lead_cluster1, 
                        lead_cluster1_cr, 
                        lead_cluster2, 
                        lead_cluster2_cr, 
                        lead_cluster3, 
                        lead_cluster3_cr, 
                        total_contact_attempts, 
                        exclusivity, 
                        date_last_refi, 
                        ltv_datamyx, 
                        mtg_balance_datamyx, 
                        ifha_va, 
                        iharp2, 
                        iloan_type, 
                        imortgage_balance, 
                        phone_code_telesign, 
                        phone_description_telesign, 
                        last_action_date, 
                        source_vendor_id, 
                        source_filter_id, 
                        loan_consultant, 
                        current_lender, 
                        loan_program, 
                        loan_close_date, 
                        current_servicer, 
                        source_reference, 
                        account_id, 
                        icontact, 
                        iqualification, 
                        milestone_date_contact, 
                        milestone_date_qualification, 
                        last_duplicate_found_by, 
                        created_at, 
                        updated_at 
from external_lead_details_stage ;

delete from external_lead_details_stage;

END;
$$ language plpgsql 
