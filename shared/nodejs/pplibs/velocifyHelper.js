const soap = require('soap');
const XML = require('pixl-xml');
const _ = require('lodash');

const leadsServiceWsdl = 'https://service.leads360.com/ClientService.asmx?wsdl';
const campaignConfig = require('./config/campaigns.json');
const fieldsConfig = require('./config/fields.json');

/**
 * Returns a SOAP client for consuming the velocify API
 * @returns {Promise<any>}
 */
function getClient() {
    return soap.createClientAsync(leadsServiceWsdl);
}

/**
 * Returns the proper velocify field id of a propair field
 * @param {string} fieldName Name of the propair field
 * @returns {string}
 */
function getFieldId(fieldName) {
    const fieldConfig = _.find(fieldsConfig, f => f.propairFieldName === fieldName);
    return fieldConfig && fieldConfig.fieldId;
}

/**
 * Returns a random campaign ID
 * @returns {string}
 */
function getRandomCampaignId() {
    return _.sample(campaignConfig).CampaignId;
}

/**
 * Generates an XML representing a lead with the format expected by velocify
 * @param {any} lead Lead payload
 */
function generateLeadXML(lead, fields) {
    let fieldTemplates = '';
    _.forEach(fields, item => {
        fieldTemplates += `<Field FieldId="${item.id}" Value="${item.value}" />`;
    });
    return `<Lead Id="${lead.id}"><Fields>${fieldTemplates}</Fields></Lead>`;
}

module.exports = class VelocifyHelper {
    /**
     * Gets a lead by ID from velocify with soap
     * @param {string} leadId ID of the desired lead
     * @param {string} username Velocify username
     * @param {string} password Velocify password
     * @returns {Promise<any>}
     */
    static async getLead(leadId, username, password) {
        const payload = { leadId, username, password };
        const client = await getClient();
        const result = await client.GetLeadAsync(payload);
        const lead = result[0]['GetLeadResult']['Leads']['Lead'];
        return lead;
    }
    

    /**
     * Verifies if a lead exists in velocify
     * @param {string} leadId ID of the desired lead
     * @param {string} username Velocify username
     * @param {string} password Velocify password
     * @returns {Promise<boolean>} true=exists, false=doesn't exist
     */
    static async leadExists(leadId, username, password) {
        try {
            await VelocifyHelper.getLead(leadId, username, password);
            return true;
        } catch (e) {
            return false;
        }
    }

    /**
     * Adds a set of leads to velocify
     * @param {any[]} leads Array containing the new leads data
     * @param {string} username Velocify username
     * @param {string} password Velocify password
     * @returns {Promise<void>}
     */
    static async addLeads(leads, username, password) {
        const leadsXML = `<Leads>${_.map(leads, l => generateLeadXML(l))}</Leads>`;
        const body = `<leads>${leadsXML}</leads><username>${username}</username><password>${password}</password>`;
        const payload = `<AddLeads xmlns="https://service.leads360.com">${body}</AddLeads>`;
        const client = await getClient();
        const result = await client.AddLeadsAsync({ _xml: payload });
        return result[0]['AddLeadsResult']['Response'];
    }

    static async modifyLeads(leads, username, password, fields) {
        const leadsXML = `<Leads>${_.map(leads, l => generateLeadXML(l, fields))}</Leads>`;
        const body = `<username>${username}</username><password>${password}</password><leads>${leadsXML}</leads>`;
        const payload = `<ModifyLeads xmlns="https://service.leads360.com">${body}</ModifyLeads>`;
        const client = await getClient();
        const result = await client.ModifyLeadsAsync({ _xml: payload });
        return result[0]['ModifyLeadsResult']['Response']['Modifications'];
    }

    static async getCampaigns(username, password) {
        const payload = {username, password}
        const client = await getClient();
        const result = await client.GetCampaignsAsync(payload);
        let campaigns = {}
        for (let campaign of result[0]['GetCampaignsResult']['Campaigns']['Campaign']) {
            campaigns[campaign.attributes['CampaignId']] = campaign.attributes
        }
        return campaigns
    }
    
    /**
     * Modifies the value of a field of an existing lead in velocify
     * @param {string} leadId ID of the lead to modify
     * @param {string} fieldId ID of the lead field to modify
     * @param {string} newValue New value for the desired field
     * @param {string} username Velocify username
     * @param {string} password Velocify password
     * @returns {Promise<void>}
     */
    static async modifyLeadField(leadId, fieldId, newValue, username, password) {
        const payload = { leadId, fieldId, newValue, username, password };
        const client = await getClient();
        const result = await client.ModifyLeadFieldAsync(payload);
        return result[0]['ModifyLeadFieldResult']['Response'];
    }
}
