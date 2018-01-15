from datetime import date

from g2x_helpers.pyfpds import Contracts
from g2x_helpers.utils import nested_get
import pandas as pd

from pandas.tseries.offsets import BDay

NAICS_LIST = ['541611', '541618', '541613', '541511', '541512', '541513',
              '541519', '518210', '541612', '519130', '541990', '541690',
              '511210', '541720']
# '3600'
FUNDING_AGENCY_LIST = ['97DH', '7504', '7505', '7522', '7523', '7524',
                       '7526', '7527', '7528', '7529', '7530', '7570']

award_map = {
    'referenced_idv_piid': 'content|award|awardID|referencedIDVID|PIID',
    'contract_piid': 'content|award|awardID|awardContractID|PIID',
    'contract_mod_number': 'content|award|awardID|awardContractID|modNumber',
    'referenced_idvid_modnumber': 'content|award|awardID|referencedIDVID|modNumber',
    'last_modified_date': 'content|award|transactionInformation|lastModifiedDate',
    'funding_agency_name': 'content|award|purchaserInformation|fundingRequestingAgencyID|@name',
    'funding_agency_id': 'content|award|purchaserInformation|fundingRequestingAgencyID|#text',
    'funding_office_name': 'content|award|purchaserInformation|fundingRequestingOfficeID|@name',
    'funding_office_id': 'content|award|purchaserInformation|fundingRequestingOfficeID|#text',
    'naics_code': 'content|award|productOrServiceInformation|principalNAICSCode|#text',
    'naics_desc': 'content|award|productOrServiceInformation|principalNAICSCode|@description',
    'vendor_name': 'content|award|vendor|vendorHeader|vendorName',
    'vendor_duns': 'content|award|vendor|vendorSiteDetails|vendorDUNSInformation|DUNSNumber',
    'global_duns': 'content|award|vendor|vendorSiteDetails|vendorDUNSInformation|globalParentDUNSNumber',
    'signed_date': 'content|award|relevantContractDates|signedDate',
    'action_obligation': 'content|award|dollarValues|obligatedAmount',
    'contracting_office': 'content|award|purchaserInformation|contractingOfficeAgencyID|@name',
    'contracting_office_number': 'content|award|purchaserInformation|contractingOfficeAgencyID|#text',
    'current_completion_date': 'content|award|relevantContractDates|currentCompletionDate',
    'solicitation_id': 'content|award|contractData|solicitationID',
    'number_of_offers_received': 'content|award|competition|numberOfOffersReceived',
    'type_of_contract_pricing_description': 'content|award|contractData|typeOfContractPricing|@description',
    'ultimate_completion_date': 'content|award|relevantContractDates|ultimateCompletionDate',
    'effective_date': 'content|award|relevantContractDates|effectiveDate',
    'major_program_code': 'content|award|contractData|majorProgramCode',
    'base_and_all_options_value': 'content|award|dollarValues|baseAndAllOptionsValue',
    'description_of_contract_requirement': 'content|award|contractData|descriptionOfContractRequirement',
    'solicitation_procedures_description': 'content|award|competition|solicitationProcedures|@description',
    'solicitation_procedures_text': 'content|award|competition|solicitationProcedures|#text',
    'extent_competed_description': 'content|award|competition|extentCompeted|@description',
    'extent_competed_text': 'content|awardcompetition|extentCompeted|#text',
    'type_of_set_aside_description': 'content|award|competition|typeOfSetAside|@description',
    'type_of_set_aside_text': 'content|award|competition|typeOfSetAside|#text',
    'created_by': 'content|award|transactionInformation|createdBy',
    'created_date': 'content|award|transactionInformation|createdDate',
    'contract_action_type_description': 'content|award|contractData|contractActionType|@description',
}

idv_map = {
    'referenced_idv_piid': 'content.IDV.contractID.referencedIDVID.PIID',
    'contract_piid': 'content.IDV.contractID.IDVID.PIID',
    'contract_mod_number': 'content.IDV.contractID.IDVID.modNumber',
    'referenced_idvid_modnumber': 'content.IDV.contractID.referencedIDVID.modNumber',
    'last_modified_date': 'content.IDV.transactionInformation.lastModifiedDate',
    'funding_agency_name': 'content.IDV.purchaserInformation.fundingRequestingAgencyID.@name',
    'funding_agency_id': 'content.IDV.purchaserInformation.fundingRequestingAgencyID.#text',
    'funding_office_name': 'content.IDV.purchaserInformation.fundingRequestingOfficeID.@name',
    'funding_office_id': 'content.IDV.purchaserInformation.fundingRequestingOfficeID.#text',
    'naics_code': 'content.IDV.productOrServiceInformation.principalNAICSCode.#text',
    'naics_desc': 'content.IDV.productOrServiceInformation.productOrServiceCode.@description',
    'vendor_name': 'content.IDV.vendor.vendorHeader.vendorName',
    'vendor_duns': 'content.IDV.vendor.vendorSiteDetails.vendorDUNSInformation.DUNSNumber',
    'global_duns': 'content.IDV.vendor.vendorSiteDetails.vendorDUNSInformation.globalParentDUNSNumber',
    'signed_date': 'content.IDV.relevantContractDates.signedDate',
    'action_obligation': 'content.IDV.dollarValues.obligatedAmount',
    'contracting_office': 'content.IDV.purchaserInformation.contractingOfficeID.@name',
    'contracting_office_number': 'content.IDV.purchaserInformation.contractingOfficeAgencyID.#text',
    'solicitation_id': 'content.IDV.contractData.solicitationID',
    'number_of_offers_received': 'content.IDV.competition.numberOfOffersReceived',
    'type_of_contract_pricing_description': 'content.IDV.contractData.typeOfContractPricing.@description',
    'ultimate_completion_date': 'content.IDV.relevantContractDates.lastDateToOrder',
    'effective_date': 'content.IDV.relevantContractDates.effectiveDate',
    'major_program_code': 'content.IDV.contractData.majorProgramCode',
    'base_and_all_options_value': 'content.IDV.dollarValues.baseAndAllOptionsValue',
    'description_of_contract_requirement': 'content.IDV.contractData.descriptionOfContractRequirement',
    'solicitation_procedures_description': 'content.IDV.competition.solicitationProcedures.@description',
    'solicitation_procedures_text': 'content.IDV.competition.solicitationProcedures.#text',
    'extent_competed_description': 'content.IDV.competition.extentCompeted.@description',
    'extent_competed_text': 'content.IDV.competition.extentCompeted.#text',
    'type_of_set_aside_description': 'content.IDV.competition.typeOfSetAside.@description',
    'type_of_set_aside_text': 'content.IDV.competition.typeOfSetAside.#text',
    'contract_marketing_data_maximum_order_limit': 'content.IDV.contractMarketingData.maximumOrderLimit',
    'created_by': 'content.IDV.transactionInformation.createdBy',
    'created_date': 'content.IDV.transactionInformation.createdDate',
    'contract_action_type_description': 'content.IDV.contractData.contractActionType.@description',
}


def date_between_list(stop=date.today()):
    start = stop - BDay(5)
    return "[{:%Y/%m/%d},{:%Y/%m/%d}]".format(start, stop)


def fpds_pull(idv_map=idv_map, award_map=award_map,
              naics_list=NAICS_LIST, **kwargs):
    for funding_agency in FUNDING_AGENCY_LIST:
        for naics in NAICS_LIST:
            c = Contracts()
            records = c.get(naics_code=naics, funding_agency_id=funding_agency,
                            num_records="all", **kwargs)
            for record in records:
                if 'IDV' in record['content']:
                    d = {key: nested_get(record, val, _sep='.') for key, val in idv_map.items()}
                    d['content_type'] = 'IDV'
                    yield d
                elif 'award' in record['content']:
                    d = {key: nested_get(record, val, _sep='|') for key, val in award_map.items()}
                    d['content_type'] = 'Award'
                    yield d


dates = date_between_list()

def get_intel():
    dates = date_between_list()
    try:
        li = fpds_pull(modification_number=0,
                       obligated_amount='[200000,)',
                       last_modified_date=dates)
    except xml.parsers.expat.ExpatError:
        pass
    lst = list(li)
    pd.set_option('display.max_colwidth', -1)
    df = pd.DataFrame(lst)
    try:
        df.base_and_all_options_value = df.base_and_all_options_value.astype(float).fillna(0.0)
        df.action_obligation = df.action_obligation.astype(float).fillna(0.0)
        df['action_obligation_fmt'] = df['action_obligation'].map('${:,.2f}'.format)
        df['base_and_all_options_value_fmt'] = df['base_and_all_options_value'].map('${:,.2f}'.format)
        df['effective_date'] = pd.to_datetime(df['effective_date'])
        df['ultimate_completion_date'] = pd.to_datetime(df['ultimate_completion_date'])
        df['created_date'] = pd.to_datetime(df['created_date'])
        df['signed_date'] = pd.to_datetime(df['signed_date'])
        df['last_modified_date'] = pd.to_datetime(df['last_modified_date'])
        df['duration'] = (df['ultimate_completion_date'] - df['effective_date']).dt.days
        df['duration'] = df['duration'] / 30
        df['duration'] = df['duration'].apply(lambda x: round(x, 0))
        df['today'] = pd.to_datetime('today')
        df['mark_for_deletion'] = (df['today'] - df['created_date']).dt.days
        df['ultimate_completion_date_fmt'] = df['ultimate_completion_date'].dt.strftime('%Y-%m-%d')
        df['created_date_fmt'] = df['created_date'].dt.strftime('%Y-%m-%d')
        df['signed_date_fmt'] = df['signed_date'].dt.strftime('%Y-%m-%d')
        df['effective_date_fmt'] = df['effective_date'].dt.strftime('%Y-%m-%d')
        df['today_fmt'] = df['today'].dt.strftime('%Y-%m-%d')
        df['last_modified_date_fmt'] = df['last_modified_date'].dt.strftime('%Y-%m-%d')
    except KeyError:
        pass
    df = df[df['mark_for_deletion'] < 121]
    df.sort_values(by=['last_modified_date', 'created_date'], ascending=False, inplace=True)
    intel = df.to_dict(orient='records')
    return df

test = get_intel()
test.to_csv('test.csv')