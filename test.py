from datetime import date
import re

from g2x_helpers.pyfpds import Contracts
from g2x_helpers.utils import nested_get
import pandas as pd

from pandas.tseries.offsets import BDay
import logging

l = logging.getLogger()
l.disabled = True

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


def date_between_list(start=None, stop=date.today(), num_days=5):
    if start is None:
        start = stop - BDay(num_days)
    return "[{:%Y/%m/%d},{:%Y/%m/%d}]".format(start, stop)



# def fpds_pull(idv_map=idv_map, award_map=award_map,
#               naics_list=NAICS_LIST, **kwargs):
#     for funding_agency in FUNDING_AGENCY_LIST:
#         for naics in NAICS_LIST:
#             c = Contracts()
#             records = c.get(naics_code=naics, funding_agency_id=funding_agency,
#                             num_records="all", **kwargs)
#             for record in records:
#                 if 'IDV' in record['content']:
#                     d = {key: nested_get(record, val, _sep='.') for key, val in idv_map.items()}
#                     d['content_type'] = 'IDV'
#                     print(d)
#                     yield d
#                 elif 'award' in record['content']:
#                     d = {key: nested_get(record, val, _sep='|') for key, val in award_map.items()}
#                     d['content_type'] = 'Award'
#                     print(d)
#                     yield d

def fpds_pull(feed_url=None,
              idv_map=idv_map,
              award_map=award_map,
              naics_list=NAICS_LIST,
              **kwargs):
    recList = []
    for funding_agency in FUNDING_AGENCY_LIST:
        for naics in NAICS_LIST:
            c = Contracts(feed_url=feed_url)
            records = c.get(naics_code=naics, funding_agency_id=funding_agency,
                            num_records="all", **kwargs)
            for record in records:
                if 'IDV' in record['content']:
                    d = {key: nested_get(record, val, _sep='.')
                         for key, val in idv_map.items()}
                    d['content_type'] = 'IDV'
                    recList.append(d)
                elif 'award' in record['content']:
                    d = {key: nested_get(record, val, _sep='|')
                         for key, val in award_map.items()}
                    d['content_type'] = 'Award'
                    recList.append(d)
    pd.set_option('display.max_colwidth', -1)
    combined_df = pd.DataFrame(recList)
    return combined_df



def etl_process(num_days=5):
    base_url = 'https://www.fpds.gov/ezsearch/FEEDS/ATOM?s=FPDS&FEEDNAME=PUBLIC&VERSION=1.5.1&q='
    dates = date_between_list(num_days=num_days)
    df = fpds_pull(feed_url=base_url,
                   modification_number=0,
                   obligated_amount='[0,)',
                   date_signed=dates)
    df.fillna('0', inplace=True)
    df['last_modified_int'] = df.apply(lambda x: '%s' % (re.sub("[^0-9]","", x['last_modified_date'])), axis=1)
    id_list = ['contract_piid', 'contract_mod_number', 'referenced_idv_piid',
               'referenced_idvid_modnumber', 'last_modified_int']
    df['id'] = df[id_list].apply(lambda x: ''.join(x), axis=1)
    df['vendor_as_int'] = df['vendor_duns'].astype(int)
    df['global_as_int'] = df['global_duns'].astype(int)
    df['vendor_duns'] = df['vendor_duns'].apply('{:0>9}'.format)
    df['global_duns'] = df['global_duns'].apply('{:0>9}'.format)
    df.drop('last_modified_int', axis=1, inplace=True)

    # Convert dates to datetime objects
    df['effective_date'] = pd.to_datetime(df['effective_date'], errors='coerce')
    df['ultimate_completion_date'] = pd.to_datetime(df['ultimate_completion_date'], errors='coerce')
    df['created_date'] = pd.to_datetime(df['created_date'], errors='coerce')
    df['signed_date'] = pd.to_datetime(df['signed_date'], errors='coerce')
    df['last_modified_date'] = pd.to_datetime(df['last_modified_date'], errors='coerce')
    df['current_completion_date'] = pd.to_datetime(df['current_completion_date'], errors='coerce')
    return df


test = etl_process()
test.to_csv('test.csv')
