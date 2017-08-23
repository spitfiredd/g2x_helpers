'''
File of dictionary, list, and pandas utility functions
'''
from functools import reduce
import csv
from datetime import date
from dateutil.relativedelta import relativedelta
import pandas as pd

from g2x_helpers.pyfpds import Contracts


def nested_get(_dict, keys, default=None, _sep='.'):
    ''' Returns the value from a dict with deep nested keys
    '''
    if isinstance(keys, str):
        _keys = keys.split(_sep)
    else:
        _keys = keys

    def _reducer(d, key):
        if isinstance(d, dict):
            return d.get(key, default)
        return default
    return reduce(_reducer, _keys, _dict)


def flatten_dict(d):
    """Flatten a nested diction
    """
    def items():
        for key, value in d.items():
            if isinstance(value, dict):
                for subkey, subvalue in flatten_dict(value).items():
                    yield key + "." + subkey, subvalue
            else:
                yield key, value
    return dict(items())


def create_mapping(filename):
    """
    Creates a python dictionay from a csv file. The csv must only hold
    two columns of data.
    """
    reader = csv.reader(open(str(filename), 'r'))
    _d = {}
    for row in reader:
        k, v = row
        _d[k] = v
    return _d


def date_between_list(start=None, stop=date.today()):
    date_list = []
    # stop = date.today()
    start = start
    end = start + relativedelta(day=1, months=12, days=-1)
    while True:
        if end > stop:
            dates_between = "[{:%Y/%m/%d},{:%Y/%m/%d}]".format(start, stop)
            date_list.append(dates_between)
            break
        else:
            dates_between = "[{:%Y/%m/%d},{:%Y/%m/%d}]".format(start, end)
            start = end + relativedelta(day=+1, months=1)
            end = start + relativedelta(day=1, months=12, days=-1)
            date_list.append(dates_between)
    return date_list


def convert_to_null(dataframe, *cols):
    dataframe['referenced_idvid_piid'] = dataframe['referenced_idvid_piid'].fillna(0)
    out = dataframe.where((pd.notnull(dataframe)), None)
    return out


def create_dict(dataframe):
    return dataframe.to_dict(orient='records')


def pull_data(**kwargs):
    c = Contracts()
    records = c.get(**kwargs)
    return records


def create_keep_list(filename):
    df = pd.read_csv(filename)
    return df['keepList'].tolist()


def apply_keep_list_to_frame(df, keep_list):
    return df[keep_list]


def check_if_col_exists(dataframe, list_):
    for item in list_:
        if item not in dataframe:
            dataframe[item] = 0
    return dataframe


def data_pull(naics_list=None, dept=None, idv_mapping=None,
              award_mapping=None, keep_list=None, **kwargs):
    combined_df = pd.DataFrame()
    d_name = '"{}"'.format(dept)
    for naics in naics_list:
        c = Contracts()
        records = c.get(naics_code=naics, department_name=d_name, **kwargs)
        for record in records:
            if 'IDV' in record['content']:
                _ivdd = flatten_dict(record)
                _idvl = list()
                _idvl.append(_ivdd)
                _tmp = pd.DataFrame(_idvl)
                _tmp = _tmp.rename(index=str, columns=idv_mapping)
                _tmp = apply_keep_list_to_frame(_tmp, keep_list)
                # idv = idv.append(_tmp)
                combined_df = combined_df.append(_tmp)
            elif 'award' in record['content']:
                _awardsd = flatten_dict(record)
                _awardsl = list()
                _awardsl.append(_awardsd)
                _tmp1 = pd.DataFrame(_awardsl)
                _tmp1 = _tmp1.rename(index=str, columns=award_mapping)
                _tmp1 = apply_keep_list_to_frame(_tmp1, keep_list)
                # award = award.append(_tmp1)
                combined_df = combined_df.append(_tmp1)
    combined_df = convert_to_null(combined_df,
                                  'referenced_idvid_piid')
    return combined_df


def data_pull_funding_agency(idv_mapping=None,
                             award_mapping=None,
                             award_keep=None,
                             ivd_keep=None,
                             naics_list=None,
                             funding_agency=None,
                             **kwargs):
    combined_df = pd.DataFrame()
    d_name = '"{}"'.format(funding_agency)
    for naics in naics_list:
        c = Contracts()
        records = c.get(naics_code=naics, num_records="all",
                        funding_agency_id=d_name, **kwargs)
        for record in records:
            if 'IDV' in record['content']:
                _ivdd = flatten_dict(record)
                _idvl = list()
                _idvl.append(_ivdd)
                _tmp = pd.DataFrame(_idvl)
                _tmp = _tmp.rename(index=str, columns=idv_mapping)
                _tmp = check_if_col_exists(_tmp, award_keep)
                _tmp = apply_keep_list_to_frame(_tmp, award_keep)
                # idv = idv.append(_tmp)
                combined_df = combined_df.append(_tmp)
            elif 'award' in record['content']:
                _awardsd = flatten_dict(record)
                _awardsl = list()
                _awardsl.append(_awardsd)
                _tmp1 = pd.DataFrame(_awardsl)
                _tmp1 = _tmp1.rename(index=str, columns=award_mapping)
                _tmp1 = check_if_col_exists(_tmp1, ivd_keep)
                _tmp1 = apply_keep_list_to_frame(_tmp1, ivd_keep)
                # award = award.append(_tmp1)
                combined_df = combined_df.append(_tmp1)
    combined_df = convert_to_null(combined_df,
                                  'referenced_idvid_piid')
    return combined_df


def data_pull_general(idv_mapping=None,
                      award_mapping=None,
                      award_keep=None,
                      ivd_keep=None,
                      naics_list=None,
                      **kwargs):
    combined_df = pd.DataFrame()
    for naics in naics_list:
        c = Contracts()
        records = c.get(naics_code=naics, num_records="all", **kwargs)
        for record in records:
            if 'IDV' in record['content']:
                _ivdd = flatten_dict(record)
                _idvl = list()
                _idvl.append(_ivdd)
                _tmp = pd.DataFrame(_idvl)
                _tmp = _tmp.rename(index=str, columns=idv_mapping)
                _tmp = check_if_col_exists(_tmp, award_keep)
                _tmp = apply_keep_list_to_frame(_tmp, award_keep)
                # idv = idv.append(_tmp)
                combined_df = combined_df.append(_tmp)
            elif 'award' in record['content']:
                _awardsd = flatten_dict(record)
                _awardsl = list()
                _awardsl.append(_awardsd)
                _tmp1 = pd.DataFrame(_awardsl)
                _tmp1 = _tmp1.rename(index=str, columns=award_mapping)
                _tmp1 = check_if_col_exists(_tmp1, ivd_keep)
                _tmp1 = apply_keep_list_to_frame(_tmp1, ivd_keep)
                # award = award.append(_tmp1)
                combined_df = combined_df.append(_tmp1)
    combined_df = convert_to_null(combined_df,
                                  'referenced_idvid_piid')
    return combined_df
