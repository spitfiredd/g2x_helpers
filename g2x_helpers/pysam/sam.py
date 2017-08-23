# -*- coding: utf-8 -*-

__author__ = 'Daniel Donovan'
__email__ = 'spitfiredd@gmail.com'
__version__ = '0.0.1'

from collections import OrderedDict
import requests
import json
import warnings

warnings.filterwarnings('ignore')

# sample pull
# base: https://api.data.gov/sam/v4/registrations/
# duns: 0261572350000
# api: ?api_key=your-api-key

# Usage
# c = Sam('your-api-key')

# t = c.get('6928857')
# c.pretty_print(t)


class SAMImporter():

    feed_url = 'https://api.data.gov/sam/v4/registrations/'
    api = '?api_key='
    query_url = ''

    def __init__(self, api_key, logger=None):
        self.api_key = str(api_key)
        # point logger to a log function, print by default
        if logger:
            self.log = logger
        else:
            self.log = print

    def pretty_print(self, data):
        self.log(json.dumps(data, indent=4))

    def pad_contract(self, contract):
        '''
        Given a contract, this will pad leading and trailing,
        zeros. A dun number is 9 and the dun plus 4 is 13.
        Thirteen is the length of the digit passed to api.data.gov.
        '''
        return '{:0<13}'.format('{:0>9}'.format(contract))

    def get(self, duns_number):

        params = self.pad_contract(duns_number)
        query = self.feed_url + params + self.api + self.api_key
        self.log("querying {}".format(query))
        resp = requests.get(query, verify=False, timeout=60)
        self.query_url = resp.url
        self.log("finished querying {}".format(self.query_url))
        try:
            data = json.loads(resp.text)
        except ValueError:
            self.log("No results for query")

        return data
