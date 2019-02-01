import requests
import singer
import json
import tempfile
import csv

LOGGER = singer.get_logger()

ITER_CHUNK_SIZE = 1024

BASE_URL = "https://dawa.aws.dk"




class Client(object):

    def __init__(self):
        self.entity=None

    def _get_response(self, url, headers=None, stream=None):

        if stream is None:
            stream = False

        if headers is None:
            headers = {}


        LOGGER.info("Requesting {url}".format(url=url))
        response = requests.get(url, headers=headers, stream=stream)
        response.raise_for_status()

        return response

    def current_txid(self):
        url = '%s/replikering/senestetransaktion' % BASE_URL

        data = self._get_response(url).json()

        return data['txid']


    def _get_initial_replication(self):
        current_txid = self.current_txid()
        url = '%s/replikering/udtraek?entitet=%s&txid=%s&format=csv' % (BASE_URL, self.entity, current_txid)

        headers = dict()
        headers['Content-Type'] = 'text/csv'

        with tempfile.NamedTemporaryFile(mode="w+", encoding="utf8") as csv_file:
            resp = self._get_response(url, headers=headers, stream=True)
            for chunk in resp.iter_content(chunk_size=ITER_CHUNK_SIZE, decode_unicode=True):
                if chunk:
                    # Replace any NULL bytes in the chunk so it can be safely given to the CSV reader
                    csv_file.write(chunk.replace('\0', ''))

            csv_file.seek(0)
            csv_reader = csv.reader(
                csv_file,
                delimiter=',',
                quotechar='"'
            )
            # Get header values
            column_name_list = next(csv_reader)

            for line in csv_reader:
                rec = dict(zip(column_name_list, line))

                vals = {}
                vals["operation"] = 'insert'
                vals["data"] = rec
                yield vals

    def _get_changes(self, last_txid):
        current_txid = self.current_txid()

        url = '%s/replikering/haendelser?entitet=%s&txid=%s' % (BASE_URL,self.entity, last_txid) 

        if last_txid == self.current_txid:
            LOGGER.info("No new records for {url}".format(url=self.entity))
            return

        resp = self._get_response(url).json()

        for item in resp:
            yield item


    def _get(self, txid):
        if txid is None:
            for item in self._get_initial_replication():
                yield item
        else:
            for item in self._get_changes(txid):
                yield item


    def adgangsadresse(self, txid=None):
        self.entity = 'adgangsadresse'
        for item in self._get(txid):
            yield item

    def adresse(self, txid=None):

        self.entity = 'adresse'
        for item in self._get(txid):
            yield item

    def postnummer(self, txid=None):
        self.entity = 'postnummer'
        for item in self._get(txid):
            yield item

    def vejstykke(self, txid=None):
        self.entity = 'vejstykke'
        for item in self._get(txid):
            yield item

    def vejpunkt(self, txid=None):
        self.entity = 'vejpunkt'
        for item in self._get(txid):
            item.pop('position', None)
            yield item
