import os
import json
import datetime
import pytz
import singer
from singer import metadata
from singer import utils
from dateutil.parser import parse

logger = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

class Stream():
    name = None
    replication_method = None
    replication_key = None
    stream = None
    key_properties = None
    session_bookmark = None

    def __init__(self, client=None):
        self.client = client

    def get_bookmark(self, state):
        return singer.get_bookmark(state, self.name, self.replication_key)

    # def update_bookmark(self, state, value):
    #     state = singer.write_bookmark(state,
    #                                   tap_stream_id,
    #                                   'log_file',
    #                                   log_file)

    #     state = singer.write_bookmark(state,
    #                                   tap_stream_id,
    #                                   'log_pos',
    #                                   log_pos)

    # return state

    def load_schema(self):
        schema_file = "schemas/{}.json".format(self.name)
        with open(get_abs_path(schema_file)) as f:
            schema = json.load(f)
        return schema

    def load_metadata(self):
        schema = self.load_schema()
        mdata = metadata.new()

        mdata = metadata.write(mdata, (), 'table-key-properties', self.key_properties)
        mdata = metadata.write(mdata, (), 'forced-replication-method', self.replication_method)

        if self.replication_key:
            mdata = metadata.write(mdata, (), 'valid-replication-keys', [self.replication_key])
        for field_name in schema['properties'].keys():
            if field_name in self.key_properties or field_name == self.replication_key:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
            else:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

        return metadata.to_list(mdata)

    # The main sync function.
    def sync(self, state):
        get_data = getattr(self.client, self.name)
        bookmark = self.get_bookmark(state)
        res = get_data()

        for item in res:
            try:
                yield (self.stream, item)
            except Exception as e:
                logger.error('Handled exception: {error}'.format(error=str(e)))
                pass


class Adgangsadresse(Stream):
    name = "adgangsadresse"
    replication_method = "LOG_BASED"
    replication_key = "id"
    key_properties = [ "id" ]

class Adresse(Stream):
    name = "adresse"
    replication_method = "LOG_BASED"
    replication_key = "id"
    key_properties = [ "id" ]

class Postnummer(Stream):
    name = "postnummer"
    replication_method = "LOG_BASED"
    replication_key = "nr"
    key_properties = [ "nr" ]

class Vejstykke(Stream):
    name = "vejstykke"
    replication_method = "LOG_BASED"
    replication_key = "id"
    key_properties = [ "id" ]

class Vejpunkt(Stream):
    name = "vejpunkt"
    replication_method = "LOG_BASED"
    replication_key = "id"
    key_properties = [ "id" ]



STREAMS = {
    "adgangsadresse": Adgangsadresse,
    "adresse": Adresse,
    "postnummer": Postnummer,
    "vejstykke": Vejstykke,
    "vejpunkt": Vejpunkt
}
