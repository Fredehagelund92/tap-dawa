import datetime
import pytz
import json

import singer
import singer.metrics as metrics
from singer import metadata
from singer import Transformer
from singer.schema import Schema

LOGGER = singer.get_logger()

SDC_DELETED_AT = "_sdc_deleted_at"

def add_automatic_properties(catalog_entry):
    catalog_entry.schema.properties[SDC_DELETED_AT] = Schema(
        type=["null", "string"],
        format='date-time'
        )
    return catalog_entry

def handle_delete_rows(record):
    event_ts = datetime.datetime.strptime(record['tidspunkt'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=pytz.UTC)

    vals = record["data"]
    vals[SDC_DELETED_AT] = event_ts

    return vals
def handle_update_rows(record):
    event_ts = datetime.datetime.strptime(record['tidspunkt'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=pytz.UTC)

    vals = record["data"]
    vals[SDC_DELETED_AT] = event_ts

    return vals

def handle_insert_rows(record):

    vals = record["data"]
    return vals


def sync_stream(state, instance):
    stream = instance.stream
    with metrics.record_counter(stream.tap_stream_id) as counter:
        for (stream, record) in instance.sync(state):
            counter.increment()


            try:
                with Transformer() as transformer:

                    catalog = add_automatic_properties(stream)

                    if record["operation"] == 'update':
                        row = handle_update_rows(record)

                        record = transformer.transform(row, catalog.schema.to_dict(), metadata.to_map(stream.metadata))
                    elif record["operation"] == 'insert':
                        row = handle_insert_rows(record)

                        record = transformer.transform(row, catalog.schema.to_dict(), metadata.to_map(stream.metadata))
                    elif record["operation"] == 'delete':
                        row = handle_delete_rows(record)

                    else:
                        LOGGER.info("Skipping row for stream %s.%s as it is not an INSERT, UPDATE, or DELETE record",
                                stream.tap_stream_id)
                        continue

                    record = transformer.transform(row, catalog.schema.to_dict(), metadata.to_map(stream.metadata))

                singer.write_record(stream.tap_stream_id, record)

                if instance.replication_method == "LOG_BASED":
                    singer.write_state(state)

            except Exception as e:
                LOGGER.error('Handled exception: {error}'.format(error=str(e)))
                continue


        return counter.value