import hashlib
import logging
import sys

from turbine.runtime import RecordList, Runtime

logging.basicConfig(level=logging.INFO)

def print_records(records: RecordList) -> RecordList:
    print(records)
    return


class App:
    @staticmethod
    async def run(turbine: Runtime):
        try:
            source = await turbine.resources("kafka-test")

            records = await source.records("user_activity", {'conduit':'true', 'readFromBeginning': 'true'})

            printed = await turbine.process(records, print_records)

            destination_db = await turbine.resources("pg")

            await destination_db.write(printed, "messages")
        except Exception as e:
            print(e, file=sys.stderr)
