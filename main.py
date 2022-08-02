import hashlib
import logging
import sys
import typing as t

from turbine.runtime import Record, Runtime

logging.basicConfig(level=logging.INFO)


def passthrough(records: t.List[Record]) -> t.List[Record]:
    print(f"cheatham kafka records: {records}")
    return records


class App:
    @staticmethod
    async def run(turbine: Runtime):
        try:
            source = await turbine.resources("kafka-test")

            records = await source.records("user_activity", {'conduit':'true', 'readFromBeginning': 'true'})

            transform = await turbine.process(records, passthrough)

            destination_db = await turbine.resources("kafka-test")

            await destination_db.write(transform, "collection_archive", {'conduit':'true', 'topic': 'eric-test'})
        except Exception as e:
            print(e, file=sys.stderr)
