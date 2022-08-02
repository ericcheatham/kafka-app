import hashlib
import logging
import sys
import typing as t

from turbine.runtime import Record, Runtime

logging.basicConfig(level=logging.INFO)


def passthrough(records: t.List[Record]) -> t.List[Record]:
    return records


class App:
    @staticmethod
    async def run(turbine: Runtime):
        try:
            source = await turbine.resources("kafka-test")

            records = await source.records("user_records", {})

            anonymized = await turbine.process(records, passthrough)

            destination_db = await turbine.resources("webhook-dest")

            await destination_db.write(anonymized, "collection_archive", {})
        except Exception as e:
            print(e, file=sys.stderr)
