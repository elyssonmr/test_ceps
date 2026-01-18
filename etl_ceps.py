import asyncio
from datetime import datetime

import pandas as pd

from cep_processor.database import Database
from cep_processor.exporter import Exporter
from cep_processor.http_client import CepHttpClient
from cep_processor.queue_processor import QueueProcessor


async def main():
    before = datetime.now()
    client = CepHttpClient(30)
    request_processos = QueueProcessor(
        consumer_count=30, consumer_function=client.request_cep_api
    )
    ceps = pd.read_csv('ceps.csv')
    print('Starting Processing CEP Data')

    async with request_processos:
        for _, row in ceps.iterrows():
            await request_processos.put(row['CEP'].astype(str))

        print(f'Processing {request_processos.size} items')
        await request_processos.wait_until_done()

    await client.close()

    # Export Data
    print('Exporting Data')
    exporter = Exporter()
    exporter.process_results(request_processos.results)
    exporter.export_to_csv('results.csv')
    exporter.export_to_xml('results.xml')
    exporter.export_errors_to_csv('errors.csv')
    print('Data Exported')

    # Save to a database
    print('Saving to database')

    db = Database()
    # Ensure tables are created
    await db.create_tables()

    db_processor = QueueProcessor(
        consumer_count=25, consumer_function=db.save_cep
    )

    async with db_processor:
        for cep in exporter.success_results:
            await db_processor.put(cep)

        print(f'Processing {db_processor.size} items')
        await db_processor.wait_until_done()
        print('CEPS Saved at the database')

    await db.close()

    after = datetime.now()
    print(f'Processing time: {after - before}')
    print('Finished!')


if __name__ == '__main__':
    asyncio.run(main())
