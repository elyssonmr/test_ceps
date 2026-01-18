import pandas as pd
import asyncio

from cep_processor.queue_processor import QueueProcessor
from cep_processor.http_client import CepHttpClient


async def main():
    client = CepHttpClient()
    processor = QueueProcessor(
        consumer_count=50,
        consumer_function=client.request_cep_api
    )
    ceps = pd.read_csv('ceps.csv')
    print('Starting Processing CEP Data')

    async with processor:
        for _, row in ceps.iterrows():
            await processor.put(row['CEP'].astype(str))

        print(f'Processing {processor._queue.qsize()} items')
        await processor.wait_until_done()

    await client.close()

if __name__ == '__main__':
    asyncio.run(main())
