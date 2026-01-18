import asyncio


async def consume(queue, worker_id, consumer_function):
    print(f'Worker {worker_id} started')
    while True:
        item = await queue.get()
        await consumer_function(item)
        queue.task_done()


class QueueProcessor:
    def __init__(self, consumer_count: int, consumer_function):
        self._queue = asyncio.Queue()
        self._consumer_count = consumer_count
        self._consumer_function = consumer_function
        self._consumers = []
        self._results = []

    async def put(self, item: any):
        await self._queue.put(item)

    async def _consume(self):
        while True:
            item = await self._queue.get()
            result = await self._consumer_function(item)
            if result is not None:
                self._results.append(result)
            self._queue.task_done()

    async def wait_until_done(self):
        await self._queue.join()
        print('Processing finished')

    @property
    def results(self):
        return self._results

    @property
    def size(self):
        return self._queue.qsize()

    async def __aenter__(self):
        print('Adding Consumers')
        for i in range(self._consumer_count):
            self._consumers.append(asyncio.create_task(
                self._consume()
            ))

    async def __aexit__(self, exc_type, exc, tb):
        for consumer in self._consumers:
            consumer.cancel()

        print('Consumers Stopped')
