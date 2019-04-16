import asyncio
import concurrent.futures
import time


def job(d):
    print('task started ' + str(d))
    time.sleep(1)
    print('task finished ' + str(d))


executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

loop = asyncio.get_event_loop()
tasks = [loop.run_in_executor(executor, job, i) for i in range(15)]
loop.run_until_complete(asyncio.gather(*tasks))