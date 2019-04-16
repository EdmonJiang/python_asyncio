import asyncio
import time


async def job(d):
    print('task started ' + str(d))
    await asyncio.sleep(d)
    print('task finished ' + str(d))


async def main(loop):
    tasks = [loop.create_task(job(x)) for x in range(1, 15)]
    await asyncio.wait(tasks)

start = time.time()
loop = asyncio.get_event_loop()
loop.run_until_complete(main(loop))
loop.close()
print(time.time() - start)