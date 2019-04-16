import asyncio


async def hello1():
    print('hello1--start')
    await asyncio.sleep(1)
    print('hello1--finished')


async def hello2():
    print('hello2--start')
    await asyncio.sleep(1)
    print('hello2--finished')


loop = asyncio.get_event_loop()
# futu = asyncio.ensure_future(hello1())
# futu = asyncio.ensure_future(hello2())
# loop.run_until_complete(futu)

# tasks = [hello1(), hello2()]
# loop.run_until_complete(asyncio.gather(*tasks))

tasks = [asyncio.ensure_future(hello1()), asyncio.ensure_future(hello2())]
loop.run_until_complete(asyncio.wait(tasks))
