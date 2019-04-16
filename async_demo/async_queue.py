import time
import asyncio
import aiohttp
from bs4 import BeautifulSoup


class Downloader:
    def __init__(self, links, concurrency=2):

        self.links = links
        self.link_list = []
        self.host = 'http://kyqgs.mnr.gov.cn'
        self.queue = asyncio.Queue()
        self.concurrency = concurrency
        self.headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5)",
                        "Accept": "text/html,application/xhtml+xml,"}

    async def download(self, url, session):
        async with session.get(url, headers=self.headers) as response:
            if response.status != 200:
                return
            content = await response.text()

        soup = BeautifulSoup(content, 'lxml')
        a = soup.find('table', class_='project_table').find_all('a')
        titles = [{'_id': self.host + title.get('href')} for title in a]
        print(titles)
        self.link_list.extend(titles)

    async def worker(self, session):
        while True:
            link = await self.queue.get()

            try:
                await self.download(link, session)

            finally:
                self.queue.task_done()

    async def run(self):
        start = time.time()
        print('Starting downloading')

        await asyncio.wait([self.queue.put(link) for link in self.links])

        async with aiohttp.ClientSession() as session:
            tasks = [asyncio.create_task(self.worker(session)) for _ in range(self.concurrency)]
            await self.queue.join()
            for task in tasks:
                task.cancel()
            # await asyncio.gather(*tasks)

        end = time.time()
        print('FINISHED AT {} secs'.format(end-start))


if __name__ == '__main__':
    mylinks = []
    for d in range(2, 37):
        mylinks.append('http://kyqgs.mnr.gov.cn/search_projects_sx.jspx?pageNo=' + str(d) +
                       '&firstListTotalCount=0&secondListTotalCount=3619&thirdListTotalCount=0' +
                       '&fourthListTotalCount=0&times=sj-2&kuangquans=kq-2&areas=dq-25&pagecount=362&_goPs=1')

    downloader = Downloader(mylinks, concurrency=8)
    # asyncio.run(downloader.run())
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(downloader.run())
    finally:
        loop.close()

    print('How many links in the list: ' + str(len(downloader.link_list)))
