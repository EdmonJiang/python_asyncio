import asyncio
import time
import aiohttp
from bs4 import BeautifulSoup


async def job(s, d, cornum):
    yn2018_362 = 'http://kyqgs.mnr.gov.cn/search_projects_sx.jspx?pageNo=' + str(d) + \
                 '&firstListTotalCount=0&secondListTotalCount=3619&thirdListTotalCount=0' + \
                 '&fourthListTotalCount=0&times=sj-2&kuangquans=kq-2&areas=dq-25&pagecount=362&_goPs=1'
    async with cornum:
        async with s.get(yn2018_362, headers=header) as r:
            if r.status == 200:
                html = await r.text()
                soup = BeautifulSoup(html, 'lxml')
                a = soup.find('table', class_='project_table').find_all('a')
                links = []
                for link in a:
                    links.append({'_id': host + link.get('href')})
                print(links)
                link_list.extend(links)


async def run():
    tasks = []
    cornum = asyncio.Semaphore(8)  # 限制并发量为500
    async with aiohttp.ClientSession() as session:  # aiohttp建议整个应用只创建一个session，不能为每个请求创建一个seesion
        for i in range(2, 37):
            task = asyncio.create_task(job(session, i, cornum))
            tasks.append(task)
        await asyncio.gather(*tasks)


if __name__ == '__main__':
    host = 'http://kyqgs.mnr.gov.cn'
    header = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0'}
    link_list = []

    start = time.time()
    asyncio.run(run())

    print('How many links in the list: ' + str(len(link_list)))
    print(time.time() - start)
