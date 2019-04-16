import asyncio
import time
import aiohttp
from bs4 import BeautifulSoup
import re
from pymongo import MongoClient


class Crawler:
    def __init__(self, url, collection, concurrency=8):
        self.semaphore = asyncio.Semaphore(concurrency)
        self.host = 'http://kyqgs.mnr.gov.cn'
        self.base_url = url
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0'}
        self.collection = collection
        self.link_list = set()
        self.tasks = []
        self.new_tasks = []

    async def get_link(self, session, pageno):
        url = self.base_url.format(pageNo=pageno)
        # print(url)
        print(f'获取第{pageno}页链接.')
        async with self.semaphore:
            async with session.get(url, headers=self.headers) as r:
                if r.status != 200:
                    return
                html = await r.text()
                soup = BeautifulSoup(html, 'lxml')
                a = soup.find('table', class_='project_table').find_all('a')
                for link in a:
                    href = self.host + link.get('href')
                    self.link_list.add(href)
                    # print(link.get_text())
                    task = asyncio.create_task(self.get_detail(session, href))
                    self.new_tasks.append(task)

    async def get_detail(self, session, url):
        async with self.semaphore:
            async with session.get(url, headers=self.headers) as r:
                if r.status != 200:
                    return
                html = await r.text()
                d = {'链接': url}
                soup = BeautifulSoup(html, 'lxml')

                td_name = soup.find("td", text=re.compile('矿山名称'))
                d['矿山名称'] = td_name.find_next_sibling("td", class_='title_back_left').text if td_name else ''
                td_cert = soup.find("td", text=re.compile('采矿许可证号'))
                d['采矿许可证号'] = td_cert.find_next_sibling("td", class_='title_back_left').text if td_cert else ''
                td_ckqr = soup.find("td", text=re.compile('采矿权人'))
                d['采矿权人'] = td_ckqr.find_next_sibling("td", class_='title_back_left').text if td_ckqr else ''
                td_kckz = soup.find("td", text=re.compile('开采矿种'))
                d['开采矿种'] = td_kckz.find_next_sibling("td", class_='title_back_left').text if td_kckz else ''
                td_kcfs = soup.find("td", text=re.compile('开采方式'))
                d['开采方式'] = td_kcfs.find_next_sibling("td", class_='title_back_left').text if td_kcfs else ''
                td_scgm = soup.find("td", text=re.compile('生产规模'))
                scgm = ''
                if td_scgm:
                    scgm = td_scgm.find_next_sibling("td", class_='title_back_left')
                d['生产规模'] = scgm.text if scgm else ''
                td_date = soup.find("td", text=re.compile('有效期限'))
                d['有效期限'] = td_date.find_next_sibling("td", class_='title_back_left').text.replace('\n', '') if td_date else ''
                td_org = soup.find("td", text=re.compile('发证机关'))
                d['发证机关'] = td_org.find_next_sibling("td", class_='title_back_left').text if td_org else ''

                td_sjksgm = soup.find("td", text=re.compile(r'设计矿山\S+?规模'))
                d['设计矿山规模'] = td_sjksgm.find_next_sibling("td", class_='title_back_left').text if td_sjksgm else ''
                td_sjckl = soup.find("td", text=re.compile('实际年采矿石量'))
                d['实际年采矿石量'] = td_sjckl.find_next_sibling("td", class_='title_back_left').text if td_sjckl else ''

                td_ckff = soup.find("td", text=re.compile('采矿方法'))
                d['采矿方法'] = td_ckff.find_next_sibling("td", class_='title_back_left').text if td_ckff else ''
                td_syfw = soup.find("td", text=re.compile('剩余服务年限'))
                d['剩余服务年限'] = td_syfw.find_next_sibling("td", class_='title_back_left').text if td_syfw else ''
                td_sjcknl = soup.find("td", text=re.compile('设计采矿能力'))
                d['设计采矿能力'] = td_sjcknl.find_next_sibling("td", class_='title_back_left').text if td_sjcknl else ''
                td_sjick = soup.find("td", text=re.compile('实际采矿能力'))
                d['实际采矿能力'] = td_sjick.find_next_sibling("td", class_='title_back_left').text if td_sjick else ''
                td_nccl = soup.find("td", text=re.compile('年采出矿量'))
                d['年采出矿量'] = td_nccl.find_next_sibling("td", class_='title_back_left').text if td_nccl else ''

                print(d)
                self.collection.insert_one(d)

    async def run(self, start_page, end_page):
        async with aiohttp.ClientSession() as session:
            for i in range(start_page, end_page+1):
                task = asyncio.create_task(self.get_link(session, i))
                self.tasks.append(task)
            await asyncio.gather(*self.tasks)
            await asyncio.gather(*self.new_tasks)


if __name__ == '__main__':
    # mc = MongoClient()
    mc = MongoClient("mongodb://edmon:redhat@192.168.199.127:27017/admin")
    db = mc['mine']
    # 数据库表名, 每次更改页面链接，表名都要重新命名
    col_name = db['yunnan2018']
    # 需要抓取的页面链接，将链接中pageNo=后面的数字替换为{pageNo}
    base_url = 'http://kyqgs.mnr.gov.cn/search_projects_sx.jspx?pageNo={pageNo}&firstListTotalCount=0&secondListTotalCount=3619&thirdListTotalCount=0&fourthListTotalCount=0&times=sj-2&kuangquans=kq-2&areas=dq-25&pagecount=362&_goPs=1'
    start = time.time()
    # 并发数量, 默认同时有8个线程在工作
    crawler = Crawler(url=base_url, collection=col_name, concurrency=8)
    loop = asyncio.get_event_loop()
    # 开始页码，结束页码
    loop.run_until_complete(crawler.run(start_page=1, end_page=2))

    print('Total number of pages retrieved: ' + str(len(crawler.link_list)))
    print(f'Total time spend: {time.time()-start}s')
