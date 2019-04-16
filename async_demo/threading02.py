import threading
import queue
import time
import requests
from bs4 import BeautifulSoup


def job(d):
    yn2018_362 = 'http://kyqgs.mnr.gov.cn/search_projects_sx.jspx?pageNo=' + str(d) + \
                 '&firstListTotalCount=0&secondListTotalCount=3619&thirdListTotalCount=0' + \
                 '&fourthListTotalCount=0&times=sj-2&kuangquans=kq-2&areas=dq-25&pagecount=362&_goPs=1'
    r = requests.get(yn2018_362, headers=header)
    links = []
    if r.status_code == 200:
        soup = BeautifulSoup(r.text, 'lxml')
        a = soup.find('table', class_='project_table').find_all('a')
        for link in a:
            links.append({'_id': host + link.get('href')})
    print(links)
    link_list.extend(links)


class MyThread(threading.Thread):

    def __init__(self, q):
        threading.Thread.__init__(self)
        self.q = q

    def run(self):
        while True:
            if not self.q.empty():
                job(self.q.get())
                self.q.task_done()


if __name__ == '__main__':
    host = 'http://kyqgs.mnr.gov.cn'
    header = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0'}
    link_list = []
    q = queue.Queue()
    start = time.time()
    for i in range(8):
        t = MyThread(q)
        t.setDaemon(True)
        t.start()

    for j in range(2, 17):
        q.put(j)

    q.join()
    print('How many links in the list: ' + str(len(link_list)))
    print(time.time() - start)