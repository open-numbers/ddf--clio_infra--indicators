# -*- coding: utf-8 -*-

"""scraper"""

import requests
from lxml import etree
import os.path as osp
from urllib.parse import urljoin


url = 'https://www.clio-infra.eu/index.html'
out_path = '../source/'


def get_home_page(url):
    response = requests.get(url)
    content = response.content
    tree = etree.fromstring(content, parser=etree.HTMLParser())

    return tree


def get_indicator_links(tree):
    elem = tree.xpath('//div[@class="col-sm-4"]/div[@class="list-group"]/p[@class="list-group-item"]')

    res1 = {}
    res2 = {}

    for e in elem:
        try:
            name = e.find('a').text
            link = e.find('*/a').attrib['href']
            if '../data' in link:  # it's indicator file
                res1[name] = link
            else:  # it's country file
                res2[name] = link
        except:  # FIXME: add exception class here.
            name = e.text
            res2[name] = ''
    return (res1, res2)


def run_scraper():
    tree = get_home_page(url)
    ind, cc = get_indicator_links(tree)

    assert len(ind) == 77, 'indicator number might changed, please review the website and script.'

    for name, link in ind.items():
        res = requests.get(urljoin(url, link), stream=True)
        fn = osp.join(out_path, f'{name}.xls')
        print(fn, end=', ')
        with open(fn, 'wb') as f:
            for chunk in res.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    f.flush()
            f.close()
        print('Done downloading source files.')


if __name__ == '__main__':
    run_scraper()
