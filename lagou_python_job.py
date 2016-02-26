#! /user/bin/env python
# encoding=utf-8

import datetime

import requests
from pymongo import MongoClient

from logger import gen_logger

MAIN_URL = 'http://www.lagou.com/jobs/positionAjax.json'
CONN = MongoClient('127.0.0.1', 27017)

# 输出日志定义
logger = gen_logger()


def get_a_page(url, pn, kd='python', px='default'):
    """
    根据得到工作的json数据
    :param url: 地址
    :param pn: 页码
    :param px: 地区
    :param kd: 查询的关键字
    :return: jobs的json数据
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) ' +
                      'AppleWebKit/537.36 (KHTML, like Gecko) ' +
                      'Chrome/48.0.2564.116 Safari/537.36'
    }
    payload_params = {'px': px}
    payload_data = {'pn': pn, 'kd': kd}
    r = requests.post(url, headers=headers,
                      params=payload_params,
                      data=payload_data)
    if r.status_code == 200:
        logger.info('success download this page.')
        return r.json()
    else:
        logger.error('download error.')
        return r.json()


def save_a_page(page):
    """
    存储一个页面
    :param page:
    :return:
    """
    job_list = page["content"]["result"]
    for job in job_list:
        save_a_job(job)


def save_a_job(job):
    """
    将一条job数据插入到mongo
    :param job:
    :return:
    """
    job['_id'] = job['positionId']
    job['updateTime'] = datetime.datetime.now()

    CONN['lagou']['python_job'].update_one(
        filter={'_id': job['_id']},
        update={'$set': job},
        upsert=True
    )


def main():
    page_no = 1
    while True:
        page = get_a_page(MAIN_URL, page_no)
        save_a_page(page)
        if page['content']:
            page_no = page["content"]["pageNo"] + 1
            logger.info('now download page {}.'.format(page_no))
        else:
            break

if __name__ == '__main__':
    main()