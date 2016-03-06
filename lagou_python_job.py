#! /user/bin/env python
# encoding=utf-8

import datetime

import requests
from gevent import monkey; monkey.patch_all()
from pymongo import MongoClient
import gevent
from gevent.pool import Group
from gevent.pool import Pool
from logger import gen_logger

COMPANY_URL = "http://www.lagou.com/gongsi/0-0-0.json"
JOB_URL = "http://www.lagou.com/gongsi/searchPosition.json"
CONN = MongoClient('127.0.0.1', 27017)

# 输出日志定义
logger = gen_logger()

# 全局变量 公司ID列表
company_ids = []


def get_company_page(pn):
    """
    :param pn:
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) ' +
                      'AppleWebKit/537.36 (KHTML, like Gecko) ' +
                      'Chrome/48.0.2564.116 Safari/537.36'
    }
    payload_data = {'pn': pn}
    r = requests.post(COMPANY_URL, headers=headers,
                      data=payload_data).json()
    if r.has_key('result'):
        save_companies(r) # 存储公司信息
        logger.info('success download %s page.' % r['pageNo'])
    else:
        logger.error('download error or this page no data.')


def save_companies(company):
    """
    存储一个页面
    :param company:
    :return:
    """
    company_list = company["result"]
    for company in company_list:
        write_company_to_db(company)
        global company_ids
        logger.info('save company %s success.' % company['companyId'])
        company_ids.append(company['companyId'])


def write_company_to_db(company):
    """
    :param company:
    """
    company['_id'] = company['companyId']
    company['updateTime'] = datetime.datetime.now()

    CONN['lagou']['lagou_company'].update_one(
        filter={'_id': company['_id']},
        update={'$set': company},
        upsert=True
    )


def get_company_jobs(company):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) ' +
                      'AppleWebKit/537.36 (KHTML, like Gecko) ' +
                      'Chrome/48.0.2564.116 Safari/537.36'
    }
    for i in range(1, 20):
        payload_data = {'pageNo': i, 'companyId': company}
        r = requests.post(JOB_URL, headers=headers,
                          data=payload_data).json()
        if r['content']['data']['page']['result']:
            save_jobs(r)
            logger.info('success download.')
        else:
            logger.error('download error or this page no data.')
            break


def save_jobs(jobs):
    """
    :param jobs:
    :return:
    """
    job_list = jobs['content']['data']['page']['result']
    for job in job_list:
        write_job_to_db(job)
        logger.info('save job %s success.' % job['positionId'])


def write_job_to_db(job):
    """
    :param job:
    """
    job['_id'] = job['positionId']
    job['updateTime'] = datetime.datetime.now()

    CONN['lagou']['lagou_job'].update_one(
        filter={'_id': job['_id']},
        update={'$set': job},
        upsert=True
    )


def get_all_company():
    company_group = Group()
    for page in range(1, 500):
        company_group.add(gevent.spawn(get_company_page, page))
    company_group.join()


def get_all_jobs():
    job_pool = Pool(1000)
    job_pool.map(get_company_jobs, company_ids)


if __name__ == '__main__':
    get_all_company()
    print company_ids
    get_all_jobs()