import pandas as pd
import re
from mechanize import Browser
from joblib import Parallel, delayed
import multiprocessing
import os
import time
import cPickle
import csv
from bs4 import BeautifulSoup


def relabel(x):
    return 1 if x > 1400 else 0
def parse(url):
    br = Browser()
    try:
        resp = br.open(url).read()
        return resp
    except:
        print('review page does not exist')
        return None
def download():
    df = pd.read_csv('OnlineNewsPopularity.csv')
    df['label'] = df[' shares']
    df['label'] = df['label'].apply(relabel)
    rst = pd.DataFrame({c:[] for c in df.columns})
    ps = []
    i = 0
    for idx, row in df.iterrows():
        txt = parse(row['url'])
        if txt is not None:
            rst = rst.append(row)
            ps.append(txt)
            print('rst: {}, ps: {}'.format(rst.shape[0], len(ps)))
            i+=1
            if i%10 == 0:
                rst['page'] = ps
                back = rst.drop(' shares', axis=1)
                back = back.drop('url', axis=1)
                back.to_csv('all.csv', index=False)
def process(chunk):
    df = pd.DataFrame({c:[] for c in chunk.columns})
    ts, arts, ls = [], [], []
    for idx, row in chunk.iterrows():
        txt = BeautifulSoup(row['page'], 'html.parser')
        title = txt.select('title')
        if title == []:
            continue
        title = title[0].get_text()
        content = txt.select('section.article-content')
        if content == []:
            continue
        content = str(content[0])
        ts.append(title)
        arts.append(content)
        ls.append(row['label'])
        df = df.append(row)
    return ts, arts, ls, df.drop('page', axis=1)
def selectTag():
    title, article, label = [], [], []
    chunksize = 10 ** 3
    other = None
    for chunk in pd.read_csv('all.csv', chunksize=chunksize, encoding='utf-8'):
        ts, arts, ls, tdf = process(chunk)
        title.extend(ts)
        article.extend(arts)
        label.extend(ls)
        # backup
        other = tdf if other is None else pd.concat([other, tdf])
        df = other
        print('len:{}, df:{}'.format(len(title), df.shape[0]))
        df['title'] = title
        df['article'] = article
        df['label'] = label
        df.to_csv('all_processed.csv', encoding='utf-8', index=False)
def otherFeature():
    df = pd.read_csv('OnlineNewsPopularity.csv')
    df = df.drop('url', axis=1)
    df['label'] = df[' shares']
    df['label'] = df['label'].apply(relabel)
    df = df.drop(' shares', axis=1)
    df.to_csv('noText.csv', index=False)
#download()
selectTag()