import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
import glob
from hashlib import sha256
from os import environ as E
from typing import Tuple
from typing import Union
from typing import List
import json
import pickle
import gzip
from tqdm import tqdm
from pathlib import Path
import psutil
import dataclasses
import gzip
import random
import shutil
from Structure import *
import shelve
import dbm
from functools import lru_cache
import MeCab

HOME = E.get("HOME")

HERE = Path(__file__).resolve().parent

# @lru_cache(maxsize=10*5)


def proc(dir: str) -> None:
    mecab = MeCab.Tagger("-O wakati -d /usr/lib/x86_64-linux-gnu/mecab/dic/mecab-ipadic-neologd")
    assert mecab.parse("新型コロナウイルス").split() == ["新型コロナウイルス"], "辞書が古いです"
    if 'users' in dir:
        return
    if 'var' in dir:
        return
    doc_size = len(glob.glob(f'{dir}/*'))
    date = Path(dir).name
    term_freq = {}
    for f in tqdm(glob.glob(f'{dir}/*'), desc=date):
        record: Record = pickle.loads(gzip.decompress(open(f, 'rb').read()))
        terms = mecab.parse(record.tweet.lower()).split()
        for term in terms:
            if term not in term_freq:
                term_freq[term] = 0
            term_freq[term] += 1 / doc_size
    
    with open(f'/nvme-pool/var/Parse/{date}.pkl.gz', 'wb') as fp:
        fp.write( gzip.compress( pickle.dumps( term_freq) ) )

dirs = glob.glob(f'/nvme-pool/*')
random.shuffle(dirs)
# [proc(dir) for dir in dirs]

with ProcessPoolExecutor(max_workers=psutil.cpu_count()) as exe:
    for idx, rets in tqdm(enumerate(exe.map(proc, dirs)), total=len(dirs), desc="Parse..."):
        if idx % 1000 == 0:
            print(f'now {idx:09d}')
