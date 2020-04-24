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

def proc(fn: str) -> None:
    mecab = MeCab.Tagger("-O wakati -d /usr/lib/x86_64-linux-gnu/mecab/dic/mecab-ipadic-neologd")
    assert mecab.parse("新型コロナウイルス").split() == ["新型コロナウイルス"], "辞書が古いです"

    db = dbm.open(fn, 'r')
    keys = list(db.keys())
    doc_size = len(keys)
    date = Path(fn).name
    term_freq = {}
    for key in tqdm(keys, desc=date):
        record: Record = pickle.loads(gzip.decompress(db.get(key)))
        terms = mecab.parse(record.tweet.lower()).split()
        # print(terms)
        for term in terms:
            if term not in term_freq:
                term_freq[term] = 0
            term_freq[term] += 1
    with open(f'{HOME}/nvme1n1/Wakati/{date}', 'wb') as fp:
        pickle.dump( (date, doc_size, term_freq), fp )

dirs = glob.glob(f'{HOME}/nvme1n1/ToDBM/20*')
# [proc(dir) for dir in dirs]


random.shuffle(dirs)
with ProcessPoolExecutor(max_workers=psutil.cpu_count()) as exe:
    for idx, ret in tqdm(enumerate(exe.map(proc, dirs)), total=len(dirs), desc="Parse..."):
        ret

