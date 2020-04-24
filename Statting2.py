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
from typing import Set
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

HOME = E.get("HOME")

HERE = Path(__file__).resolve().parent

OUT_DIR = f'{HOME}/nvme1n1'
@lru_cache(maxsize=10*5)
def check_day_dir_is_exists(date: str) -> bool:
    if not Path(f'{OUT_DIR}/{date}').exists():
        Path(f'{OUT_DIR}/{date}').mkdir(exist_ok=True, parents=True)
    return True


@lru_cache(maxsize=10*6)
def check_tweet_is_exists(date: str, id: str, tweet: str, likes_count: int) -> bool:
    if not Path(f'{OUT_DIR}/{date}/{id}').exists():
        record = Record(id, tweet, likes_count, date)
        ser = gzip.compress(pickle.dumps(record))
        with open(f'{OUT_DIR}/{date}/{id}', 'wb') as fp:
            fp.write(ser)


def proc(dir: str) -> None:
    try:
        user = Path(dir).name
        if Path(f'{OUT_DIR}/users/{user}').exists():
            return set()
        rets = set()
        for file in glob.glob(f'{dir}/*'):
            with open(file) as fp:
                for line in fp:
                    try:
                        line = line.strip()
                        obj = json.loads(line)
                        id = obj["id"]
                        likes_count = obj["likes_count"]
                        date = obj['date']
                        tweet = obj['tweet']
                        record = Record(id, tweet, likes_count, date)
                        ret = Ret(date=date, id=id, ser=gzip.compress(pickle.dumps(record)))
                        rets.add(ret)
                    except:
                        continue
        return rets
    except Exception as exc:
        return set()


dirs = glob.glob(f'{HOME}/.mnt/favs*/*')
"""
make chunks
"""
size = 30000
for i in range(0, len(dirs), size):
    chunk = dirs[i:i+size]
    if Path(f'{HOME}/nvme1n1/Statting2/{i:012d}.pkl', 'wb').exists():
        continue

    rets = set()
    with ProcessPoolExecutor(max_workers=psutil.cpu_count()) as exe:
        for idx, _rets in tqdm(enumerate(exe.map(proc, chunk)), total=len(chunk), desc="agging..."):
            rets |= _rets

    with open(f'{HOME}/nvme1n1/Statting2/{i:012d}.pkl', 'wb') as fp:
        fp.write(pickle.dumps(rets))
