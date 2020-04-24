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

HOME = E.get("HOME")

HERE = Path(__file__).resolve().parent

@lru_cache(maxsize=10*5)
def check_day_dir_is_exists(date: str) -> bool:
    if not Path(f'/nvme-pool/{date}').exists():
        Path(f'/nvme-pool/{date}').mkdir(exist_ok=True, parents=True)
    return True

@lru_cache(maxsize=10*6)
def check_tweet_is_exists(date: str, id: str, tweet: str, likes_count: int) -> bool:
    if not Path(f'/nvme-pool/{date}/{id}').exists():
        record = Record(id, tweet, likes_count, date)
        ser = gzip.compress(pickle.dumps(record))
        with open(f'/nvme-pool/{date}/{id}', 'wb') as fp:
            fp.write(ser)
    

def proc(dir: str) -> None:
    try:
        user = Path(dir).name
        if Path(f'/nvme-pool/users/{user}').exists():
            return None
        rets = []
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
                        rets.append(record)
                    except:
                        continue

        for record in rets:
            check_day_dir_is_exists(date=record.date)
            check_tweet_is_exists(date=record.date, id=record.id, tweet=record.tweet, likes_count=record.likes_count)

        Path(f'/nvme-pool/users/{user}').touch()
        return None
    except Exception as exc:
        print(exc)
        # shutil.rmtree(dir)
        return None


dirs = glob.glob(f'{HOME}/.mnt/fav*/*')
random.shuffle(dirs)

with ProcessPoolExecutor(max_workers=psutil.cpu_count()) as exe:
    for idx, rets in tqdm(enumerate(exe.map(proc, dirs)), total=len(dirs), desc="agging..."):
        if idx % 1000 == 0:
            print(f'now {idx:09d}')

