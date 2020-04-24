from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor
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
import dbm
from pathlib import Path
import psutil
import dataclasses
import gzip
import random
from collections import namedtuple
@dataclasses.dataclass
class Record:
    id: int
    tweet: str
    likes_count: int
    date: str


HOME = E.get("HOME")
HERE = Path(__file__).resolve().parent
OUT_DIR = f'{HOME}/nvme1n1'

Record2 = namedtuple("Record2", ['id', 'tweet', 'likes_count', 'date'])
Ret = namedtuple('Ret', ['date', 'id', 'ser'])

IdSer = namedtuple("IdSer", ["id", "ser"])

ids = set()


def _load_ids_from_dbm(fn):
    ids = set()
    with dbm.open(fn, 'r') as db:
        for id in db.keys():
            id = int(id.decode('utf8'))
            ids.add(id)
    return ids


def load_ids_from_dbm() -> Set[int]:
    ids = set()
    fns = [fn for fn in tqdm(glob.glob(f'{OUT_DIR}/ToDBM/*'), desc="load_ids_from_dbm...")]
    with ProcessPoolExecutor(max_workers=3) as exe:
        for _ids in tqdm(exe.map(_load_ids_from_dbm, fns), total=len(fns), desc="load_ids_from_dbm"):
            ids |= _ids
    return ids

def init():
    global ids
    ids = load_ids_from_dbm()
    print('total tweet num', len(ids))


def check_already_id(id: str) -> bool:
    global ids
    if int(id) in ids:
        return True
    else:
        ids.add(int(id))
        return False

        # print(id, fn)
if __name__ == "__main__":
    ids = load_ids_from_dbm()
    print('total tweet num', len(ids))


def save_dbm(arg: Tuple[str, List[IdSer]]) -> None:
    try:
        date = arg[0]
        id_sers = arg[1]
        with dbm.open(f'{OUT_DIR}/ToDBM/{date}', 'c') as db:
            # db.reorganize()
            for id, ser in tqdm(id_sers, desc=f"microflashing {date}..."):
                if db.get(id) is None:
                    db[id] = ser
            db.sync()
    except Exception as exc:
        print(exc)


# date単位でgrouping
def grouping(rets: Set[Ret]):
    date_args = {}
    for date, id, ser in tqdm(rets, desc="inverting..."):
        if check_already_id(id):
            continue
        if date not in date_args:
            date_args[date] = []
        date_args[date].append(IdSer(str(id), ser))
    args = [(date, id_sers) for date, id_sers in date_args.items()]
    [save_dbm(arg) for arg in tqdm(args, desc="single flashing...")]
    # with ProcessPoolExecutor(max_workers=300) as exe:
    #    for ret in tqdm(exe.map(save_dbm, args), total=len(args), desc="flashing..."):                                                                                                  ret
