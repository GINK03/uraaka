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
import MeCab
import random

HOME = E.get("HOME")

HERE = Path(__file__).resolve().parent

# @lru_cache(maxsize=10*5)


def proc(pkl: str) -> None:
    mecab = MeCab.Tagger("-O wakati -d /usr/lib/x86_64-linux-gnu/mecab/dic/mecab-ipadic-neologd")
    assert mecab.parse("新型コロナウイルス").split() == ["新型コロナウイルス"], "辞書が古いです"

    print('start load', pkl)
    try:
        rets: Set[Ret] = pickle.load(open(pkl, 'rb'))
    except Exception as exc:
        return
    print('finish load', pkl)
    grouping(rets) 
    Path(pkl).unlink()
    Path(pkl).touch()

pkls = glob.glob(f'{HOME}/nvme1n1/Statting2/*.pkl')
random.shuffle(pkls)
[proc(pkl) for pkl in pkls]
