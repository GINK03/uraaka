import glob
import pickle
from os import environ as E
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor
import pandas as pd
from pathlib import Path
import dbm
import gzip

HOME = E.get('HOME')

targets = ['コロナ', '安倍', 'お金', '不安', '安心', '破産', '家賃', '資金繰', '税金', 'マスク', 'トイレットペーパ', '買占め']
def proc(fn):
    ret = {}

    db = dbm.open(fn, 'r')
    keys = list(db.keys())
    tweet_size = len(keys)
    ret = {'date':Path(fn).name, 'tweet_size':tweet_size}
    for target in targets:
        ret[target] = 0
    
    for key in tqdm(keys, desc=Path(fn).name):
       record = pickle.loads(gzip.decompress(db[key]))
       for target in targets:
           if target in record.tweet:
                ret[target] += 1                

    return ret
    
fns = [fn for fn in tqdm(sorted(glob.glob(f'{HOME}/nvme1n1/ToDBM/*'))) if Path(fn).name >= '2020-01-01']

rets = []
# [proc(fn) for fn in fns]
with ProcessPoolExecutor(max_workers=24) as exe:
    for ret in tqdm(exe.map(proc, fns), total=len(fns), desc="make json"):
        rets.append(ret)

df = pd.DataFrame(rets)
df.sort_values(by=['date'], inplace=True)
for target in targets:
    df[target] /= df.tweet_size
    df[target] = df[target].rolling(3).mean().reset_index()[target]
df.to_csv('local.csv', index=None)
