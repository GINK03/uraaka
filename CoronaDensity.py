import glob
import pickle
from os import environ as E
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor
import pandas as pd
from pathlib import Path

HOME = E.get('HOME')

targets = ['コロナ', 'マスク', 'トイレットペーパー', '買い占め']
def proc(fn):
    ret = {}
    date, tweet_size, tf = pickle.load(open(fn, 'rb'))
    ret = {'date':date}
    for target in targets:
        ret[target] = tf[target]/tweet_size if tf.get(target) else 0


    return ret
    
fns = [fn for fn in tqdm(sorted(glob.glob(f'{HOME}/nvme1n1/Wakati/*'))) if Path(fn).name >= '2020-01-01']

rets = []
with ProcessPoolExecutor(max_workers=24) as exe:
    for ret in tqdm(exe.map(proc, fns), total=len(fns), desc="make json"):
        rets.append(ret)

df = pd.DataFrame(rets)
df.sort_values(by=['date'], inplace=True)
df.to_csv('local.csv', index=None)
