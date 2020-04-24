import glob
import json
from os import environ as E
import MeCab
from tqdm import tqdm
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
import random
import re

from collections import namedtuple

Tweet = namedtuple('Tweet', ['tweet', 'username'])

HOME = E.get("HOME")

mecab = MeCab.Tagger("-O wakati -d /usr/lib/x86_64-linux-gnu/mecab/dic/mecab-ipadic-neologd")

def proc(user_dir):
    term_freq = {}
    term_freq2 = {}
    cnt = 0 
    cnt2 = 0
    for json_fn in glob.glob(f'{user_dir}/*'):
        # print(json_fn)
        try:
            tweets = []
            for line in open(json_fn):
                line = line.strip()
                obj = json.loads(line)
                # print(obj)
                tweets.append( Tweet(obj['tweet'], f"@{obj['username']}") )
        except:
            continue
       
        keywords = {'れいわ新選組'}
        require_size = 1
        # idxs = [idx for idx, tweet in enumerate(tweets) if len(keywords & set(mecab.parse(tweet.tweet.lower()).split())) == require_size]
        idxs = [idx for idx, tweet in enumerate(tweets) if '裏垢女子' in tweet.tweet.lower()]

        for idx in idxs:
            batch  = tweets[min(idx-3,0):idx+3]
            # print(batch)
            cnt += 1

            # term_freq[f'{}']
            for term in set(mecab.parse(' '.join([b.tweet for b in batch]).lower()).strip().split()):
                if term not in term_freq:
                    term_freq[term] = 0
                term_freq[term] += 1
            
            for tweet in [b.tweet for b in batch]:
                img = re.search(r'(pic.twitter.com/[a-z][A-Z]{1,})$', tweet)
                if img is None:
                    continue
                img = img.group(1)
                # print(tweet)
                # print(img)
                if img not in term_freq:
                    term_freq[img] = 0
                term_freq[img] += 1

            for username in [b.username for b in batch]:
                if username not in term_freq:
                    term_freq[username] = 0
                term_freq[username] += 1

        if len(tweets) != 0:
            for N in range(5):  
                idx2 = random.choice(list(range(len(tweets))))
                batch2 = tweets[min(idx2-3,0):idx2+3]
                cnt2 += 1
                for term in set(mecab.parse(' '.join([b.tweet for b in batch2]).lower()).strip().split()):
                    if term not in term_freq2:
                        term_freq2[term] = 0
                    term_freq2[term] += 1

                for tweet in [b.tweet for b in batch2]:
                    img = re.search(r'(pic.twitter.com/[a-z][A-Z]{1,})$', tweet)
                    if img is None:
                        continue
                    img = img.group(1)
                    if img not in term_freq2:
                        term_freq2[img] = 0
                    term_freq2[img] += 1

                for username in [b.username for b in batch2]:
                    if username not in term_freq2:
                        term_freq2[username] = 0
                    term_freq2[username] += 1
    return term_freq, cnt, term_freq2, cnt2


term_freq, term_freq2 = {}, {}
cnt, cnt2 = 0, 0
user_dirs = [user_dir for user_dir in tqdm(glob.glob(f'{HOME}/.mnt/favs03/*')[-250000:])]
# _term_freq, _cnt = proc(user_dir)
# [proc(user_dir) for user_dir in tqdm(user_dirs)]
with ProcessPoolExecutor(max_workers=24) as exe:
    for _term_freq, _cnt, _term_freq2, _cnt2 in tqdm(exe.map(proc, user_dirs), total=len(user_dirs)):
        cnt += _cnt
        for term, freq in _term_freq.items():
            if term not in term_freq:
                term_freq[term] = 0
            term_freq[term] += freq
        
        cnt2 += _cnt2
        for term, freq in _term_freq2.items():
            if term not in term_freq2:
                term_freq2[term] = 0
            term_freq2[term] += freq

# サンプルサイズが減りすぎてしまうので、ターゲットに観測されたtermはベースラインで1とする
delta_terms = set(term_freq) - set(term_freq2)
for delta_term in delta_terms:
    term_freq[delta_term] = 1
terms = list( set(term_freq.keys()) & set(term_freq2.keys()) )
terms = [term for term in terms if re.search('^[a-z]{1,}$', term) is None and re.search('^[0-9]{1,}', term) is None]

df = pd.DataFrame({'term':terms, 'rel':[term_freq[term] for term in terms ], 'rel2':[term_freq2[term] for term in terms]})
df['total'] = df['rel'] + df['rel2']
# backup rel, rel2
df['cnt'] = df['rel']
df['cnt2'] = df['rel2']
# overwrite
df['rel'] /= cnt
df['rel2'] /= cnt2
df['rel'] /= df['rel'] + df['rel2']
df.sort_values(by=['rel'], ascending=False, inplace=True)
df.to_csv('csvs/result.csv', index=None)

