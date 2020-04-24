import glob
import json
from os import environ as E
import MeCab
from tqdm import tqdm
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
import random
import re
from pathlib import Path
from collections import namedtuple
import click
import psutil

Tweet = namedtuple('Tweet', ['tweet', 'username'])

HOME = E.get("HOME")
HERE = Path(__name__).resolve().parent

mecab = MeCab.Tagger("-O wakati -d /usr/lib/x86_64-linux-gnu/mecab/dic/mecab-ipadic-neologd")
assert mecab.parse('新型コロナウイルス').strip().split() == ['新型コロナウイルス'], "辞書が最新ではありません"


def proc(arg):
    keyword, user_dir = arg
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
                try:
                    obj = json.loads(line)
                except:
                    continue
                # print(obj)
                tweets.append(Tweet(obj['tweet'], f"@{obj['username']}"))
        except:
            continue

        require_size = 1
        # idxs = [idx for idx, tweet in enumerate(tweets) if keyword in set(mecab.parse(tweet.tweet.lower()).split())]
        idxs = [idx for idx, tweet in enumerate(tweets) if keyword in tweet.tweet.lower()]

        for idx in idxs:
            batch = tweets[min(idx - 3, 0): idx + 3]
            cnt += 1

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
                batch2 = tweets[min(idx2 - 3, 0): idx2 + 3]
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


@click.command()
@click.option('--count', default=250000, help='Number of sampling users.')
@click.option('--keyword', default='裏垢', help='対象キーワード')
def main(count, keyword):
    print(f'keyword={keyword}, sampling_size={count}で処理を開始します')
    Path(f'{HERE}/var/results').mkdir(exist_ok=True, parents=True)
    term_freq, term_freq2 = {}, {}
    cnt, cnt2 = 0, 0
    args = [(keyword, user_dir) for user_dir in tqdm(glob.glob(f'{HOME}/.mnt/favs03/*')[-count:])]

    with ProcessPoolExecutor(max_workers=psutil.cpu_count()) as exe:
        for _term_freq, _cnt, _term_freq2, _cnt2 in tqdm(exe.map(proc, args), total=len(args)):
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
    terms = list(set(term_freq.keys()) & set(term_freq2.keys()))
    terms = [term for term in terms if re.search('^[a-z]{1,}$', term) is None and re.search('^[0-9]{1,}', term) is None]

    df = pd.DataFrame({'term': terms, 'term_num': [term_freq[term] for term in terms], 'term_num2': [term_freq2[term] for term in terms]})
    # サンプルサイズの正規化
    # fix = lambda x: int(x * (cnt/cnt2))
    df['prob'] = df['term_num'] / cnt
    df['prob2'] = df['term_num2'] / cnt2
    df['total'] = df['term_num'] + df['term_num2']
    df['rel'] = df['prob'] / (df['prob'] + df['prob2'])
    df.sort_values(by=['rel'], ascending=False, inplace=True)
    df.to_csv(f'{HERE}/var/results/{keyword}_{count}.csv', index=None)


if __name__ == "__main__":
    main()
