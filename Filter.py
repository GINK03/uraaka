import pandas as pd
import glob
from pathlib import Path

HERE = Path(__file__).resolve().parent
files = glob.glob(f'{HERE}/var/results/*.csv')
Path(f'{HERE}/var/filter').mkdir(exist_ok=True, parents=True)

for file in files:
    name = Path(file).name
    df = pd.read_csv(file)
    ## user
    df1 = df[df['term'].apply(lambda x:'@' in str(x))]
    t1 = sorted(df1[df1.rel >= 0.5].total.tolist())
    th = t1[int(len(t1) * 9/10)]
    print(th)
    df1 = df1[df1['total'] >= th]
    df1.to_csv(f'{HERE}/var/filter/users_{name}', index=None)

    ## term
    df2 = df[df['term'].apply(lambda x:'@' not in str(x) and ".jpg" not in str(x))]
    t2 = sorted(df2[df2.rel >= 0.5].total.tolist())
    th = t2[int(len(t2) * 9/10)]
    print(th)
    df2 = df2[df2['total'] >= th]
    df2.to_csv(f'{HERE}/var/filter/terms_{name}', index=None)

    ## pics
    df3 = df[df['term'].apply(lambda x:".jpg" in str(x) or ".png" in str(x))]
    t3 = sorted(df3[df3.rel >= 0.5].total.tolist())
    th = t3[int(len(t3) * 5/10)]
    print(th)
    df3 = df3[df3['total'] >= th]
    df3.to_csv(f'{HERE}/var/filter/pics_{name}', index=None)
