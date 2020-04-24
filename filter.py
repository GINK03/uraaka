import pandas as pd

df = pd.read_csv('csvs/result.csv')

## user
df1 = df[df['term'].apply(lambda x:'@' in str(x))]
t1 = sorted(df1[df1.rel >= 0.5].total.tolist())
th = t1[int(len(t1) * 9/10)]
print(th)
df1 = df1[df1['total'] >= th]

df1.to_csv('csvs/users.csv', index=None)

## term
df2 = df[df['term'].apply(lambda x:'@' not in str(x) and "pic." not in str(x))]
t2 = sorted(df2[df2.rel >= 0.5].total.tolist())
th = t2[int(len(t2) * 9/10)]
print(th)
df2 = df2[df2['total'] >= th]
df2.to_csv('csvs/terms.csv', index=None)

## pics
df3 = df[df['term'].apply(lambda x:"pic." in str(x))]
t3 = sorted(df3[df3.rel >= 0.5].total.tolist())
th = t3[int(len(t3) * 5/10)]
print(th)
df3 = df3[df3['total'] >= th]
df3.to_csv('csvs/pics.csv', index=None)
