import pandas as pd
from users_submits import DIRNAME, ID_COLUMN_LANG_BEGIN

abrgc = pd.read_csv(DIRNAME + 'abrgc.csv', index_col=0)
abrgc = abrgc.drop(abrgc.columns[[n for n in range(ID_COLUMN_LANG_BEGIN)]], axis=1)
ahc = pd.read_csv(DIRNAME + 'ahc.csv', index_col=0)
ahc = ahc.drop(ahc.columns[[n for n in range(ID_COLUMN_LANG_BEGIN)]], axis=1)

print('abc/arc/agc_lang')
print(abrgc.sum(axis=0).sort_values(ascending=False)[:20])
print('ahc_lang')
print(ahc.sum(axis=0).sort_values(ascending=False)[:20])
