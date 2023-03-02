import pandas as pd
from lib import Submits, Transforms

DIRNAME = '../out/atcoder_stats/users_submits/'
ID_COLUMN_LANG_BEGIN = 3

def create_users_submits(submits, filename, *, transform=lambda x: x, ):
    print(f'Creating users_submits/{filename} ...', flush=True, end='')
    df = submits.filter('contest_id', lambda x: transform(x) != '').df
    lang = pd.crosstab(df['user_id'], df['language'])
    submits =  pd.DataFrame(lang.sum(axis=1), columns=['submits'])
    epoch_grouped = df[['user_id', 'epoch_second']].groupby('user_id')
    epoch_min = epoch_grouped.min()
    epoch_min.columns=['epoch_min']
    epoch_max = epoch_grouped.max()
    epoch_max.columns=['epoch_max']
    res = pd.concat([epoch_min, epoch_max, submits, lang], axis=1)
    res.to_csv(DIRNAME + filename)
    print('done')

def main():
    # ユーザでグルーピングしてepoch_timeの最大最小、languageのカウントをとる
    transform_language = Transforms().language
    submits = Submits().transform('language', transform_language)
    create_users_submits(submits, 'all.csv')
    create_users_submits(submits, 'axc.csv', transform=Transforms().axc)
    create_users_submits(submits, 'abrgc.csv', transform=Transforms().abrgc)
    create_users_submits(submits, 'ahc.csv', transform=Transforms().ahc)

if __name__=='__main__':
    main()


# 2021年度に活動開始したユーザについて、
# 参加期間（列）とPython率（行）とのクロス集計（行方向に正規化）をしてヒートマップを作成する

# 2021年度に活動開始したユーザについて、
# 参加期間（列）とC++率（行）とのクロス集計（行方向に正規化）をしてヒートマップを作成する

# 2020年度に活動開始したユーザについて、
# 参加期間（列）とPython率（行）とのクロス集計（行方向に正規化）をしてヒートマップを作成する

# 2020年度に活動開始したユーザについて、
# 参加期間（列）とPython率（行）とのクロス集計（行方向に正規化）をしてヒートマップを作成する
