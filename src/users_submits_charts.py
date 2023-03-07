import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import datetime
import users_submits

DIRNAME = '../out/atcoder_stats/users_submits_charts/'

def create_users_submits(key):
    filename = key + '.csv'
    filename_src = users_submits.DIRNAME + filename
    filename_dst = DIRNAME + filename
    if os.path.isfile(filename_dst) and os.stat(filename_src).st_mtime < os.stat(filename_dst).st_mtime:
        print(f'Already updated {filename_dst}, quit creating.')
        return
    print(f'Creating {filename_dst} ...', flush=True, end='')
    df = pd.read_csv(filename_src, index_col=0)

    begin_date = df['epoch_min'].transform(lambda x: datetime.datetime.fromtimestamp(x))
    end_date = df['epoch_max'].transform(lambda x: datetime.datetime.fromtimestamp(x))
    duration = end_date - begin_date
    now = datetime.datetime.now()
    elapsed = now - end_date

    df_lang = users_submits.languages(df)
    main_lang = df_lang.idxmax(axis=1)
    # PythonとPyPyを集約
    df['Pythons'] = df['Python'] + df['PyPy']
    df = df.drop(['Python', 'PyPy'], axis=1)
    df_lang = users_submits.languages(df)
    main_lang_pythons = df_lang.idxmax(axis=1)

    df = pd.concat([begin_date, duration, elapsed, main_lang, main_lang_pythons], axis=1)
    df.columns=['join_date', 'duration', 'elapsed', 'main_lang', 'main_lang_pythons']
    df.to_csv(filename_dst)
    print('done')

def read_duration(key):   # Pythonsは結合
    df = pd.read_csv(DIRNAME + key + '.csv', index_col=0)
    languages_rank = df.groupby(['main_lang_pythons']).size().sort_values(ascending=False)
    start = datetime.datetime(2019, 1, 1)
    df = df[pd.to_datetime(df['join_date']) >= start]
    df['join_date'] = pd.to_datetime(df['join_date']).dt.to_period('Q')
    df['duration'] = (df['duration'] / datetime.timedelta(days=1)).apply(np.floor).astype(int)
    df['elapsed'] = (df['elapsed'] / datetime.timedelta(days=1)).apply(np.floor).astype(int)
    df.loc[~df['main_lang_pythons'].isin(languages_rank[:15].index.to_list()), 'main_lang_pythons'] = 'misc'
    return df

def read_lang_selection(a_head=10, h_head=10, lang=None):
    df1 = pd.read_csv(DIRNAME + 'algorithm.csv', index_col=0).rename(columns={'main_lang': 'a_lang'})
    df2 = pd.read_csv(DIRNAME + 'heuristic.csv', index_col=0).rename(columns={'main_lang': 'h_lang'})
    if lang is None:
        languages_rank1 = df1.groupby(['a_lang']).size().sort_values(ascending=False)
        languages_rank2 = df2.groupby(['h_lang']).size().sort_values(ascending=False)
    elif lang == 'heuristic':
        languages_rank1 = df2.groupby(['h_lang']).size().sort_values(ascending=False)
        languages_rank2 = languages_rank1.copy()
    df1.loc[~df1['a_lang'].isin(languages_rank1[:a_head].index.to_list()), 'a_lang'] = 'misc'
    df2.loc[~df2['h_lang'].isin(languages_rank2[:h_head].index.to_list()), 'h_lang'] = 'misc'
    df = pd.concat([df1['a_lang'], df2['h_lang']], axis=1)
    return df

def create_pivot(df, key, index, columns, values, aggfunc=np.mean, set_=None, cmap=None, normalize=None):
    filename = DIRNAME + f'{key}_pivot_{index}-{columns}-{values}.csv'
    print(f'Creating {filename} ...', flush=True, end='')
    pivot = pd.pivot_table(df, index=index, columns=columns, values=values, aggfunc=aggfunc)
    pivot.to_csv(filename)
    print('done')

    print(pivot)
    if normalize is not None:
        for row in pivot.index:
            pivot.loc[row] = pivot.loc[row] / pivot.loc[row].sum()
        print(pivot)
    graph = sns.heatmap(pivot, cmap=cmap)
    if set_ is not None: graph.set(**set_)
    plt.show()

def main():
    # Heuristic lang ratio for each Algorithm lang (only dual players)
    for key in ['algorithm', 'heuristic']:
        create_users_submits(key)
    df = read_lang_selection(lang='heuristic')
    df['players'] = 1
    create_pivot(df, 'selection', index='a_lang', columns='h_lang', values='players',
                 set_={'title': 'Heuristic lang ratio for each Algorithm lang (only dual players)'},
                 aggfunc=np.sum, cmap='Greens', normalize=True)
    # Average duration (last - first submit: days) of all players for each lang
    for key in ['all']:
        create_users_submits(key)
        df = read_duration(key)
        create_pivot(df, key, index='main_lang_pythons', columns='join_date', values='duration',
                     set_={'title': 'Average duration (last - first submit: days) of all players',
                           'xlabel': 'join date (calender year)',
                           'ylabel': 'main (argmax) language'})




    # prospects : 参加期間90日未満のユーザ
    # customers : 参加期間90日以上のユーザ
    #   new : 利用開始した月
    #   churn : 最終参加から90日以上経過した最終利用翌月
    #   renewal : newとchurnの間の期間（churnしていない場合は現在まで）
    # conversion% : 毎月の customers::new / (prospects::new + customers::new)
    # churn%: 毎月の customers::churn / (customers::churn + customers::renewal)

if __name__=='__main__':
    main()
