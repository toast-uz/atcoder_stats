import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import datetime
from users_submits import DIRNAME as DIRNAME_SRC

DIRNAME_DST = '../out/atcoder_stats/users_active/'

def create_users_active(filename):
    filename_src = DIRNAME_SRC + filename
    filename_dst = DIRNAME_DST + filename
    if os.path.isfile(filename_dst) and os.stat(filename_src).st_mtime < os.stat(filename_dst).st_mtime:
        return
    print(f'Creating {filename_dst} ...', flush=True, end='')
    df = pd.read_csv(filename_src, index_col=0)
    begin_date = df['epoch_min'].transform(lambda x: datetime.datetime.fromtimestamp(x))
    end_date = df['epoch_max'].transform(lambda x: datetime.datetime.fromtimestamp(x))
    duration = end_date - begin_date
    now = datetime.datetime.now()
    elapsed = now - end_date
    python_rate = df['Python'] / df['submits']
    df = pd.concat([begin_date, duration, elapsed, python_rate], axis=1)
    df.columns=['join', 'duration', 'elapsed', 'python_rate']
    df.to_csv(filename_dst)
    print('done')

def main():
    filename ='all.csv'
    create_users_active(filename)
    df = pd.read_csv(DIRNAME_DST + filename, index_col=0)
    start = datetime.datetime(2020, 1, 1)
    df = df[pd.to_datetime(df['join']) >= start]
    df['join'] = pd.to_datetime(df['join']).dt.to_period('Q')
    df['duration'] = (df['duration'] / datetime.timedelta(days=1)).apply(np.floor).astype(int)
    df['elapsed'] = (df['elapsed'] / datetime.timedelta(days=1)).apply(np.floor).astype(int)
    df['python_rate'] = (df['python_rate'] * 10).apply(np.floor).astype(int).apply(lambda x: f'{x*10:02}%+' if x < 10 else '90%+')
    duration = pd.pivot_table(df, index='python_rate', columns='join', values='duration')
    elapsed = pd.pivot_table(df, index='python_rate', columns='join', values='elapsed')
    print('duration(days)')
    print(duration)
    duration.to_csv(DIRNAME_DST + 'all_python_duration.csv')
    print('elapsed(days)')
    duration.to_csv(DIRNAME_DST + 'all_python_elapsed.csv')
    print(elapsed)

    graph = sns.heatmap(duration)
    graph.set_title('duration (days)')
    plt.show()
    graph = sns.heatmap(elapsed, cmap='Blues')
    graph.set_title('elapsed (days)')
    plt.show()

if __name__=='__main__':
    main()
