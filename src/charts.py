# 各種チャート
# あらかじめコマンドラインで python users.py を実行しておくこと
# KeyError: コンテスト名リストが出たら in/contsts.json を削除して再実行すること
#
# Pythonコマンドメニューにおいて
#  from charts import *
#  関数名
# で実行できる

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from users import *

# レート、国別精進状況
# 2023/4/8 Twitter投稿グラフ
def charts_rate_shojin():
    df = Users.merge([UsersProfile(), UsersShojin()]).df
    df.loc[(df['country'] != 'JP')*(df['country'] != 'CN'), 'country'] = 'Rest of the World'
    p = sns.scatterplot(x='accepted', y='a_rate', hue='country', data=df, s=3)
    p.set_title('Accepted and Algorithm rate')
    plt.show()
    p = sns.scatterplot(x='rps', y='a_rate', hue='country', data=df, s=3)
    p.set_title('Rated point sum and Algorithm rate')
    plt.show()
    p = sns.scatterplot(x='tee', y='a_rate', hue='country', data=df, s=3)
    p.set_title('TEE and Algorithm rate')
    plt.show()

# 典型90、鉄則、アル数のsubmit数の推移比較
# それぞれのレート比率（2022/11〜2023/4の間）
# 2023/5/20 Twitter投稿グラフ
def chatts_typical_tessoku_alsu():
    db = AtCoderDB()
    users = Users.from_db(db, 'submissions').filter({'contest_id':
        (['typical90', 'tessoku-book', 'math-and-algorithm'], 'isin')})

    def trend(users, contest_id):
        res = users.filter({'contest_id': contest_id})\
            .resample('M').drop_duplicates(['contest_id'])\
            .df.groupby('datetime').size()
        res.name = contest_id
        return res

    typical = trend(users, 'typical90')
    tessoku = trend(users, 'tessoku-book')
    alsu = trend(users, 'math-and-algorithm')
    data = pd.concat([typical, tessoku, alsu], axis=1)[:-1].reset_index()  # 最新月は含まず
    data['datetime'] = (data['datetime'] - 200000).astype('str')
    data = data.set_index('datetime')
    p = sns.lineplot(data)
    p.set_title('Monthly unique users for THE three contests')
    p.set_ylabel('unique users')
    p.set_xlabel('month')
    for label in p.get_xticklabels():
        if label.get_text()[-2:] not in ['01', '04', '07', '10']:
            label.set_visible(False)
    plt.show()

    profile = UsersProfile()
    import datetime
    def show_rates(users, contest_id):
        tz_jst = datetime.timezone(datetime.timedelta(hours=9))
        from_ = datetime.datetime(2022, 11, 1, tzinfo=tz_jst).timestamp()
        res = users.filter({'contest_id': contest_id, 'datetime': (from_, '>=')})\
            .drop_duplicates(['contest_id'])\
            .df.merge(profile.df, on='user_id')
        atcoder_color = ['gray', 'brown', 'green', 'lightblue', 'blue', 'yellow', 'orange', 'red']
        p = sns.histplot(res, binwidth=100, x='a_rate', hue='a_color', hue_order=atcoder_color, palette=atcoder_color)
        p.set_title(f'Rating distribution for {contest_id} (since Nov-22)')
        p.set_ylabel('users')
        p.set_xlabel('current algorithm rating')
        p.set_xlim(left=0, right=3000)
        p.set_ylim(top=1000)
        plt.show()

    show_rates(users, 'typical90')
    show_rates(users, 'tessoku-book')
    show_rates(users, 'math-and-algorithm')

# 言語別、特定コンテストのAC-TLEグラフ
# コンテストのリアルタイム提出のみ
# レートはコンテスト開始時点
# 2023/6/23 Twitter投稿グラフ
def charts_AC_TLE(contest_id, languages=['C++', 'Python', 'Rust'], compare=['AC', 'TLE']):
    db = AtCoderDB()
    db.filter('contests', contest_id=contest_id)
    fr, to = map(int, db.df('contests')[['start_epoch_second', 'end_epoch_second']].to_numpy().tolist()[0])
    # コンテスト実行中のみに限定（終了時刻0秒は採用されない: AtCoderの動作で確認）
    db.filter('submissions', contest_id=contest_id, epoch_second=range(fr, to))
    db.transfer_languages_in_submissions()
    submits = db.df('submissions')
    db.filter('results', contest_id=contest_id)
    results = db.df('results')
    data = pd.merge(submits, results, on='user_id')
    data = data[data['result'].isin(compare)]
    data['problem_id'] = data['problem_id'].str.split('_').apply(lambda x: x[-1].upper())
    data = data.sort_values(['problem_id', 'result']).reset_index(drop=True)
    p = sns.violinplot(data=data, x='problem_id', y='old_rate', scale='count', hue='result', split=True, inner=None)
    sns.move_legend(p, "lower right")
    p.set_ylabel('Each user rate before the contest')
    p.set_title(f'{compare[0]} vs {compare[1]} submissions at {contest_id} (All languages)')
    plt.show()
    for language in languages:
        p = sns.violinplot(data=data[data['language'] == language], x='problem_id', y='old_rate', scale='count', hue='result', split=True, inner=None)
        sns.move_legend(p, "lower right")
        p.set_ylabel('Each user rate before the contest')
        p.set_title(f'{compare[0]} vs {compare[1]} submissions at {contest_id} ({language})')
        plt.show()

# AtCoderの特定時間帯でのリクエスト推移
# 2023/6/25 Twitter投稿グラフ
def charts_submissions_trend(contest_id, margin=300, resolution=60, heuristics=False):
    db = AtCoderDB()
    db.filter('contests', contest_id=contest_id)
    fr, to = map(int, db.df('contests')[['start_epoch_second', 'end_epoch_second']].to_numpy().tolist()[0])
    # コンテスト実行中のみに限定（終了時刻0秒は採用されない: AtCoderの動作で確認）
    if heuristics:
        db.filter('submissions', problem_id=contest_id + '_a')
    db.filter('submissions', epoch_second=range(fr - margin, to + margin))
    data = db.df('submissions')
    data['problem_id'][data['contest_id'] != contest_id] = 'other_contests'
    hue_order = sorted(data['problem_id'].unique().tolist())
    data['datetime'] = pd.to_datetime(data['epoch_second'], unit='s', utc=True).dt.tz_convert('Asia/Tokyo')
    p = sns.histplot(data=data, x='datetime', hue='problem_id', hue_order=hue_order,
        multiple='stack', bins=(to - fr + margin * 2) // resolution)
    p.set_ylabel(f'submissions / {resolution} sec')
    p.set_title(f'Submissions\' trend at the time of {contest_id}')
    plt.show()

# ChaptGPTの影響を見える化する
# 2024/11/16 Twitter投稿グラフ
# warmingup: ユーザー自身の成長影響を除くために、以前の期間の最小rated数を指定
# after: チャットGPTが導入された後の成績、最小rated数
# before: チャットGPTが導入される前の成績、最小rated数
def chart_chatgpt_impact(warmingup=('abc001', 'abc344', 30), before=('abc345', 'abc353', 3), after=('abc371', 'abc379', 3)):
    a_rate_hist = UsersARateHistory()
    candidate_warmingup = a_rate_hist.filter({'contest_id': (warmingup[0], '>=')}).filter({'contest_id': (warmingup[1], '<=')}).agg({'perf': ('count', 'rated')}).filter({'rated': (warmingup[2], '>=')})
    a_rate_hist_before = a_rate_hist.filter({'contest_id': (before[0], '>=')}).filter({'contest_id': (before[1], '<=')})
    a_rate_hist_after = a_rate_hist.filter({'contest_id': (after[0], '>=')}).filter({'contest_id': (after[1], '<=')})
    candidate_before = a_rate_hist_before.agg({'perf': ('count', 'rated')}).filter({'rated': (before[2], '>=')})
    candidate_after = a_rate_hist_after.agg({'perf': ('count', 'rated')}).filter({'rated': (after[2], '>=')})
    candidate = sorted(set(candidate_warmingup.df.index) & set(candidate_before.df.index) & set(candidate_after.df.index))
    a_rate_mean_before = a_rate_hist_before.agg({'perf': ('mean', 'before')})
    a_rate_mean_after = a_rate_hist_after.agg({'perf': ('mean', 'after')})
    a_rate_mean = Users.merge([a_rate_mean_before, a_rate_mean_after])
    data = a_rate_mean.df.loc[candidate]
    data['impact'] = data['after'] - data['before']
    print(data.describe())
    #print(data[(data['before'] < 1200)*(data['before'] >= 800)].describe())
    p = sns.scatterplot(data=data, x='before', y='after', hue='impact', palette='coolwarm', s=10)
    p.set_title(f'Impact of ChatGPT N={len(data)}, #warmingup(-{warmingup[1]})>={warmingup[2]}')
    p.set_xlabel(f'Mean perf before 4o ({before[0]}-{before[1]}, #rated>={before[2]})')
    p.set_ylabel(f'Mean perf after o1 ({after[0]}-{after[1]}, #rated>={after[2]})')
    plt.show()
