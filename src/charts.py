# 各種チャート

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from users import *

# レート、国別精進状況
# 2023/4/8 Twitter投稿グラフ
class RateShojinCharts:
    def __init__(self):
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
class TypicalTessokuAlsuCharts:
    def __init__(self):
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

# 作成中
# dfの内容について、軸（dimension）での比較棒グラフを描く
# 棒グラフはメジャー(measure)内容を、AtCoder Colorにして積み上げグラフにする
# dimension, measureはdfのcolumnを表す文字列（例: dimension='country', measure='a_rate'）
# Percent=Trueにすると100分率でのグラフになる
class CompareColorCharts:
    def __init__(self, df, dimension, measure, percent=False):
        pass
