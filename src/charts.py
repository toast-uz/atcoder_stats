# 各種チャート

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from users import *

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

