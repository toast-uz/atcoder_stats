import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from lib import Results
from users_submits_charts import create_users_submits, read_lang_selection

DIRNAME = '../out/atcoder_stats/users_rates_charts/'

def main():
    for key in ['algorithm', 'heuristic']:
        create_users_submits(key)
    df_users = Results().compute_users()
    df_lang = read_lang_selection(a_head=15)
    df = pd.concat([df_users, df_lang], axis=1)
    for key in ['algorithm', 'heuristic']:
        rate_label = f'{key[0].lower()}_rate'
        lang_label = f'{key[0].lower()}_lang'
        df_sub = df[[lang_label, rate_label]].dropna()
        df_sub = df_sub[df_sub[rate_label] > 0]
        print(df_sub[df_sub[lang_label] == 'misc'].sort_values(rate_label, ascending=False))
        order = df_sub.groupby(lang_label).size().sort_values(ascending=False).index.tolist()
        order.remove('misc')
        order.append('misc')
        graph = sns.boxplot(data=df, x=rate_label, y=lang_label, orient='h', order=order)
        set_={'title': f'Boxplot of users\' {key} rate for each main language',
                           'xlabel': f'{key} rate',
                           'ylabel': 'main (argmax) language sorted by population'}
        graph.set(**set_)
        plt.show()

if __name__=='__main__':
    main()
