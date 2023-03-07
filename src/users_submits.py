import pandas as pd
from lib import Submits, Contests, Transforms

DIRNAME = '../out/atcoder_stats/users_submits/'
ID_COLUMN_LANG_BEGIN = 3

# 提出結果を抽出して保存する
#   contests_filter: コンテスト種別やratedかなどでフィルタ
#   realtime: コンテスト時間内の提出かでフィルタ
#   result: ACなどの結果でフィルタ
def create_users_submits(submits, filename, *, contests_filter=None, realtime=None, result=None):
    print(f'Creating users_submits/{filename} ...', flush=True, end='')
    df = submits.df
    contests = Contests().filter(**contests_filter)
    if contests_filter is not None:
        df = df[df['contest_id'].isin(contests.index.to_list())]
    if result is not None:
        df = df[df['result'] == result]
    if realtime is not None:
        df_list = []
        for contest_id, detail in contests.df.iterrows():
            print(f'Filtering realtime={realtime} submit for {contest_id} ...')
            if realtime:
                df_list.append(df[df['contest_id' == contest_id]
                                * df['epoch_second' >= detail['start_epoch_second']
                                * df['epoch_second' <= detail['start_epoch_second']
                                    + detail['duration_second']]]])
            else:
                df_list.append(df[df['contest_id' == contest_id]
                                * df['epoch_second' < detail['start_epoch_second']
                                * df['epoch_second' > detail['start_epoch_second']
                                    + detail['duration_second']]]])
        df = pd.concat(df_list)
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

# 言語リストのみ抽出
def languages(df):
    return df.drop(df.columns[[n for n in range(ID_COLUMN_LANG_BEGIN)]], axis=1)

# 言語ランキングの求め方
# languages_rank = users_submits.languages(df).sum(axis=0).sort_values(ascending=False)

def main():
    # ユーザでグルーピングしてepoch_timeの最大最小、languageのカウントをとる
    transform_language = Transforms().language
    submits = Submits().transform('language', transform_language)
    create_users_submits(submits, 'all.csv')
    create_users_submits(submits, 'algorithm.csv', contests_filter={'type_': 'Algorithm'})
    create_users_submits(submits, 'heuristic.csv', contests_filter={'type_': 'Heuristic'})

if __name__=='__main__':
    main()
