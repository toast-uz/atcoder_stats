# 生情報ダウンロード〜保管
# 保管した情報の読み出し用ユーティリティ

import os
import time
import datetime
import re
import math
import pandas as pd
import requests
from bs4 import BeautifulSoup

# Submitデータ
# 初期ダウンロード（手動）
# https://s3-ap-northeast-1.amazonaws.com/kenkoooo/submissions.csv.gz
# in/submissions.csv.gz として保存

class Submits:
    filename = '../in/submissions.csv.gz'

    def __init__(self, df=None):
        if df is None:
            print(f'Wait a minute for loading {Submits.filename} ...', flush=True, end='')
            self.df = pd.read_csv(Submits.filename).dropna()
            print(f' done.')
        else:
            self.df = df

    def clone(self):
        return Submits(self.df.copy())

    def print(self):
        print(self.df)

    # 特定の列でフィルタする（func f がTrueで抽出）
    def filter(self, column, f):
        return Submits(self.df[self.df[column].apply(f)])

    # 名寄せする
    def transform(self, column, transform=lambda x: x):
        tmp = self.clone()
        tmp.df[column] = tmp.df[column].apply(transform)
        return tmp

    # 特定の列をキーにカウントして、多い純からソートされる
    # 返り値は1列のdf（行ラベルがcolumn指定したもの）であるが、ratioありなら比率の列もつく
    def count(self, column, *, ratio=False, ascending=False):
        df = pd.DataFrame(self.df.groupby(column).size().sort_values(ascending=ascending),
                          columns=['count'])
        if ratio:
            df['ratio'] = df['count'] / df['count'].sum()
        return df

# Submitデータ追加ダウンロード機能（予定）
# ファイルが増えすぎても読み込みがめんどいので、gzをダウンロードしなおしたら綺麗になるようにしておく
#
# Submitデータ中の最終 submit時刻 を確認（ファイルタイムスタンプで↓のキャッシュを使うかは決める）
# 両方なければエラーとして中断する（大量ダウンロードが危険なので）
#
# 生ファイルを確認したら、in/submissions.csv.gz.latest_epoch_second として数値をキャッシュとしてテキストに書く
# {unix_time_second}をgzの最終submit時刻とと比較して古いものは削除
# 最終submit時刻以降での最新{unix_time_second}を確認（なければ最終submit時刻）
# 上記の時刻以降を以下から順次ダウンロード（500件未満になったら終了）
# https://kenkoooo.com/atcoder/atcoder-api/v3/from/{unix_time_second}
# そのまま保管
# 重複データがあることに注意


# コンテストデータ
# https://kenkoooo.com/atcoder/resources/contests.json は誤情報あり、使わない
# https://atcoder.jp/contests/archive から取得
# out/atcoder_stats/contests.jsonが無い場合は、https://atcoder.jp/contests/archive?page={} からも取る
# スクレイピングして、結果をout/atcoder_stats/contests.jsonに保存

class Contests:
    filename = '../out/contests.csv'
    columns = ['contest_id', 'start_epoch_second', 'duration_second', 'title', 'rated', 'type_']

    # check_update = None (use local only), 'new' (check AtCoder new info), 'all': check AtCoder all info
    def __init__(self, df=None, *, check_update=None):
        if df is not None:
            self.df = df
            return
        elif check_update is None and os.path.isfile(Contests.filename):
            self.df = pd.read_csv(Contests.filename, index_col=0)
            return
        elif check_update == 'all' and os.path.isfile(Contests.filename):
            print(f'Wait a minute for updating {Contests.filename} ...')
            df_org = pd.read_csv(Contests.filename, index_col=0)
        else:
            print(f'Wait a minute for creating {Contests.filename} ...')
            df_org = pd.DataFrame(columns=Contests.columns).set_index('contest_id')

        page = 1
        df_new = pd.DataFrame(columns=Contests.columns).set_index('contest_id')
        while True:
            try:
                df_page = Contests.scraping(page)
            except Exception as e:
                print(e)
                break
            org_contests = set(df_org.index.to_list())
            page_contests = set(df_page.index.to_list())
            duplicated = org_contests & page_contests
            new = page_contests - org_contests
            print(f'Found {len(new)} new contests: {", ".join(new)}')
            if not check_update=='all' and len(duplicated) > 0:
                print(f'Found {len(duplicated)} duplicated contests, quit searching. You can continue with force=True.')
                break
            df_new = pd.concat([df_new, df_page]).drop_duplicates()
            page += 1
            time.sleep(1)
        if len(df_new) == 0:
            self.df = df_org
            print(f'{Contests.filename} is already updated.')
        else:
            self.df = pd.concat([df_org, df_new]).sort_values('start_epoch_second', ascending=False)
            self.df.to_csv(Contests.filename)
            print(f'{Contests.filename} is updated now.')

    def scraping(page):
        url = f'https://atcoder.jp/contests/archive?page={page}'
        print(f'Getting {url} ...')
        header = { 'accept-language': 'ja-JP' }
        response = requests.get(url, headers=header)
        response.raise_for_status()
        response.encoding = response.apparent_encoding
        bs = BeautifulSoup(response.text, 'html.parser').find('tbody')
        df = pd.DataFrame(columns=Contests.columns).set_index('contest_id')
        if bs is None:
#            with open(f'../in/contest_archive_{page}.html') as f:
#                bs = BeautifulSoup(f.read(), 'html.parser').find('tbody')
            raise Exception(f'Get no contest from archive page={page}')
        while True:
            bs = bs.find_next('tr')
            if bs is None: break
            bs_td = bs.find_all('td')
            start_epoch_second = datetime.datetime.fromisoformat(bs_td[0].find('time').text).timestamp()
            type_ = bs_td[1].find('span')['title']
            contest_id = bs_td[1].find('a')['href'].split('/')[-1]
            title = bs_td[1].find('a').text
            hours, minutes = map(int, bs_td[2].text.split(':'))
            duration_second = datetime.timedelta(hours=hours, minutes=minutes).total_seconds()
            rated = int(bs_td[3].text != '-')
            df_add = pd.DataFrame([[contest_id, start_epoch_second, duration_second, title, rated, type_]],
                                  columns=Contests.columns).set_index('contest_id')
            df = pd.concat([df, df_add])
        return df

    def filter(self, type_=None, rated=None):
        df = self.df.copy()
        if type_ is not None:
            df = df[df['type_'] == type_]
        if rated is not None:
            df = df[df['rated'] == rated]
        return Contests(df)

# Resultsデータ
# Rated分について存在しない in/results/* を https://atcoder.jp/contests/コンテスト名/results/json から取得
# Contestデータとの付き合わせを実施
# ContestTypeが使えるかもしれないが使っていない

class Results:
    dirname = '../in/'
    filename = '../out/results.csv'
    columns_org = ['ContestScreenName', 'UserScreenName', 'Country', 'Affiliation',
                   'Place', 'OldRating', 'NewRating', 'Performance', 'IsRated']
    columns = ['contest_id', 'user_id', 'country', 'affiliation',
               'place', 'old_rate', 'new_rate', 'perf', 'rated']

    def __init__(self):
        df_list = []
        df_contests = Contests().df   # 最新のContestを得たい場合は、別にContestsを呼び出す
        df_rated_contests = df_contests[df_contests['rated'] == 1].copy()
        num_contests = len(df_rated_contests)

        # contest_results に欠損が無く、results の日付が最新であれば、resultsから読みこんで終了する
        if os.path.isfile(Results.filename):
            updated = True
            results_date = os.stat(Results.filename).st_mtime
            for i, contest_id in enumerate(df_rated_contests.index):
                filename = f'{Results.dirname}{contest_id}.json'
                if not os.path.isfile(filename) or os.stat(filename).st_mtime > results_date:
                    updated = False
                    break
            if updated:
                self.df = pd.read_csv(Results.filename, index_col=0)
                return

        # contest_results データの読み込みとパース
        print(f'{Results.filename} is old, wait a minute to update ...')
        df_rated_contests['end_epoch_second'] = df_rated_contests['start_epoch_second']
        df_rated_contests['end_epoch_second'] += df_rated_contests['duration_second']
        df_rated_contests = df_rated_contests.sort_values('end_epoch_second', ascending=False)
        for i, contest_id in enumerate(df_rated_contests.index):
            filename = f'{Results.dirname}{contest_id}.json'
            if os.path.isfile(filename):
                print(f'({i+1}/{num_contests}) Loading {filename} ...')
                df_list.append(pd.read_json(filename)[Results.columns_org])
            else:
                url = f'https://atcoder.jp/contests/{contest_id}/results/json'
                print(f'({i+1}/{num_contests}) Getting {url} ...')
                try:
                    response = requests.get(url)
                    response.raise_for_status()
                    with open(filename, 'w') as f:
                        f.write(response.text)
                    df_list.append(pd.read_json(filename)[Results.columns_org])
                    print(f'Saved as {filename}')
                except Exception as e:
                    print(e)
                time.sleep(1)
        print('Parsing contests results ...', end='', flush=True)
        self.df = pd.concat(df_list)
        print('(pass 1/3)...', end='', flush=True)
        self.df.columns = Results.columns
        self.df['contest_id'] = self.df['contest_id'].str.split('.', expand=True)[0]
        print('(pass 2/3)...', end='', flush=True)
        self.df['rated'] = self.df['rated'].astype(int)
        self.df['perf'] = self.df['perf'].apply(Results.adjust_perf)   # 内部perfを外部perfへ変換
        self.df.loc[self.df['rated'] == 0, 'perf'] = 0    # ratedでなければperf=0に（上と２段階に分けるのが速い）
        print('(pass 3/3)...', end='', flush=True)
        self.df = self.df.reset_index(drop=True)
        print('done.')
        # パース結果のセーブ
        self.df.to_csv(Results.filename)
        print(f'Saved {Results.filename}')

    # 内部perfを外部perfへ変換
    def adjust_perf(perf):
        return int(400 / math.exp((400 - perf) / 400)) if perf < 400 else perf

    # コンテスト終了時点（Noneなら最新）でのユーザーrate情報 (affiliateは必ず最新のもの)
    def compute_users(self, at_contest_id=None):
        df_user = self.df.groupby('user_id').head(1).set_index('user_id')[['country', 'affiliation']]
        at_idx = self.df[self.df['contest_id'] == at_contest_id].index.to_list()[0] if at_contest_id is not None else 0
        df = self.df.iloc[at_idx:]
        df = df[df['rated'] == 1]
        contests = Contests()
        df_rate = []
        for type_, name in [('Algorithm', 'a_rate'), ('Heuristic', 'h_rate')]:
            contests_list = contests.filter(type_=type_).df.index.to_list()
            df_rate.append(df[df['contest_id'].isin(contests_list)].groupby('user_id').
                           head(1).set_index('user_id')['new_rate'])
            df_rate[-1].name = name
        return pd.concat([df_user, *df_rate], axis=1)


# 使わないAPI
# https://atcoder.jp/users/ユーザ名/history/json
# https://atcoder.jp/contests/コンテスト名/standings/json


# submit生データとつきあわせ
# user_id,contest_id,place,old_rate,new_rate,perf,rated,submits,ac,language
# 結果を out/atcoder_stats/results.csv として保管
# user_id,a_rate,h_rate,affiliate
# 結果を out/atcoder_stats/users.csv として保管


# 各種の名寄せスクリプト

# 言語名の名寄せ
# 空白の前、かつ末尾の数値は取る

class Transforms:
    def __init__(self):
        # 言語名の名寄せ
        pattern = '^[A-Za-z\+\#]+'
        self.re_pattern_lang = re.compile(pattern)
    # 言語名の名寄せ
    def language(self, x):
        tmp = self.re_pattern_lang.match(x)
        return tmp.group() if tmp is not None else ''

# 大学名の名寄せ
