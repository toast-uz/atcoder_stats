# AtCoderDB: 生情報ダウンロード〜保管し、情報の読み出しユーティリティを提供する（シングルトン）
# Users: ユーザー単位で分析するための基本クラス（継承して使う）
#
# かならず、submissions.csv.gz を手作業でin/フォルダに仕込むこと（圧縮したまま）
# （1GB程度のファイルであり、安全性を確保するため自動化しない）
# ↑ が理解できることが、本スクリプトを使う上での最低条件です笑

import os
import copy
import glob
import time
from abc import ABC, abstractmethod
import yaml
import logging
import logging.config
import datetime
import math
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

# データベース管理クラス（シングルトン）
# 遅延loading, fetch（fetchした結果を in/にキャッシュ）, 依存関係自動最適化、ログ出力

class AtCoderDB:
    CONFIG_FILENAME = 'config.yaml'

    # 初期設定

    def __new__(cls, *args, **karg):
        if not hasattr(cls, "_instance"):
            cls._instance = super(AtCoderDB, cls).__new__(cls)
        return cls._instance

    def __init__(self, force_update=False):
        self.load_config_and_updated(force_update)
        logging.config.dictConfig(self.config['logging'])
        # 初期のDataFrameをセット {key: 空のDataFrame}
        self.__df = {}
        self.__df_saved = {}
        for key in self.config['database'].keys():
            self.__df[key] = None
            self.__df_saved[key] = None

    def load_config_and_updated(self, force_update):
        # コンフィグ読み込み（リードオンリー）
        with open(self.__class__.CONFIG_FILENAME) as f:
            self.config = yaml.safe_load(f)
        # 更新状況を読み込み、または新規作成
        self.updated = None
        updated_filename = self.config['updated']['filename']
        if os.path.isfile(updated_filename) and not force_update:
            with open(updated_filename) as f:
                self.updated = yaml.safe_load(f)
        if self.updated is None:
            self.updated = copy.deepcopy(self.config['updated']['init'])

    def save_updated(self):
        # 更新状況を書き込み
        updated_filename = self.config['updated']['filename']
        with open(updated_filename, 'w') as f:
            yaml.safe_dump(self.updated, f, default_flow_style=False)
        logging.info(f'Saved {updated_filename}')

    # データへの基本アクセス

    def df(self, key):
        if self.__df[key] is None: self.load(key)   # 遅延ローディング
        return self.__df[key]

    # データのload / fetch

    def load(self, key):
        self.__df[key] = pd.DataFrame()
        # 先読みすべきファイルの読み込み
        if 'pre_processing' in self.config['database'][key]: self.load_pre_file(key)
        # dependencyを確認して不整合あればfetchする
        broken = self.broken_dependencies(key)
        filename = self.config['database'][key]['filename']
        if not broken and os.path.isfile(filename):
            self.__df[key] = pd.concat([self.__df[key], self.load_file(key, filename)])
        else:
            self.fetch(key, broken)

    # 依存関係の確認 正常 → False(0)、破れあり True (1〜2 の2段階)
    def broken_dependencies(self, key):
        res = False
        for item in self.config['database'][key]['dependencies']:
            self_ = time.time() if item['self'] == 'now_epoch_second' else self.updated[key][item['self']]
            other = self.updated[item['other_key']][item['other_value']]
            op = item['op']
            type_ = item['type']
            if self_ == 'virtual':
                assert item['self'] == 'contest_ids'
                filename = self.config['database'][key]['filename']
                if os.path.isfile(filename):
                    df = self.load_file(key, filename)
                    self_ = df.reset_index()['contest_id'].to_list()
                else:
                    self_ = []
            if self_ is not None and other is not None:
                if 'self_gap' in item: self_ += item['self_gap']
                if op == 'lt':
                    if self_ < other: continue
                elif op == 'eq':
                    if self_ == other: continue
                elif op == 'set_ge':
                    if set(self_) >= set(other): continue
                else:
                    assert False, f'Op: {op} is not implemented.'
            res = max(res, 1 if type_=='soft' else 2)
            # ログ表示を短縮するため、リストの場合は要素数だけにする
            x = self_ if not isinstance(self_, list) else f'list#{len(self_)}'
            y = other if not isinstance(other, list) else f'list#{len(other)}'
            logging.warn(f'Found broken dependencies on {key} at type={type_} as self_={x}, op={op}, other={y}')
            if op == 'set_ge': logging.warn(f'- diff: {set(other) - set(self_)}')
        return res

    def load_file(self, key, filename):
        if self.__df_saved[key] is None:
            logging.info(f'Loading {key} form {filename}')
            self.__df_saved[key] = pd.read_csv(filename, index_col=0)
        return self.__df_saved[key]

    # ベースファイルを読み込む
    # 現状はsubmissions専用、かつベースファイルの存在が前提
    # （1GB近くあるため、手動ダウンロードに限定する）
    def load_pre_file(self, key):
        assert key == 'submissions'
        pre_cache = self.config['database'][key]['pre_processing']['fetch']['cache']
        pre_url = self.config['database'][key]['pre_processing']['fetch']['url']
        assert os.path.isfile(pre_cache), f'Download {pre_url} and set it to {pre_cache} manually.'
        logging.info(f'Wait a minutes loading {key}_base_file from {pre_cache}')
        start_time = time.time()
        self.__df[key] = pd.read_csv(pre_cache, index_col=0)
        duration = int(time.time() - start_time)
        logging.info(f'Loaded {key}_base_file in {duration} seconds.')
        logging.info(f'Wait a minutes sorting {key}.')
        start_time = time.time()
        self.__df[key] = self.__df[key].sort_values('epoch_second')
        duration = int(time.time() - start_time)
        logging.info(f'Sorted {key} in {duration} seconds.')
        self.updated[key]['base_file_last_epoch_previous'] = self.updated[key]['base_file_last_epoch']
        last_epoch = int(self.__df[key].tail(1)['epoch_second'].values[0])
        logging.info(f'Last epoch of base_file is {datetime.datetime.fromtimestamp(last_epoch)}')
        if time.time() - last_epoch >= 864000:
            logging.error('Base file is too old, restart this after getting new base file.')
            exit()
        if self.updated[key]['base_file_last_epoch_previous'] != last_epoch:
            logging.info(f'Detected new base_file.')
        self.updated[key]['base_file_last_epoch'] = last_epoch
        self.save_updated()

    # type_=1: 現在のout/をもとに追加をfetchしてout/を新規作成または上書きする
    # type_=2: baseは維持するが、現在のout/とin/キャッシュを破棄して、最初からfetchしなおす
    def fetch(self, key, type_=1):
        filename = self.config['database'][key]['filename']
        cache = self.config['database'][key]['fetch']['cache']
        # type_=2の処理をまずはやっておく
        if type_ == 2:
            assert key == 'submissions' or key == 'problem_models'
            logging.info(f'Removing files for {key}')
            if os.path.isfile(filename): os.remove(filename)
            if cache is not None:
                for f in glob.glob(cache.replace('{}', '*')):
                    os.remove(f)
        # 以降は共通処理
        # baseファイルで読み込み済な位置を記録しておく
        pre_file_len = len(self.__df[key])
        df_list = []
        if pre_file_len > 0:
            # 重複確認のため、すでに読み込まれているうち1000行を読み出す
            assert key == 'submissions'
            df_list.append(self.__df[key].tail(1000))
            self.__df[key] = self.__df[key].head(pre_file_len - len(df_list[-1]))
        # ファイルが存在していれば、まず読み込む
        if os.path.isfile(filename):
            df = self.load_file(key, filename)
            if len(df) > 0: df_list.append(df)
        # 読み込んだ部分からの追加をフェッチする
        uniqueness = self.config['database'][key].get('uniqueness')
        state = self.get_value_state(key, df_list)
        while state is not None:
            value, state = self.get_next_value(key, state)
            df = self.get_cached_url(key, value)
            if len(df) == 0:
                break
            if uniqueness is not None and len(df_list) > 0:   # 重複処理
                name = df.index.name
                df_unique = pd.concat([df_list[-1], df]).reset_index().drop_duplicates(
                    uniqueness['column']).iloc[len(df_list[-1]):].set_index(name)
                df_list.append(df_unique)
                if len(df_unique) == 0 or (len(df_unique) < len(df) and uniqueness['break_if_duplicated']):
                    break
            else:
                df_list.append(df)
        # 後処理
        if 'post_processing' in self.config['database'][key]:
            item = self.config['database'][key]['post_processing']
            if key == 'submissions':
                recently_contest_ids = pd.concat(df_list)['contest_id'].drop_duplicates().to_list()
                self.updated[key]['recently_contest_ids'] = recently_contest_ids
            elif key == 'contests':
                df = self.get_cached_url(key, cache=item['fetch']['cache'], url=item['fetch']['url'],
                                         post_processing=True)
                df_list.append(df)
                assert uniqueness is not None
                # 読み込んだ分をすべてつなげて後処理との重複を確認する
                df_list = [pd.concat(df_list).reset_index().drop_duplicates(uniqueness['column']).set_index('contest_id')]
            elif key == 'results':
                # ソートする
                df_list = [pd.concat(df_list).reset_index(drop=True).sort_values('end_epoch_second')]
            else:
                assert False
        # フェッチした結果を全てつなげる
        self.__df[key] = pd.concat([self.__df[key], *df_list])
        # セーブする
        self.save(key, pre_file_len)

    # configにしたがってキャッシュから読み込み、キャッシュが無ければurlを読み込んでキャッシュする
    # パースしたデータフレームを返す
    def get_cached_url(self, key, value=None, cache=None, url=None, post_processing=False):
        if cache is None:
            cache = self.config['database'][key]['fetch']['cache']
            if cache is not None: cache = cache.replace('{}', str(value))
        if url is None:
            url = self.config['database'][key]['fetch']['url'].replace('{}', str(value))
        if cache is not None and os.path.isfile(cache):
            logging.info(f'Loading {cache}')
            with open(cache) as f:
                text = f.read()
            df = self.parse(key, text, post_processing)
        else:
            logging.info(f'Getting {url}')
            try:
                header = { 'accept-language': 'ja-JP' }
                response = requests.get(url, headers=header)
                response.raise_for_status()
                response.encoding = response.apparent_encoding
                text = response.text
                df = self.parse(key, text, post_processing)
                if cache is not None and len(df) > 0:
                    with open(cache, 'w') as f:
                        f.write(text)
                        logging.info(f'Saved as {cache}')
                        # fetch時刻をupdatedに登録
                        if 'fetch_epoch_second' in self.updated[key]:
                            self.updated[key]['fetch_epoch_second'] = time.time()
            except Exception as e:
                logging.error(e)
                assert False
            time.sleep(1)
        return df

    def get_value_state(self, key, df_list):
        if key == 'submissions':
            return df_list
        elif key == 'contests':
            return 1
        elif key == 'results':
            loaded_contest_ids = pd.concat(df_list)['contest_id'].drop_duplicates().to_list() if len(df_list) > 0 else []
            df = self.df('contests')
            fetching_contest_ids = df[df['rated'] == 1].sort_values(
                'start_epoch_second', ascending=False).index.to_list()
            fetching_contest_ids = [i for i in fetching_contest_ids if i not in set(loaded_contest_ids)]
            return fetching_contest_ids if len(fetching_contest_ids) > 0 else None
        elif key == 'problem_models':
            return 0
        else:
            assert False

    def get_next_value(self, key, state):
        if key == 'submissions':
            value = int(state[-1].tail(1)['epoch_second'].values[0])
            return value, state
        elif key == 'contests':
            return state, state + 1
        elif key == 'results':
            new_state = state[1:] if len(state) > 1 else None
            return state[0], new_state
        elif key == 'problem_models':
            return 0, None
        else:
            assert False

    # 得られたテキストをパースしてDataFrameにする
    def parse(self, key, text, post_processing=False):
        if key == 'submissions':
            df = pd.read_json(text).set_index('id')
            if df['epoch_second'].nunique() <= 1:       # 時刻が一種類の場合は最新と判断して消す
                logging.info('Clear this fetch because of epoch_second nunique <= 1')
                return pd.DataFrame()
            count_under_judge = df['result'].str.match('.*\d.*').sum()
            if count_under_judge > 0:  # 結果に数字が含まれていたらジャッジ中と判断して消す
                logging.info(f'Clear this fetch because of including {count_under_judge} submissions under judgement.')
                return pd.DataFrame()
            return df
        elif key == 'contests' and not post_processing:
            bs = BeautifulSoup(text, 'html.parser').find('tbody')
            df = pd.DataFrame()
            if bs is None:
                logging.info(f'Get no contest.')
                return pd.DataFrame()
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
                end_epoch_second = start_epoch_second + duration_second
                rated = int(bs_td[3].text != '-')
                df_add = pd.DataFrame([[contest_id, start_epoch_second, end_epoch_second, duration_second, title, rated, type_]],
                    columns=['contest_id', 'start_epoch_second', 'end_epoch_second',
                             'duration_second', 'title', 'rated', 'type_']).set_index('contest_id')
                df = pd.concat([df, df_add])
            return df
        elif key == 'contests' and post_processing:
            df = pd.read_json(text).rename(columns={'id': 'contest_id', 'rate_change': 'rated'})
            df['rated'] = (df['rated'] != '-').astype(int)
            df['type_'] = 'Algorithm'
            df.insert(2, 'end_epoch_second', df['start_epoch_second'] + df['duration_second'])
            return df.set_index('contest_id')
        elif key == 'results':
            if text == '[]':
                logging.warn('Have\'t prepared results. Maybe before computing final results.')
                return pd.DataFrame()
            df = pd.read_json(text)[['ContestScreenName', 'EndTime', 'UserScreenName', 'Country', 'Affiliation',
                'Place', 'OldRating', 'NewRating', 'Performance', 'IsRated']]
            df.columns = ['contest_id', 'end_epoch_second', 'user_id', 'country', 'affiliation',
                'place', 'old_rate', 'new_rate', 'perf', 'user_rated']
            df['contest_id'] = df['contest_id'].str.split('.', expand=True)[0]
            df['end_epoch_second'] = df['end_epoch_second'].apply(lambda x: datetime.datetime.fromisoformat(x).timestamp())
            df['user_rated'] = df['user_rated'].astype(int)
            df['perf'] = df['perf'].apply(self.__class__.adjust_perf)   # 内部perfを外部perfへ変換
            df.loc[df['user_rated'] == 0, 'perf'] = 0    # ratedでなければperf=0に（上と２段階に分けるのが速い）
            return df
        elif key == 'problem_models':
            df_raw = pd.read_json(text).T
            df_raw.index.name = 'problem_id'
            df_raw['contest_id'] = df_raw.index.map(lambda s: '_'.join(s.split('_')[:-1]))
            diff = df_raw['difficulty'].agg(self.adjust_diff)
            diff.name = 'diff'
            tee = (df_raw['slope'] * 4000 + df_raw['intercept']).agg(np.exp)
            tee.name = 'tee'
            # isProblemModelWithDifficultyModel の判断基準として、すべてのproblem_modelsフィールドが有効であること
            return pd.concat([df_raw['contest_id'], diff, tee], axis=1).dropna()
        else:
            assert False

    # 内部perfを外部perfへ変換
    def adjust_perf(perf):
        return int(400 / math.exp((400 - perf) / 400)) if perf < 400 else perf

    # 内部diffを外部diffへ変換
    def adjust_diff(self, diff):
        return round(400 / math.exp((400 - diff) / 400)) if diff < 400 else diff

    # ファイルおよびupdate状態をセーブ
    def save(self, key, pre_file_len=0):
        # ファイルをセーブ
        filename = self.config['database'][key]['filename']
        self.__df[key].iloc[pre_file_len:].to_csv(filename)
        logging.info(f'Saved {key} to {filename}')
        # update状態を更新してセーブ
        if 'recently_rated_contest_ids' in self.updated[key]:
            assert key == 'contests'
            df = self.__df[key]
            df = df[(df['rated'] == 1)*(df['end_epoch_second'] > time.time() - 86400 * 30)]  # 直近30日
            self.updated[key]['recently_rated_contest_ids'] = df.index.to_list()
            if 'recently_rated_algo_contest_ids' in self.updated[key]:
                df = df[df['type_'] == 'Algorithm']
                self.updated[key]['recently_rated_algo_contest_ids'] = df.index.to_list()
        self.save_updated()

    # 変換

    # 言語を変換集約する
    def transfer_languages_in_submissions(self):
        key = 'submissions'
        logging.info(f'Transfer languages in {key}.')
        start_time = time.time()
        self.__df[key]['language'] = self.df(key)['language'].str.extract('^([A-Za-z\+\#]+)', expand=True)
        duration = int(time.time() - start_time)
        logging.info(f'Done, transferred languages in {key} in {duration}sec.')

    # ratedか/heuristicかどうかを表すフラグを追加する
    def add_rated_and_type(self, key):
        if 'rated' in self.df(key).index or 'type_' in self.df(key).index:
            logging.info(f'Abort, already added rated and type in {key}.')
            return
        logging.info(f'Adding rated and type in {key}.')
        start_time = time.time()
        contests = self.df('contests')
        append = contests.loc[self.df(key)['contest_id'], ['rated', 'type_']]
        append.index = self.df(key).index
        self.__df[key] = pd.concat([self.df(key), append], axis=1)
        duration = int(time.time() - start_time)
        logging.info(f'Done, added rated and type in {key} in {duration}sec.')

    # diff/teeを追加する
    def add_diff_and_tee_in_submissions(self):
        key = 'submissions'
        if 'diff' in self.df(key).index or 'tee' in self.df(key).index:
            logging.info(f'Abort, already added diff and tee in {key}.')
            return
        logging.info(f'Adding diff and tee in {key}.')
        start_time = time.time()
        problems_models = self.df('problem_models')[['diff', 'tee']]
        key_problem_id = pd.DataFrame(self.df(key)['problem_id'])
        problems_models = pd.concat([problems_models.reset_index(), key_problem_id.drop_duplicates()]
                                    ).drop_duplicates('problem_id').set_index('problem_id')
        append = problems_models.loc[key_problem_id['problem_id'], ['diff', 'tee']]
        append.index = self.df(key).index
        self.__df[key] = pd.concat([self.df(key), append], axis=1)
        duration = int(time.time() - start_time)
        logging.info(f'Done, added diff and tee in {key} in {duration}sec.')

    # 汎用関数

    # DataFrameをフィルタ
    # - リストを指定したら要素のorで抽出
    # - rangeを指定したら範囲クエリー
    # 例 filter(key, column1=x, column2=[y, z], column3=range(s, t))
    def filter(self, key, inplace=False, **karg):
        df = self.df(key)
        for column, value in karg.items():
            if column in self.df(key).columns:
                if isinstance(value, range):
                    df = df[(df[column] >= value[0]) * (df[column] <= value[-1])]
                elif isinstance(value, list):
                    df = df[df[column].isin(value)]
                else:
                    df = df[df[column] == value]
        if inplace:
            self.__df[key] = df
        return  df

    # DataFrameの重複排除
    def drop_duplicates(self, key, arg, inplace=False):
        df = self.df(key).drop_duplicates(arg)
        if inplace:
            self.__df[key] = df
        return  df

# Usersは仮想クラスなので、からなず継承して使ってください

class Users(ABC):
    def __init__(self, df=None, filename=None):
        self.df = df
        self.filename = filename
        if df is None and filename is not None and os.path.isfile(filename):
            self.load()

    @abstractmethod
    def update(self):
        pass

    # filter後に、user_idをキーにaggで集計したDataFrameを、Usersオブジェクトとして設定する
    # drop_duplicatesは、user_idごとのdrop_duplicates
    # sizeは、user_idごとのsize
    # aggは、{column名: 集計ポリシー} または {元column名: (集計ポリシー, 先column名)} 形式の辞書
    #   集計ポリシーは、max, min, sum, head, tail, np.argmax などをサポート
    @classmethod
    def from_db(cls, db, key, filter=None, drop_duplicates=None, size=None, agg={}):
        # DataFrameの読み込み
        if filter is None:
            df = db.df(key)
        else:
            df = db.filter(key, **filter)
        res = []
        if drop_duplicates is not None:
            df = df.drop_duplicates(['user_id', *drop_duplicates])

        grouped = df.groupby('user_id')
        if size is not None:
            size_col = grouped.size()
            size_col.name = size
            res.append(size_col)
        for fr_, policy in agg.items():
            if isinstance(policy, tuple):
                policy, to_ = policy
            else:
                to_ = fr_
            col = grouped[fr_].agg(policy)
            col.name = to_
            res.append(col)
        return cls(pd.concat(res, axis=1)) if len(res) > 0 else Users(df)

    # 'user_id' をキーに別のUserオブジェクトをマージ
    # others はリスト
    @classmethod
    def merge(cls, others):
        return cls(pd.concat([other.df for other in others], axis=1))

    # 特定の列をキーに比率を追加する
    def add_rate(self, fr_, to_):
        self.df[to_] = self.df[fr_] / self.df[fr_].sum()

    def save(self, filename=None):
        filename = self.filename if filename is None else filename
        assert filename is not None
        self.df.to_csv(filename)
        logging.info(f'Saved {self.__class__} to {filename}')

    def load(self, filename=None):
        filename = self.filename if filename is None else filename
        assert filename is not None
        logging.info(f'Loading {self.__class__} from {filename}')
        self.df = pd.read_csv(filename, index_col=0)

# 使わないAPI
# https://atcoder.jp/users/ユーザ名/history/json
# https://atcoder.jp/contests/コンテスト名/standings/json


