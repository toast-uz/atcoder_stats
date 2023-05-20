import time
import os
import logging
import numpy as np
import pandas as pd
from lib import AtCoderDB, logging_time

# Usersクラス
# submissions または results をもとに、user_idをインデックス管理、datetimeフィールドを持つ
# (双方ともlibにおいてdatetimeで昇順にソート済)

class Users:
    def __init__(self, df=None, filename=None):
        self.df = df
        self.filename = filename
        if df is None and filename is not None and os.path.isfile(filename):
            self.load()

    @classmethod
    def from_db(cls, db, key):
        assert key == 'submissions' or key == 'results'
        # datetimeは、submissionsであればepoch_second, resultsであれば end_epoch_secondを使う
        time_column = 'epoch_second' if key == 'submissions' else 'end_epoch_second'
        # そのまま数値で持ち、datetime64型は使わない(user_idグループの中でresampleすると遅いので)
        df = db.df(key).rename(columns={time_column: 'datetime'}).set_index('user_id')
        return cls(df)

    # datetimeのepoch数値をアップorダウンサンプリングする、resampleのように集計はしない
    # 'epoch': エポック秒, 'D': YYYYMMDD、'M': YYYYMM、'Y': YYYY の4形式 (いずれも東京TZ)
    # epochに戻すことはできない（遅いため）
    def resample(self, fleq, inplace=False):
        df = self.df
        fleq_org = self.__class__._type_datetime(df)
        if fleq == fleq_org: return self.__class__(df)
        match fleq_org:
            case 'epoch':
                datetime = pd.to_datetime(df['datetime'], unit='s', utc=True).dt.tz_convert('Asia/Tokyo')
                year, month, day = datetime.dt.year, datetime.dt.month, datetime.dt.day
            case 'Y':
                year, month, day = df['datetime'], 1, 1
            case 'M':
                year, month, day = df['datetime'] // 100, df['datetime'] % 100, 1
            case 'D':
                year, month, day = df['datetime'] // 10000, df['datetime'] // 100 % 100, df['datetime'] % 100
            case _:
                assert False
        match fleq:
            case 'Y':
                df['datetime'] = year
            case 'M':
                df['datetime'] = year * 100 + month
            case 'D':
                df['datetime'] = year * 10000 + month * 100 + day
            case _:
                assert False
        if inplace:
            self.df = df
        else:
            return self.__class__(df)

    # datetime列の型の自動判定
    @classmethod
    def _type_datetime(cls, df):
        match len(str(int(df['datetime'].head(1).values[0]))):
            case digits if digits >= 10:
                return 'epoch'
            case digits if digits == 8:
                return 'D'
            case digits if digits == 6:
                return 'M'
            case digits if digits == 4:
                return 'Y'
            case _:
                assert False

    # 重複削除において、datetime型がresample済であれば一致条件に入れる
    @logging_time
    def drop_duplicates(self, subset=None, inplace=False):
        df = self.df.reset_index()
        if subset is None:
            subset = []
        if 'datetime' in self.df.columns and self._type_datetime(self.df) != 'epoch':
            subset.insert(0, 'datetime')
        subset.insert(0, 'user_id')
        df = df.drop_duplicates(subset).set_index('user_id')
        if inplace:
            self.df = df
        else:
            return self.__class__(df)

    # param={column: value} または param={column: (value, op)}
    # opは、'==' と '!=' を許容
    @logging_time
    def filter(self, params, inplace=False):
        df = self.df
        for column, param in params.items():
            (value, op) = param if isinstance(param, tuple) else (param, '==')
            match op:
                case '==':
                    df = df[df[column] == value]
                case '!=':
                    df = df[df[column] != value]
                case '<':
                    df = df[df[column] < value]
                case '<=':
                    df = df[df[column] <= value]
                case '>':
                    df = df[df[column] > value]
                case '>=':
                    df = df[df[column] >= value]
                case _:
                    assert False
        if inplace:
            self.df = df
        else:
            return self.__class__(df)

    # 集計において、datetime型がresample済であれば一致条件に入れる
    # map={元column: 集計func} または {元column: (集計func, 集計後column)}
    @logging_time
    def agg(self, map, inplace=False):
        df = self.df.reset_index()
        by = ['user_id']
        if 'datetime' in df.columns and self.__class__._type_datetime(df) != 'epoch':
            by.append('datetime')
        res = []
        for column_fr, param in map.items():
            (func, column_to) = param if isinstance(param, tuple) else (param, column_fr)
            to_ = df.groupby(by)[column_fr].agg(func)
            to_.name = column_to
            res.append(to_)
        df = pd.concat(res, axis=1)
        if inplace:
            self.df = df
        else:
            return self.__class__(df)

    @classmethod
    def merge(cls, others):
        return cls(pd.concat([other.df for other in others], axis=1))

    # 特定の列をキーに比率を追加する
    def add_rate(self, column, rate_column, inplace=False):
        df = self.df
        df[rate_column] = df[column] / df[column].sum()
        if inplace:
            self.df = df
        else:
            return self.__class__(df)

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

# Usersを継承して、代表的な分析を行う
#
# 共通的な使い方
# クラスオブジェクトを作ると、既存でセーブされていれば読み込む
# update() で最新情報にする（関連のAtCoderDB情報が遅延最新化される）
# save() で指定ファイルにセーブする
# dfに計算したDataFrameが入る
# append(others) でたとえばBaseProfileとその他をマージできる

# user基本情報 country affiliation algo/heuristicsの最新レート を得る
class UsersProfile(Users):
    def __init__(self, df=None):
        super().__init__(df, filename='../out/users/profile.csv')

    @logging_time
    def update(self):
        db = AtCoderDB()
        db.add_rated_and_type('results')
        users = Users.from_db(db, 'results')
        users = users.agg({'country': 'last', 'affiliation': 'last'})
        db.filter('results', rated=1, inplace=True)
        a_rate = Users.from_db(db, 'results').filter({'type_': 'Algorithm'}).agg({'new_rate': ('last', 'a_rate')})
        h_rate = Users.from_db(db, 'results').filter({'type_': 'Heuristic'}).agg({'new_rate': ('last', 'h_rate')})
        self.df = Users.merge([users, a_rate, h_rate]).df
        self.df[['a_rate', 'h_rate']] = self.df[['a_rate', 'h_rate']].fillna(0).astype(int)

# 精進情報 accepted, rps, tee と累積diffを得る
class UsersShojin(Users):
    def __init__(self, df=None):
        super().__init__(df, filename='../out/users/shojin.csv')

    @logging_time
    def update(self):
        db = AtCoderDB()
        db.add_rated_and_type('submissions')
        # ユニークなAC提出をすべて抽出して、accepted、tee、diff累積 を計算
        db.filter('submissions', result='AC')
        db.drop_duplicates('submissions', ['user_id', 'problem_id'])
        db.add_diff_and_tee_in_submissions()
        accepted = Users.from_db(db, 'submissions').agg({'result': ('size', 'accepted')})
        tee_diff = Users.from_db(db, 'submissions').agg({'tee': 'sum', 'diff': 'sum'})
        # rpsは本家では「rated かつ 問題が2問以上のコンテスト」のpoint総計
        # ここではより正確に「rated かつ アルゴ」でフィルタしている
        rps = Users.from_db(db, 'submissions').filter({'rated': 1, 'type_': 'Algorithm'})\
            .agg({'point': ('sum', 'rps')})
        self.df = Users.merge([accepted, rps, tee_diff]).df
        self.df.fillna(0).round().astype(int)

# ユーザ単位での、Algoのrate更新履歴
class UsersARateHistory(Users):
    def __init__(self, df=None):
        super().__init__(df, filename='../out/users/a_rate_hist.csv')

    @logging_time
    def update(self):
        db = AtCoderDB()
        db.add_rated_and_type('results')
        self.df = Users.from_db(db, 'results').filter({'type_': 'Algorithm', 'user_rated': 1})\
            .df[['datetime', 'contest_id', 'old_rate', 'new_rate', 'perf']]

# ユーザ単位での、Heuristicのrate更新履歴
class UsersHRateHistory(Users):
    def __init__(self, df=None):
        super().__init__(df, filename='../out/users/h_rate_hist.csv')

    @logging_time
    def update(self):
        db = AtCoderDB()
        db.add_rated_and_type('results')
        self.df = Users.from_db(db, 'results').filter({'type_': 'Heuristic', 'user_rated': 1})\
            .df[['datetime', 'contest_id', 'old_rate', 'new_rate', 'perf']]

# 月間ユニークAC数(diffありのもの)(unique_ac)、左記の(diff-月初rate)累積(diff_sub_rate_sum)
# マージでの補完つき（rate記録無い月でもrateを入れる）
# *********** 工事中 ************

class UsersShojinExHistory(Users):
    def __init__(self, df=None):
        super().__init__(df, filename='../out/users/shojin_ex.csv')

    def update(self):
        logging.info('Computing shojin_ex ...')
        start_time = time.time()
        db = AtCoderDB()
        db.add_rated_and_type('submissions')
        db.filter('submissions', result='AC', type_='Algorithm', inplace=True)
        db.add_diff_and_tee_in_submissions()
        self.df = self.__class__.from_db(db, 'submissions', time_columns=('epoch_second', 'time'),
            resolution='month',
            filter={'type_': 'Algorithm', 'user_rated': 1},
            drop_duplicates=['time', 'problem_id'],
            size='accepted', agg={'tee': np.sum, 'diff': np.sum}).df
        duration = int(time.time() - start_time)
        logging.info(f'Computed shojin_ex in {duration}sec.')

# 定義されているクラスの情報を一斉にアップデートする
def main():
    profile = UsersProfile()
    profile.update()
    profile.save()
    shojin = UsersShojin()
    shojin.update()
    shojin.save()
    a_rate_hist = UsersARateHistory()
    a_rate_hist.update()
    a_rate_hist.save()
    h_rate_hist = UsersHRateHistory()
    h_rate_hist.update()
    h_rate_hist.save()
    #shojin_ex = UsersShojinExHistory()
    #shojin_ex.update()
    #shojin_ex.save()

if __name__ == '__main__':
    main()
