import time
import logging
import numpy as np
from lib import AtCoderDB, Users

# Usersを継承して、代表的な分析を行う
#
# 共通的な使い方
# クラスオブジェクトを作ると、既存でセーブされていれば読み込む
# update() で最新情報にする（関連のAtCoderDB情報が遅延最新化される）
# save() で指定ファイルにセーブする
# dfに計算したDataFrameが入る
# merge(others) でたとえばBaseProfileとその他をマージできる

# user基本情報 country affiliation algo/heuristicsの最新レート を得る
class UserBaseProfiles(Users):
    def __init__(self, df=None):
        super().__init__(df, filename='../out/users/base_profiles.csv')

    def update(self):
        tail = lambda x: x.tail(1)
        db = AtCoderDB()
        db.add_rated_and_type('results')
        users = self.__class__.from_db(db, 'results', agg={'country': tail, 'affiliation': tail})
        db.filter('results', rated=1, inplace=True)
        a_rate = self.__class__.from_db(db, 'results', filter={'type_': 'Algorithm'},
            agg={'new_rate': (tail, 'a_rate')})
        h_rate = self.__class__.from_db(db, 'results', filter={'type_': 'Heuristic'},
            agg={'new_rate': (tail, 'h_rate')})
        self.df = self.__class__.merge([users, a_rate, h_rate]).df
        self.df[['a_rate', 'h_rate']] = self.df[['a_rate', 'h_rate']].fillna(0).astype(int)

# 精進情報 accepted, rps, tee と累積diffを得る
class UserShojins(Users):
    def __init__(self, df=None):
        super().__init__(df, filename='../out/users/shojins.csv')

    def update(self):
        db = AtCoderDB()
        db.add_rated_and_type('submissions')
        db.add_diff_and_tee_in_submissions()
        logging.info('Computing shojins ...')
        # ユニークなAC提出をすべて抽出して、accepted、tee、diff累積 を計算
        start_time = time.time()
        db.filter('submissions', result='AC', inplace=True)
        db.drop_duplicates('submissions', ['user_id', 'problem_id'], inplace=True)
        accepted_tee_diff = self.__class__.from_db(db, 'submissions', size='accepted',
            agg={'tee': np.sum, 'diff': np.sum})
        # rpsは本家では「rated かつ 問題が2問以上のコンテスト」のpoint総計
        # ここではより正確に「rated かつ アルゴ」でフィルタしている
        rps = self.__class__.from_db(db, 'submissions', filter={'rated': 1, 'type_': 'Algorithm'},
            agg={'point': (np.sum, 'rps')})
        self.df = self.__class__.merge([accepted_tee_diff, rps]).df.fillna(0).round().astype(int)
        duration = int(time.time() - start_time)
        logging.info(f'Computed shojins in {duration}sec.')

# 定義されているクラスの情報を一斉にアップデートする
def main():
    base_profile = UserBaseProfiles()
    base_profile.update()
    base_profile.save()
    shojins = UserShojins()
    shojins.update()
    shojins.save()

if __name__ == '__main__':
    main()