import os
import time
import numpy as np
from lib import AtCoderDB, Users

# user基本情報 country affiliation algo/heuristicsの最新レート を得る
class UserBaseProfiles(Users):
    def __init__(self, df=None):
        super().__init__(df, filename = '../out/users/base_profiles.csv')
        if df is not None:
            self.df = df
        elif os.path.isfile(self.filename):
            self.load()

    def update(self):
        tail = lambda x: x.tail(1)
        db = AtCoderDB()
        db.add_rated_and_type('results')
        users = Users.from_db(db, 'results', agg={'country': tail, 'affiliation': tail})
        db.filter('results', rated=1, inplace=True)
        a_rate = Users.from_db(db, 'results', filter={'type_': 'Algorithm'},
            agg={'new_rate': (tail, 'a_rate')})
        h_rate = Users.from_db(db, 'results', filter={'type_': 'Heuristic'},
            agg={'new_rate': (tail, 'h_rate')})
        self.df = Users.merge([users, a_rate, h_rate]).df
        self.df[['a_rate', 'h_rate']] = self.df[['a_rate', 'h_rate']].fillna(0).astype(int)

# 精進情報 accepted, rps, tee と累積diffを得る
class UserShojins(Users):
    def __init__(self, df=None):
        super().__init__(df, filename = '../out/users/shojins.csv')
        if df is not None:
            self.df = df
        elif os.path.isfile(self.filename):
            self.load()

    def update(self):
        db = AtCoderDB()
        db.add_rated_and_type('submissions')
        db.add_diff_and_tee_in_submissions()
        # rated algo acを抽出して、accepted、rated_point_sum、tee、diff累積 の計算
        db.logger.info('Computing shojins ...')

        start_time = time.time()
        db.filter('submissions', result='AC', inplace=True)
        db.drop_duplicates('submissions', ['user_id', 'problem_id'], inplace=True)
        accepted_tee_diff = Users.from_db(db, 'submissions', size='accepted',
            agg={'tee': np.sum, 'diff': np.sum})
        # rpsは本家では「rated かつ 問題が2問以上のコンテスト」のpoint総計
        # ここではより正確に「rated かつ アルゴ」としている
        rps = Users.from_db(db, 'submissions', filter={'rated': 1, 'type_': 'Algorithm'},
            agg={'point': (np.sum, 'rps')})
        self.df = Users.merge([accepted_tee_diff, rps]).df.fillna(0).round().astype(int)
        duration = int(time.time() - start_time)
        db.logger.info(f'Computed shojins in {duration}sec.')

def main():
    base_profile = UserBaseProfiles()
    base_profile.update()
    base_profile.save()
    shojins = UserShojins()
    shojins.update()
    shojins.save()

if __name__ == '__main__':
    main()
