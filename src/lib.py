# 生情報ダウンロード〜保管
# 保管した情報の読み出し用ユーティリティ

import pandas as pd
import re

# Submitデータ
# 初期ダウンロード（手動）
# https://s3-ap-northeast-1.amazonaws.com/kenkoooo/submissions.csv.gz
# in/submissions.csv.gz として保存

FILENAME_SUBMISSIONS = '../in/submissions.csv.gz'

class SubmitData:
    def __init__(self, df=None):
        if df is None:
            print(f'Waite a minute for loading {FILENAME_SUBMISSIONS} ...', flush=True, end='')
            self.df = pd.read_csv(FILENAME_SUBMISSIONS).dropna()
            print(f' done.')
        else:
            self.df = df

    def print(self):
        print(self.df)

    # 特定の列でフィルタする（func f がTrueで抽出）
    def filter(self, column, f):
        return SubmitData(self.df[self.df[column].apply(f)])

    # 特定の列をカウントする、名寄せ変換オプション、多い純からソートされる
    # 返り値は1列のdf（行ラベルがcolumn指定したもの）であるが、ratioありなら比率の列もつく
    def count(self, column, *, transform=lambda x: x, ratio=False, ascending=False):
        tmp = pd.DataFrame(pd.DataFrame(self.df[column].apply(transform))
                           .groupby(column).size().sort_values(ascending=ascending),
                           columns=['count'])
        if ratio:
            tmp['ratio'] = tmp['count'] / tmp['count'].sum()
        return tmp

# Submitデータ
# 追加ダウンロード
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

# Submitデータ 読み出しスクリプト


# コンテストデータ、種別(A/H/Rated)やdurationを知りたい
# https://kenkoooo.com/atcoder/resources/contests.json を取得
# in/contests.json と比較
# 存在していないコンテスト分を https://atcoder.jp/contests/archive から取る
# こちらはページを落として、スクレイピングする（）
# 上記に存在しない場合、https://atcoder.jp/contests/archive?page={} を取る
# 以上の結果をマージして in/contests.json に保管

# 個々のコンテストのresultsデータ（パフォやレートをとる）
# in/contests.jsonをもとに、まだin/に存在していないコンテストを抽出
# https://atcoder.jp/contests/コンテスト名/results/json をin/にダウンロード
#   in/コンテスト名.json としてそのまま保存



# 使わないAPI
# https://atcoder.jp/users/ユーザ名/history/json
# https://atcoder.jp/contests/コンテスト名/standings/json


# 各種の名寄せスクリプト

# 言語名の名寄せ
# 空白の前、かつ末尾の数値は取る

class Transform:
    def __init__(self):
        # 言語名の名寄せ
        pattern = '^[A-Za-z\+\#]+'
        self.re_pattern_lang = re.compile(pattern)
        # コンテスト名寄せ
        pattern = 'a[brgh]c\d\d\d'
        self.re_pattern_axc = re.compile(pattern)
        pattern = 'a[brg]c\d\d\d'
        self.re_pattern_abrgc = re.compile(pattern)
        pattern = 'ahc\d\d\d'
        self.re_pattern_ahc = re.compile(pattern)
    # 言語名の名寄せ
    def language(self, x):
        tmp = self.re_pattern_lang.match(x)
        return tmp.group() if tmp is not None else ''
    # コンテスト名寄せ
    def axc(self, x):
        tmp = self.re_pattern_axc.match(x)
        return tmp.group() if tmp is not None else ''
    def abrgc(self, x):
        tmp = self.re_pattern_abrgc.match(x)
        return tmp.group() if tmp is not None else ''
    def ahc(self, x):
        tmp = self.re_pattern_ahc.match(x)
        return tmp.group() if tmp is not None else ''

# 大学名の名寄せ
