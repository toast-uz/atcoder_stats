from lib import SubmitData, Transform

transform_language = Transform().language
transform_axc = Transform().axc
transform_abrgc = Transform().abrgc
transform_ahc = Transform().ahc

submit = SubmitData().filter('contest_id', lambda x: transform_axc(x) != '')
abrgc = submit.filter('contest_id', lambda x: transform_abrgc(x) != '')
ahc = submit.filter('contest_id', lambda x: transform_ahc(x) != '')

abrgc_lang = abrgc.count('language', transform=transform_language, ratio=True)
ahc_lang = ahc.count('language', transform=transform_language, ratio=True)

print('abc/arc/agc_lang')
print(abrgc_lang[:20])
print('ahc_lang')
print(ahc_lang[:20])
