import requests
import json
import pandas as pd
import numpy as np
import datetime

def single_get_first(unicode1):
    str1 = unicode1.encode('gbk')
    try:
        ord(str1)
        return str1
    except:
        asc = str1[0] * 256 + str1[1] - 65536
        if asc >= -20319 and asc <= -20284:
            return 'a'
        if asc >= -20283 and asc <= -19776:
            return 'b'
        if asc >= -19775 and asc <= -19219:
            return 'c'
        if asc >= -19218 and asc <= -18711:
            return 'd'
        if asc >= -18710 and asc <= -18527:
            return 'e'
        if asc >= -18526 and asc <= -18240:
            return 'f'
        if asc >= -18239 and asc <= -17923:
            return 'g'
        if asc >= -17922 and asc <= -17418:
            return 'h'
        if asc >= -17417 and asc <= -16475:
            return 'j'
        if asc >= -16474 and asc <= -16213:
            return 'k'
        if asc >= -16212 and asc <= -15641:
            return 'l'
        if asc >= -15640 and asc <= -15166:
            return 'm'
        if asc >= -15165 and asc <= -14923:
            return 'n'
        if asc >= -14922 and asc <= -14915:
            return 'o'
        if asc >= -14914 and asc <= -14631:
            return 'p'
        if asc >= -14630 and asc <= -14150:
            return 'q'
        if asc >= -14149 and asc <= -14091:
            return 'r'
        if asc >= -14090 and asc <= -13119:
            return 's'
        if asc >= -13118 and asc <= -12839:
            return 't'
        if asc >= -12838 and asc <= -12557:
            return 'w'
        if asc >= -12556 and asc <= -11848:
            return 'x'
        if asc >= -11847 and asc <= -11056:
            return 'y'
        if asc >= -11055 and asc <= -10247:
            return 'z'
        return ''
 
 
def getpinyin(string):
    if string == None:
        return None
    lst = list(string)
    charLst = []
    for l in lst:
        charLst.append(single_get_first(l))
    return ''.join(charLst)
 
    

data_response = requests.get( "https://www.swsresearch.com/institute-sw/api/index_publish/current/?page=1&page_size=50&indextype=%E4%B8%80%E7%BA%A7%E8%A1%8C%E4%B8%9A")

data = data_response.json()

alll = []
allnames = []
for one in data['data']['results']:
    code = one['swindexcode']
    nm = one['swindexname']
    allnames.append(nm)
    cl = one['l8']
    amc = one['l5']
    amq = one['l11']
    op = one['l4']
    lp = one['l7']
    hp = one['l6']
    alll.append([nm, code, op, cl, lp, hp, amc, amq])

alll = np.array(alll)
idx = ['name', 'code', 'open', 'close', 'low', 'high', 'amount', 'number']
allst = [getpinyin(i) for i in allnames]
sw_df = pd.DataFrame(alll.T, columns=allst, index=idx)
sw_df.to_csv(datetime.date.today().strftime('%Y-%m-%d') + '-swid.csv', encoding='utf-8_sig')