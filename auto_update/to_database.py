import numpy as np
import pickle
import time 
import os
import requests
import json
import pandas as pd
import datetime
from datetime import datetime
from operator import itemgetter

data_folder_path = 'C:\\Users\\goodluck\\Desktop\\DB'
raw_data_path =  "C:\\Users\\goodluck\\Desktop\\DB\\raw_data"
storage_path =  "C:\\Users\\goodluck\\Desktop\\DB\\database_storage"
update_path =   "C:\\Users\\goodluck\\Desktop\\DB\\auto_update"
DTBS_path = os.path.join(storage_path, "DTBS.pkl")

def find_diff(list1,list2):
    bt = set(list1).intersection(set(list2)) 
    al = set(list1).union(set(list2))
    new = set(list1).difference(set(list2)) 
    die = set(list2).difference(set(list1))
#     print("union:" +str(len(A)))
#     print("intersect:" +str(len(B)))
#     print("only in list 1 " +str(len(C)))
#     print("only in list 2 " +str(len(D)))
    return bt ,al, new, die

def convert_code(ori_code, mkt):
    m = '.' + mkt[0:2].upper()
    ori_code = str(ori_code)
    l = len(ori_code)
    while l < 6:
        ori_code = '0' + ori_code
        l += 1
    return ori_code + m
    

# re-open
with open(DTBS_path, 'rb') as f:  
    DTBS = pickle.load(f)
    
    
tdd =datetime.today()

td = tdd.strftime('%Y-%m-%d')

name = td + '-light.csv'
name_full = td + '-full.csv'




new_df = pd.read_csv(name, encoding='utf-8')
new_df.set_index('bond_id', inplace=True)

new_full_df = pd.read_csv(name_full, encoding='utf-8')
new_full_df.set_index('bond_id', inplace=True)

new_code = [str(i) for i in list(new_df.index)]
new_key = []
for i in new_code:
    one = i + '.' + new_df.loc[int(i)]['market_cd'][0:2].upper()
    new_key.append(one)
old_key = [i for i in list(DTBS['B'].keys())]


bt, al, new, die = find_diff(new_key,old_key)

for code in bt:
    DTBS['A'][code][td] = dict()
    s_code = DTBS['B'][code]['sc']
    
    DTBS['B'][code]['sn'] = new_df.loc[int(code[0:6])]['stock_nm']
    
    DTBS['E'][s_code][td] = dict()
    DTBS['A'][code][td]['cpr'] = new_df.loc[int(code[0:6])]['premium_rt']
    DTBS['A'][code][td]['dp'] = new_df.loc[int(code[0:6])]['price']
    DTBS['A'][code][td]['dl'] = new_df.loc[int(code[0:6])]['premium_rt'] + new_df.loc[int(code[0:6])]['price']
    DTBS['A'][code][td]['ytm'] = new_df.loc[int(code[0:6])]['ytm_rt']
    DTBS['A'][code][td]['bl'] = new_df.loc[int(code[0:6])]['curr_iss_amt']
    DTBS['A'][code][td]['trt'] = new_df.loc[int(code[0:6])]['turnover_rt']
    DTBS['A'][code][td]['yl'] = new_df.loc[int(code[0:6])]['year_left']
    DTBS['A'][code][td]['csp'] = new_full_df.loc[int(code[0:6])]['convert_price']
    DTBS['A'][code][td]['ia'] = 1
    DTBS['A'][code][td]['xx'] = 0
    DTBS['A'][code][td]['hs'] = 0
    
    DTBS['B'][code]['sn'] = new_df.loc[int(code[0:6])]['stock_nm']
#     if code == '127075.SZ':
#         print(DTBS['E']['002455.SZ']['2023-04-21'])
    DTBS['E'][s_code][td]['cl'] = new_full_df.loc[int(code[0:6])]['sprice']
    DTBS['E'][s_code][td]['pb'] = new_full_df.loc[int(code[0:6])]['pb']
    
#     if code == '127075.SZ':
#         print(DTBS['E']['002455.SZ']['2023-04-21'])
#     if code == '127075.SZ':
#         print(new_full_df.loc[int(code[0:6])]['sprice'],  new_full_df.loc[int(code[0:6])]['pb'])
#         print(s_code, td, DTBS['E'][s_code][td])
#         print(DTBS['E']['002455.SZ']['2023-04-21'])
    DTBS['A'][code][td]['csv'] = ((100.0 / DTBS['A'][code][td]['csp']) *  DTBS['E'][s_code][td]['cl'])

    if DTBS['E'][s_code][td]['cl'] / DTBS['A'][code][td]['csp'] > 1.2987:
        DTBS['A'][code][td]['qs'] = 1
    else:
        DTBS['A'][code][td]['qs'] = 0
                
            
            
    idx = len(DTBS['D']['day'])
    idx15 = [i for i in range(idx-14, idx)]
    idx30 = [i for i in range(idx-29, idx)]
    s = DTBS['A'][code][td]['qs']
    for j in idx15:
        if DTBS['A'][code][DTBS['D']['day'][j]]['qs'] == 1:
            s += 1
        DTBS['A'][code][td]['qs15'] = s
    s = DTBS['A'][code][td]['qs']
    for j in idx30:                
        if DTBS['A'][code][DTBS['D']['day'][j]]['qs'] == 1:
            s += 1
        DTBS['A'][code][td]['qs30'] = s                



for code in new:
    DTBS['A'][code] = dict()
    
    DTBS['B'][code] = dict()
    DTBS['B'][code]['ipo'] = new_df.loc[int(code[0:6])]['list_dt']
    DTBS['B'][code]['dld'] = '2046-01-01'
    DTBS['B'][code]['cn'] = new_df.loc[int(code[0:6])]['bond_nm']
    DTBS['B'][code]['sc'] = convert_code(new_df.loc[int(code[0:6])]['stock_id'], new_df.loc[int(code[0:6])]['market_cd'])
    DTBS['B'][code]['sn'] = new_df.loc[int(code[0:6])]['stock_nm']
    DTBS['B'][code]['cat1'] = np.nan
    DTBS['B'][code]['cat2'] = np.nan 

    s_code = DTBS['B'][code]['sc']
    if not DTBS['E'].__contains__(s_code):
        DTBS['E'][s_code] = dict()
    if not DTBS['E'][s_code].__contains__(td):    
        DTBS['E'][s_code][td] = dict()
    for dt in DTBS['D']['day']:
        DTBS['A'][code][dt] = dict()
        if not DTBS['E'][s_code].__contains__(dt):  
            DTBS['E'][s_code][dt] = dict()
            DTBS['E'][s_code][dt]['cl'] = 0
            DTBS['E'][s_code][dt]['pb'] = 0
        DTBS['A'][code][dt]['cpr'] = np.nan
        DTBS['A'][code][dt]['dp'] = np.nan
        DTBS['A'][code][dt]['dl'] = 999        
        DTBS['A'][code][dt]['ytm'] = np.nan
        DTBS['A'][code][dt]['bl'] = np.nan
        DTBS['A'][code][dt]['trt'] = np.nan
        DTBS['A'][code][dt]['yl'] = np.nan
        DTBS['A'][code][dt]['csp'] = np.nan
        DTBS['A'][code][dt]['csv'] = np.nan
        DTBS['A'][code][dt]['ia'] = 0
        DTBS['A'][code][dt]['qs'] = 0
        DTBS['A'][code][dt]['qs15'] = 0
        DTBS['A'][code][dt]['qs30'] = 0
        DTBS['A'][code][dt]['xx'] = 0
        DTBS['A'][code][dt]['hs'] = 0


    
   
    DTBS['A'][code][td] = dict()
    s_code = DTBS['B'][code]['sc']
    DTBS['E'][s_code][td] = dict()
    DTBS['A'][code][td]['cpr'] = new_df.loc[int(code[0:6])]['premium_rt']
    DTBS['A'][code][td]['dp'] = new_df.loc[int(code[0:6])]['price']
    DTBS['A'][code][td]['dl'] = new_df.loc[int(code[0:6])]['premium_rt'] + new_df.loc[int(code[0:6])]['price']
    DTBS['A'][code][td]['ytm'] = new_df.loc[int(code[0:6])]['ytm_rt']
    DTBS['A'][code][td]['bl'] = new_df.loc[int(code[0:6])]['curr_iss_amt']
    DTBS['A'][code][td]['trt'] = new_df.loc[int(code[0:6])]['turnover_rt']
    DTBS['A'][code][td]['yl'] = new_df.loc[int(code[0:6])]['year_left']
    DTBS['A'][code][td]['csp'] = new_full_df.loc[int(code[0:6])]['convert_price']

    DTBS['A'][code][td]['ia'] = 1

    DTBS['A'][code][td]['xx'] = 0
    DTBS['A'][code][td]['hs'] = 0
    
    
       
    
    DTBS['E'][s_code][td]['cl'] = new_full_df.loc[int(code[0:6])]['sprice']
    DTBS['E'][s_code][td]['pb'] = new_full_df.loc[int(code[0:6])]['pb']
    DTBS['A'][code][td]['csv'] = ((100.0 / DTBS['A'][code][td]['csp']) *  DTBS['E'][s_code][td]['cl'])
    
    
    if DTBS['E'][s_code][td]['cl'] / DTBS['A'][code][td]['csp'] > 1.2987:
        DTBS['A'][code][td]['qs'] = 1
        DTBS['A'][code][td]['qs15'] += 1
        DTBS['A'][code][td]['qs30'] += 1
    else:
        DTBS['A'][code][td]['qs'] = 0
        DTBS['A'][code][td]['qs15'] = min(0, DTBS['A'][code][td]['qs15']-1)
        DTBS['A'][code][td]['qs30'] = min(0, DTBS['A'][code][td]['qs30']-1)
                
        


for code in die:
    DTBS['A'][code][td] = dict()
    s_code = DTBS['B'][code]['sc']

    DTBS['A'][code][td]['cpr'] = np.nan
    DTBS['A'][code][td]['dp'] = np.nan
    DTBS['A'][code][td]['dl'] = 999
    DTBS['A'][code][td]['ytm'] = np.nan
    DTBS['A'][code][td]['bl'] = np.nan
    DTBS['A'][code][td]['trt'] = np.nan
    DTBS['A'][code][td]['yl'] = np.nan
    DTBS['A'][code][td]['csp'] = np.nan
    DTBS['A'][code][td]['csv'] = np.nan                              
    DTBS['A'][code][td]['ia'] = 0
    DTBS['A'][code][td]['qs'] = 0
    DTBS['A'][code][td]['qs15'] = 0
    DTBS['A'][code][td]['qs30'] = 0
    DTBS['A'][code][td]['xx'] = 0
    DTBS['A'][code][td]['hs'] = 0
    DTBS['B'][code]['dld'] = td
    if not DTBS['E'][s_code].__contains__(td):
        DTBS['E'][s_code][td] = dict()
        DTBS['E'][s_code][td]['cl'] = 0
        DTBS['E'][s_code][td]['pb'] = 0

  

if td not in DTBS['D']['day']:

    DTBS['D']['day'].append(td)
    if tdd.weekday() == 0:
        DTBS['D']['mon'].append(td)
    elif tdd.weekday() == 1:
        DTBS['D']['tue'].append(td)
    if tdd.weekday() == 2:
        DTBS['D']['wed'].append(td)
    if tdd.weekday() == 3:
        DTBS['D']['thu'].append(td)
    if tdd.weekday() == 4:
        DTBS['D']['fri'].append(td)
    
index = requests.get('https://www.jisilu.cn/data/idx_performance/list/flex_idx')
index_dict = index.json()
prc = ''
for term in index_dict['rows']:
    if term['id'] == '000832':
        prc = term['cell']['price']
        break

DTBS['C']['zi'][td] = np.float64(prc)

f = open(td+'-log.txt','a', encoding='utf-8')
f.write('今日('+td+')共有'+ str(len(new))+'支新债上市'+',目前市面上活跃转债数量为'+str(len(new_key))+'支\n')
f.write('今日转股价变化监控：\n')
ystd = DTBS['D']['day'][-2]
for cd in bt:
    if DTBS['A'][cd][ystd]['csp'] != DTBS['A'][cd][td]['csp']:
#         f.write('转债 ' + cd + ' ' + DTBS['B'][cd]['cn'] + '转股价昨日为'+ str(round(DTBS['A'][cd][ystd]['csp'],2)) +',今日为'+ str(round(DTBS['A'][cd][td]['csp'],2)) + ',变化幅度为' + str(round(100*(DTBS['A'][cd][td]['csp'] -  DTBS['A'][cd][ystd]['csp'])/ DTBS['A'][cd][ystd]['csp']),2) +'%\n')
         f.write(cd + ' ' + DTBS['B'][cd]['cn'] + '转股价昨日为'+ str(round(DTBS['A'][cd][ystd]['csp'],2)) +',今日为'+ str(round(DTBS['A'][cd][td]['csp'],2))+ ',变化幅度为' + str(round((((DTBS['A'][cd][td]['csp'] -  DTBS['A'][cd][ystd]['csp'])/DTBS['A'][cd][ystd]['csp']) * 100),2)) +'%\n')
    
f.close()
# save
    
f_save = open(DTBS_path, 'wb')
pickle.dump(DTBS, f_save)
f_save.close()

