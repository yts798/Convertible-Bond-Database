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
DCBS_path = os.path.join(storage_path, "DCBS.pkl")


def convert_code(ori_code, mkt):
    m = '.' + mkt[0:2].upper()
    ori_code = str(ori_code)
    l = len(ori_code)
    while l < 6:
        ori_code = '0' + ori_code
        l += 1
    return ori_code + m
    

# re-open
with open(DCBS_path, 'rb') as f:  
    DCBS = pickle.load(f)
    
    
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

DCBS[td] = dict()

for code in new_key:
    
    
    
    DCBS[td][code] = dict()
    
    
    
    DCBS[td][code]['cpr'] = new_df.loc[int(code[0:6])]['premium_rt']
    DCBS[td][code]['dp'] = new_df.loc[int(code[0:6])]['price']
    DCBS[td][code]['dl'] = new_df.loc[int(code[0:6])]['premium_rt'] + new_df.loc[int(code[0:6])]['price']
    DCBS[td][code]['ytm'] = new_df.loc[int(code[0:6])]['ytm_rt']
    DCBS[td][code]['bl'] = new_df.loc[int(code[0:6])]['curr_iss_amt']
    DCBS[td][code]['trt'] = new_df.loc[int(code[0:6])]['turnover_rt']
    DCBS[td][code]['yl'] = new_df.loc[int(code[0:6])]['year_left']
    DCBS[td][code]['csp'] = new_full_df.loc[int(code[0:6])]['convert_price']
    DCBS[td][code]['ia'] = 1
    DCBS[td][code]['ipo'] = new_df.loc[int(code[0:6])]['list_dt']
    DCBS[td][code]['cn'] = new_df.loc[int(code[0:6])]['bond_nm']
    DCBS[td][code]['sc'] = convert_code(new_df.loc[int(code[0:6])]['stock_id'], new_df.loc[int(code[0:6])]['market_cd'])
    DCBS[td][code]['sn'] = new_df.loc[int(code[0:6])]['stock_nm']
    DCBS[td][code]['cat1'] = np.nan
    DCBS[td][code]['cat2'] = np.nan 

    DCBS[td][code]['cl'] = new_full_df.loc[int(code[0:6])]['sprice']
    DCBS[td][code]['pb'] = new_full_df.loc[int(code[0:6])]['pb']
    
    

# save
    
f_save = open(DCBS_path, 'wb')
pickle.dump(DCBS, f_save)
f_save.close()



