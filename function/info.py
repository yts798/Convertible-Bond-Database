# set up
import pandas as pd
import numpy as np
import pickle
import time 
import os
import matplotlib.pyplot as plt
import datetime
from operator import itemgetter
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from pylab import *
from matplotlib.font_manager import FontProperties  
data_folder_path = 'C:\\Users\\goodluck\\Desktop\\DB'
raw_data_path =  "C:\\Users\\goodluck\\Desktop\\DB\\raw_data"
storage_path =  "C:\\Users\\goodluck\\Desktop\\DB\\database_storage"
DTBS_path = os.path.join(storage_path, "DTBS.pkl")
with open(DTBS_path, 'rb') as f:  
    DTBS = pickle.load(f)
# tdt = datetime.date.today().strftime('%Y-%m-%d')
tdt = DTBS['D']['day'][-1]
td= DTBS['D']['day'][-1] 
f = open(tdt+'-info.txt','w', encoding='utf-8')
f.write('今日('+td+')数据库信息简报：\n')

total = 0
codes = []
codes_his = []
for cd in DTBS['A'].keys():
    if DTBS['A'][cd][tdt]['ia'] == 1:
        total += 1
        codes.append(DTBS['B'][cd]['sc'])
    codes_his.append(DTBS['B'][cd]['sc'])
codes = list(set(codes))
codes_his = list(set(codes_his))
sthis = len(codes_his)
stnow = len(codes)
hstr = len(list(DTBS['A'].keys()))
f = open(tdt+'-info.txt','w', encoding='utf-8')
f.write('今日('+td+')数据库信息简报：\n')        
f.write('已存储市面上'+str(total)+'支活跃转债信息, 及对应'+str(stnow)+'支活跃正股信息。\n')
f.write('已存储历史上'+str(hstr)+'支转债信息, 及对应'+str(sthis)+'支正股信息。\n')
f.write('已存储'+DTBS['D']['day'][0]+'至'+DTBS['D']['day'][-1]+'日信息, 共有' + str(len(DTBS['D']['day']))+'天。\n')
f.write('A区（转债每日信息）已存储'+str(len(DTBS['A']['110088.SH']['2023-06-09'].keys()))+'个指标。\n')   
f.write('B区（转债基本信息）已存储'+str(len(DTBS['B']['110088.SH'].keys()))+'个指标。\n')    
f.write('C区（指数每日信息）已存储'+str(len(DTBS['C'].keys()))+'个指数。\n')    
f.write('E区（转债对应正股每日信息）已存储'+str(len(DTBS['E']['600985.SH']['2023-06-09'].keys()))+'个指数。\n')  
f.write('F区（转债对应正股季度信息）已存储'+str(len(DTBS['F']['600985.SH']['2023-03'].keys()))+'个指数。\n')    
f.write('G区（转债对应正股基本信息）已存储'+str(len(DTBS['G']['600985.SH'].keys()))+'个指数。\n')    
f.close()