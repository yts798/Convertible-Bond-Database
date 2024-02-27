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
font = FontProperties(fname=r"simsun.ttf", size=14)  
matplotlib.rcParams['axes.unicode_minus'] =False
mpl.rcParams['font.sans-serif'] = ['SimHei']
tick_spacing = 4

# re-open
with open(DTBS_path, 'rb') as f:  
    DTBS = pickle.load(f)
    
def takeFifth(elem):
    return elem[4]

tdt = DTBS['D']['day'][-1]
td= DTBS['D']['day'][-1] 
dt = DTBS['D']['day'][-1]

re = []
for code in DTBS['A'].keys():
    if DTBS['A'][code][dt]['ia'] == 1 and DTBS['A'][code][dt]['qs30'] != 0:
        re.append((code, DTBS['B'][code]['cn'], DTBS['A'][code][dt]['qs'],DTBS['A'][code][dt]['qs15'],DTBS['A'][code][dt]['qs30']))

re.sort(key = takeFifth, reverse = True)




f = open(tdt+'-强赎.txt','w', encoding='utf-8')
f.write('今日('+td+')数据库强赎信息简报：\n')        
f.write('共有'+str(len(re))+'支转债近日满足过强赎条件。\n')


for one in re:
    f.write(one[0] + ' ' + one[1])
    f.write(' 今日是否满足条件：' + str(one[2]) + ', 最近15日满足条件计数：' + str(one[3])+ '/15')
    f.write(', 最近30日满足条件计数：' + str(one[4]) +'/30。\n')

f.close()