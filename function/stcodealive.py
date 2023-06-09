import pandas as pd
import numpy as np
import pickle
import time 
import os
from datetime import datetime
from operator import itemgetter
data_folder_path = 'C:\\Users\\goodluck\\Desktop\\DB'
raw_data_path =  "C:\\Users\\goodluck\\Desktop\\DB\\raw_data"
storage_path =  "C:\\Users\\goodluck\\Desktop\\DB\\database_storage"
daily_path =  "C:\\Users\\goodluck\\Desktop\\DB\\daily"
DTBS_path = os.path.join(storage_path, "DTBS.pkl")

# re-open
with open(DTBS_path, 'rb') as f:  
    DTBS = pickle.load(f)
    
    
cds = []
ld = DTBS['D']['day'][-1]
for cd in DTBS['A'].keys():
    if DTBS['A'][cd][ld]['ia'] == 1:
        cds.append(DTBS['B'][cd]['sc'])
        

df = pd.DataFrame(columns=cds)
df.to_csv(ld+'日市面转债对应正股代码.csv')    