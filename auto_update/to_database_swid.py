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


# re-open
with open(DTBS_path, 'rb') as f:  
    DTBS = pickle.load(f)
    
    
tdd =datetime.today()

td = tdd.strftime('%Y-%m-%d')

name = td + '-swid.csv'

name = '2023-06-09-swid.csv'




new_df = pd.read_csv(name, encoding='utf-8', index_col = 0)

ids = list(new_df.columns)
for i in ids:
    DTBS['C'][ids][td] = new_df[i]['close']
    
f_save = open(DTBS_path, 'wb')
pickle.dump(DTBS, f_save)
f_save.close()

