import requests
import json
import pandas as pd
import datetime

login_header = {
    "origin": "https://www.jisilu.cn",
    "Referer": "https://www.jisilu.cn/account/login/",
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36 Edg/111.0.1661.44"
}
login_url = 'https://www.jisilu.cn/webapi/account/login_process/'
login_info = {
   "return_url": "https://www.jisilu.cn/",
   "user_name": "d0cb8eb409b54ff8444593da78e20495",
   "password": "09e373cb61c6dc7d2f9ca59fdc996067",
   "aes": 1,
   "auto_login": 0
}



session = requests.session()

session.post(login_url, data = login_info, headers = login_header)
data_header = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36 Edg/111.0.1661.44'
}

# data_header = {
#     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.104 Safari/537.36',
#     'Cookie': 'kbz_newcookie=1; kbzw__Session=rr4iblgsj1muvcj8ldse5v13p7; Hm_lvt_164fe01b1433a19b507595a43bf58262=1679282750,1679388637,1679391827,1679392007; Hm_lpvt_164fe01b1433a19b507595a43bf58262=1679392007; kbzw__user_login=7Obd08_P1ebax9aX2M7s2vDwlLOO4MLn7OzY69DVv6GUrausrpSpw6Wqpc2t0qeYqpqwrNfeltWRqa-lna2j2oOxjuzg19a-m5Ktrq6gqZ2YnaOiy9XQo5KmmK2srpupn6iDsY7MuNHVjL3Q7uLh1dqbrJCmgZ_O3ObF39jnmcO9mZ2nkKacl87c5peknJTxq52ijLjS5s3cztjarNnVo66ooKefrYKerL_LwMSNkM3d5NqJwNHazeWKl7rb6tDdxqOqppqnnKWSpJGXytTewuLKo66ooKefrQ..'
# }



# jsl data
data_url = "https://www.jisilu.cn/data/cbnew/cb_list_new/?___jsl=LST___t=1637410410639"



# retrieve data
# data_response =  session.get(data_url, headers = try_header)
data_response = session.get(data_url, headers=data_header)


data = data_response.json()

td = datetime.datetime.today().date()
clean_data = []
for i in range(0,len(data['rows'])):
    if data['rows'][i]["cell"]['bond_id'] != '132018':
        if data['rows'][i]["cell"]['list_dt'] != None:
            ipo_dt = datetime.datetime.strptime(data['rows'][i]["cell"]['list_dt'], '%Y-%m-%d').date()
            if ipo_dt <= td:
                clean_data.append(data['rows'][i]["cell"])
                
                
     
    

            
full = list(clean_data[0].keys())
light = ['bond_id', 'bond_nm', 'list_dt', 'price','premium_rt','ytm_rt','curr_iss_amt','turnover_rt','year_left','stock_id','stock_nm', 'market_cd']
t = td.strftime('%Y-%m-%d')
name_full = t + '-full.csv'
name_light = t + '-light.csv'

df_full = pd.DataFrame(clean_data)[full]
df_light = pd.DataFrame(clean_data)[light]
print(len(data['rows']))
print(len(clean_data))
df_full.to_csv(name_full,index=None, encoding='utf_8_sig')
df_light.to_csv(name_light,index=None, encoding='utf_8_sig')




