import requests
import json
import pandas as pd
import datetime



# data_header = {
#     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.104 Safari/537.36',
#     'Cookie': 'kbz_newcookie=1; kbzw__Session=rr4iblgsj1muvcj8ldse5v13p7; Hm_lvt_164fe01b1433a19b507595a43bf58262=1679282750,1679388637,1679391827,1679392007; Hm_lpvt_164fe01b1433a19b507595a43bf58262=1679392007; kbzw__user_login=7Obd08_P1ebax9aX2M7s2vDwlLOO4MLn7OzY69DVv6GUrausrpSpw6Wqpc2t0qeYqpqwrNfeltWRqa-lna2j2oOxjuzg19a-m5Ktrq6gqZ2YnaOiy9XQo5KmmK2srpupn6iDsY7MuNHVjL3Q7uLh1dqbrJCmgZ_O3ObF39jnmcO9mZ2nkKacl87c5peknJTxq52ijLjS5s3cztjarNnVo66ooKefrYKerL_LwMSNkM3d5NqJwNHazeWKl7rb6tDdxqOqppqnnKWSpJGXytTewuLKo66ooKefrQ..'
# }



# jsl data



# retrieve data
# data_response =  session.get(data_url, headers = try_header)

data_response = requests.get( "https://www.jisilu.cn/data/etf/etf_list/?___jsl=LST___t=1686030881875")

data = data_response.json()

td = datetime.datetime.today().date()
clean_data = []
for i in range(0,len(data['rows'])):
    clean_data.append(data['rows'][i]["cell"])






full = list(clean_data[0].keys())
t = td.strftime('%Y-%m-%d')
name_full = t + 'jsl.csv'

df_full = pd.DataFrame(clean_data)[full]
df_full.to_csv(name_full,index=None, encoding='utf_8_sig')





