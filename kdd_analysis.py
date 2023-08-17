# -*- coding: utf-8 -*-
"""
Created on Mon Jan  3 15:16:35 2022

@author: Ben
"""
import os
import pandas as pd
import numpy as np
from itertools import groupby
from bs4 import BeautifulSoup as soup
from tqdm import tqdm

def split_a_list_at_zeros(L):
    return [list(g) for k, g in groupby(L, key=lambda x:x!=0) if k]

os.chdir('data_path')

df_log = pd.read_csv('KDD19_UserStudy/anno_log.csv', encoding='UTF-8')
df_rel = pd.read_table('KDD19_UserStudy/all_relevance_annotation.txt', encoding='UTF-8')
df_use = pd.read_csv('KDD19_UserStudy/anno_annotation.csv', encoding='UTF-8')
df_sat = pd.read_csv('KDD19_UserStudy/anno_querysatisfaction.csv', encoding='UTF-8')

df_results = pd.read_csv('KDD19_UserStudy/anno_searchresult.csv', encoding='UTF-8')
df_results['query'] = df_results['query'].str.strip()

df_log['query']=df_log['query'].str.strip()
df_rel['query']=df_rel['query'].str.strip()
df_use['query']=df_use['query'].str.strip()
df_sat['query']=df_sat['query'].str.strip()

df_log['session'] = df_log['studentID'].astype('string') + '-' + df_log['task_id'].astype('string')
df_use['session'] = df_use['studentID'].astype('string') + '-' + df_use['task_id'].astype('string')
df_sat['session'] = df_sat['studentID'].astype('string') + '-' + df_sat['task_id'].astype('string')

df_log['timestamp'] = df_log['content'].str.split('\t').str[0].str.replace('TIME=', '').astype('int64')
df_log.sort_values(['session', 'timestamp'], inplace=True)

sessions_list = list(df_log['session'].drop_duplicates().values)

df_rel['query_avg'] = df_rel.groupby(['query', 'rank','url'])['query_relevance'].transform('mean')
df_rel['task_avg'] = df_rel.groupby(['query', 'rank','url'])['task_relevance'].transform('mean')
df_rel_avg = df_rel.drop_duplicates(['query','rank','url']).copy()
df_rel_avg.sort_values(['task','query', 'rank'], inplace=True)

df_use.sort_values(['session','query','result_id'], inplace=True)

df_results['url'] = df_results['content'].transform(lambda x: soup(x, 'lxml').find('a')['href'])

url_list = list(set(df_rel_avg['url'].values))

url_list_use = list(set(df_use['result_url'].values))

for url in url_list_use:
    if url not in url_list:
        fuzz_record = df_use.loc[df_use['result_url']==url].copy()
        
        fuzz_query = fuzz_record['query'].values[0]
        fuzz_rank = fuzz_record['result_id'].values[0]
        
        correct_url = df_results.loc[(df_results['query']==fuzz_query) & 
                                     (df_results['result_id']==fuzz_rank)]['url'].values[0]
        
        df_use['result_url'] = df_use['result_url'].replace(url,correct_url)

# for s in sessions_list:
#     df_s = df_log[df_log['session']==s]
#     ts_s = list(df_s['timestamp'].values)
#     for i in range(1,len(ts_s)):
#         if int(ts_s[i])<int(ts_s[i-1]):
#             print(df_s[df_s['timestamp']==ts_s[i]]['id'])

df_log['info'] = df_log['content'].str.split(':').str[1].str.lstrip()

#%%
data_features = ['studentID','task_id','session','query','dwelltime_query',
                 'dwelltime_doc','click','click_time','relevance_q','relevance_t',
                 'usefulness','action','distance','satisfaction']
data = pd.DataFrame(columns=data_features)

for s in tqdm(sessions_list):
    log_session = df_log[df_log['session']==s]
    sat_session = df_sat[df_sat['session']==s]
    dict_s = {}
    dict_s['studentID'] = s.split('-')[0]
    dict_s['task_id'] = s.split('-')[1]
    dict_s['session'] = s
    dict_s['satisfaction'] = sat_session['score'].values[0]
    
    # query_list1 = list(log_session.loc[log_session['action']=='QUERY_REFORM','info']
    #                    .str.split('\t').str[1].str.replace('NEW_QUERY=', '').str.strip())
    query_list = list(sat_session['query'].values)
    
    # if query_list1 != query_list2:
    #     print (s)
    #     print (query_list1,'\n',query_list2)
    
    # dict_s['queries'] = []
    # dict_s['dwelltime_doc'] = []
    
    log_query = pd.DataFrame(columns=log_session.columns)
    old_query = ''
    dw_doc = []
    clc_doc = []
    clc_time = []
    dist = 0    
    for index, row in log_session.iterrows():
        query = row['query']
        within_query = True
        if query:
            if not old_query:
                old_query = query
            if query == old_query:
                log_query = pd.concat([log_query,pd.DataFrame([row],index=[0])],ignore_index=True)
                if row['action'] == 'OVER':
                    within_query = False
            else:
                within_query = False
                                
        elif row['action'] == 'OVER':
            within_query = False
        
        if within_query:
            if row['action'] == 'CLICK':
                if clc_doc:
                    if dw == 0:
                        del clc_doc[-1], clc_time[-1]
                clc_doc.append(row['info'].split('\t')[1].replace('result=',''))
                clc_time.append(row['timestamp'])
                dw = 0
                dw_doc.append(dw)
            if clc_doc:
                if row['action'] == 'JUMP_OUT':
                    dw_start = row['timestamp']
                elif row['action'] == 'JUMP_IN':
                    dw_end = row['timestamp']
                    dw += dw_end - dw_start
                    dw_start = dw_end
                    dw_doc.append(dw)         
        
        else:
            if old_query in query_list:
                
                # dict_s['queries'].append(old_query)
                dict_s['query'] = old_query
                query_time = list(log_query['timestamp'])
                dict_s['dwelltime_query'] = query_time[-1]-query_time[0]
                try:
                    dict_s['relevance_q'] = df_rel_avg.loc[df_rel_avg['query']==old_query,
                                                           ['rank','query_avg']].values.tolist()
                    dict_s['relevance_t'] = df_rel_avg.loc[df_rel_avg['query']==old_query,
                                                           ['rank','task_avg']].values.tolist()
                except IndexError:
                    dict_s['relevance_q'] = []
                    dict_s['relevance_t'] = []
                    
                try:
                    dict_s['usefulness'] = df_use.loc[(df_use['session']==s) 
                                                      & (df_use['query']==old_query),
                                                      ['result_id','score']].values.tolist()
                except IndexError:
                    dict_s['usefulness'] = []
                    
                dict_s['action'] = log_query['action'].values.tolist()
                
                scroll_df = log_query.loc[log_query["action"]=="SCROLL"]
                if len(scroll_df) > 0:
                    Sx = scroll_df['info'].str.split('\t').str[1].str.replace('x=', '').astype("float").values
                    Sy = scroll_df['info'].str.split('\t').str[2].str.replace('y=', '').astype("float").values
                    Ex = scroll_df['info'].str.split('\t').str[4].str.replace('x=', '').astype("float").values
                    Ey = scroll_df['info'].str.split('\t').str[5].str.replace('y=', '').astype("float").values
                    dist = sum(np.sqrt((Ex-Sx)**2 + (Ey-Sy)**2))
                
                
                # for i, r in log_query.iterrows():
                #     if r['action'] == 'CLICK':
                #         clc_doc.append(r['info'].split('\t')[1].replace('result=',''))
                #         clc_time.append(r['timestamp'])
                #         dw = 0
                #         dw_doc.append(dw)
                #     elif r['action'] == 'JUMP_OUT':
                #         dw_start = r['timestamp']
                #     elif r['action'] == 'JUMP_IN':
                #         dw_end = r['timestamp']
                #         dw += dw_end - dw_start
                #         dw_doc.append(dw)
                    # elif r['action'] == 'SCROLL':
                    #     Sx = float(r['info'].split('\t')[1].replace('x=', ''))
                    #     Sy = float(r['info'].split('\t')[2].replace('y=', ''))
                    #     Ex = float(r['info'].split('\t')[4].replace('x=', ''))
                    #     Ey = float(r['info'].split('\t')[5].replace('y=', ''))
                    #     dist += np.sqrt((Ex-Sx)**2 + (Ey-Sy)**2)
                dw_doc = split_a_list_at_zeros(dw_doc)
                dict_s['dwelltime_doc'] = [sum(item) for item in dw_doc]
                dict_s['click'] = clc_doc
                dict_s['click_time'] = [clc_t - query_time[0] for clc_t in clc_time]
                dict_s['distance'] = dist
                
                data = pd.concat([data,pd.DataFrame([dict_s],index=[0], columns=data_features)], ignore_index=True)
            old_query = query
            dw_doc = []
            clc_doc = []
            clc_time = []
            dist = 0
                            
#%%
data.to_csv('KDD19_UserStudy/kdd_rawfeatures.csv')                            
                            
            
        
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    