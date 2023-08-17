import os
import pandas as pd
import numpy as np
from itertools import groupby
from bs4 import BeautifulSoup
import re

os.chdir('data_path')

def Merge(dict1, dict2):
    return(dict2.update(dict1))

relevance_dict = {}
with open(r'SearchSuccess\WWW18_Search_Success_Dataset\Main_Annotation.txt') as f:
    lines = f.readlines()
    for line in lines:
        line = line.strip()
        docno = line[0:5]
        if docno not in relevance_dict:
            relevance_dict[docno] = [int(line[-3])]
        else:
            relevance_dict[docno].append(int(line[-3]))
relevance_dict = {k: sum(relevance_dict[k])/len(relevance_dict[k]) for k in relevance_dict.keys()}

url_dict = {}
with open(r'SearchSuccess\WWW18_Search_Success_Dataset\Url_index.txt') as f:
    lines = f.readlines()
    for line in lines:
        components = line.split('\t')
        url_dict[components[1]] = components[0]
        

#%%
data_features = ['user','task','session','query','dwelltime_query',
                 'dwelltime_doc','click','click_time','relevance',
                 'usefulness','action',]
data = pd.DataFrame(columns=data_features)

with open(r'SearchSuccess\WWW18_Search_Success_Dataset\Search_Log.txt', encoding='UTF-8') as f:
    lines = f.readlines()
    for line in lines:
        line = line.strip()
        if line == '#session:':
            dict_s = {}
            query_info = []
            QA = []
            continue
        if line.startswith('user'):
            user_line = re.split('\t|:', line)
            dict_s['user'] = user_line[1]
            dict_s['task'] = user_line[3]
            dict_s['session'] = user_line[1] + '-' + user_line[3]
            
            result_folder = (r'SearchSuccess\WWW18_Search_Success_Dataset\SearchResults\task' + 
                             dict_s['task'] + '\\' +  dict_s['user'])
            query_folders = [q for q in os.listdir(result_folder) if q.startswith('query')]
            query_folders.sort()
            
            query_n = 0
            continue
        if line.startswith('pre_difficulty'):
            review_line = re.split('\t|:', line)
            dict_s['pre_difficulty'] = review_line[1]
            dict_s['pre_interest'] = review_line[3]
            dict_s['pre_knowledge'] = review_line[5]
            continue
        if line.startswith('pre_answer'):
            dict_s['pre_answer'] = line.split(':')[1]
            continue
        if re.findall(r'[\u4e00-\u9fff]+', line):
            if not line.startswith('query'):
                QA.append(line)
            else:
                query_dict = {}
                query_line = re.split('\t|:', line)
                query_dict['query'] = query_line[1]
                query_dict['dwelltime_query'] = int(query_line[5]) - int(query_line[3])
                query_dict['dwelltime_doc'] = []
                query_dict['clicked'] = []
                query_dict['click'] = []
                query_dict['click_time'] = []
                query_dict['usefulness'] = []
                query_dict['action'] = []
                                
                query_results = result_folder + '\\' + query_folders[query_n]
                pages = [p for p in os.listdir(query_results) 
                         if p.endswith('.html') and 'LP' not in p]
                doc_list = []
                doc_relevance = []
                rank = 0
                for page in pages:
                    query_dict['action'].append('PAGINATION')
                    with open(query_results + '\\' + page, encoding='UTF-8') as p:
                        soup = BeautifulSoup(p, 'html.parser')
                        for doc in soup.find_all('li',{'class':'b_algo'}):
                            doc_url = doc.find('a').get('href')
                            doc_list.append(doc_url)
                            if doc_url in url_dict:
                                doc_relevance.append([rank,relevance_dict[url_dict[doc_url]]])
                            rank += 1
                query_dict['relevance'] = doc_relevance
                
                query_info.append(query_dict)            
                query_n += 1
            continue
        if line.startswith('click_url'):
            doc_info = {}
            click_line = line.split('\t')
            doc_info['url'] = click_line[0].replace('click_url:','')
            doc_info['dwelltime_doc'] = (int(click_line[2].split(':')[1]) - 
                                         int(click_line[1].split(':')[1]))
            doc_info['click_time'] = (int(click_line[1].split(':')[1]) - 
                                      int(query_line[3]))
            doc_info['usefulness'] = int(click_line[3].split(':')[1])
            doc_info['relevance'] = relevance_dict[url_dict[doc_info['url']]]
            
            try:
                doc_info['rank'] = doc_list.index(doc_info['url'])
            except ValueError:
                doc_info['rank'] = -1
            
            query_info[-1]['dwelltime_doc'].append(doc_info['dwelltime_doc'])
            query_info[-1]['click'].append(doc_info['rank'])
            query_info[-1]['click_time'].append(doc_info['click_time'])
            query_info[-1]['usefulness'].append([doc_info['rank'],doc_info['usefulness']])
            query_info[-1]['action'].append('CLICK')
            
            query_info[-1]['clicked'].append(doc_info)
            continue
        if line.startswith('post_answer'):
            dict_s['post_answer'] = line.split(':')[1]
            continue
        if line.startswith('session_satisfaction'):
            dict_s['satisfaction'] = line.split(':')[1]
            
            
            query_info = [dict_s|q for q in query_info]
            data = data.append(query_info, ignore_index=True)
            
#%%            
data.to_csv('SearchSuccess/SS_rawfeatures.csv')                 
            
        
