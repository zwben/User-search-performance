import pandas as pd
from lxml import etree

import os
import pandas as pd
import numpy as np
from itertools import groupby

os.chdir('data_path')

df_rel = pd.read_csv('UsefulnessUserStudyData-master/relevance_annotation.tsv', index_col=[0], sep='\t',encoding='UTF-8')
df_sat = pd.read_csv('UsefulnessUserStudyData-master/task_satisfaction_annotation.tsv', index_col=[0], sep='\t',encoding='UTF-8')
df_sat['user-task'] = df_sat['userid'].astype(str) + '-' + df_sat['topic_num'].astype(str)
df_rel['query']=df_rel['query'].str.strip()
df_rel['url_avg'] = df_rel.groupby(['query','docno'])['relevance'].transform('mean')
rel_url_avg = df_rel.drop_duplicates(['docno'])
rel_dict = rel_url_avg.set_index('docno')['relevance'].to_dict()

data_features = ['studentID','task_id','session','query','dwelltime_query',
                 'dwelltime_doc','click','click_time','relevance',
                 'usefulness','action']

#%%
data = pd.DataFrame(columns=data_features)
parser = etree.XMLParser(recover=True, encoding='utf-8')
tree = etree.parse(r'SIGIR18_UserStudy\ComparisonUserStudy\search_logs.xml', parser=parser)
root = tree.getroot()

for s in root.iter('session'):
    data_s = {}
    data_s['session'] = s.attrib['num']
    data_s['user'] = s.attrib['userid']
    data_s['task'] = s[0].attrib['num']
    data_s['user-task'] = data_s['user'] + '-' + data_s['task']
    data_s['satisfaction'] = df_sat.loc[df_sat['user-task']==data_s['user-task']]['task_satisfaction_annotation'].values[0]
    query_info = []
    for interaction in s.iter('interaction'):
        if interaction.attrib['type'] == 'reformulate':
            query_dict = {}
            query_dict['query'] = interaction[0].text
            query_dict['starttime_query'] = float(interaction.attrib['starttime'])
            query_dict['starttime_act'] = []
            query_dict['dwelltime_doc'] = []
            query_dict['click'] = []
            query_dict['click_time'] = []
            query_dict['usefulness'] = []
            query_dict['action'] = []
            doc_relevance = []
            query_info.append(query_dict)
            
        if interaction.attrib['type'] == 'page':
            query_info[-1]['starttime_act'].append(float(interaction.attrib['starttime']))
            query_info[-1]['action'].append('PAGINATION')
        for result in interaction.iter('result'):
            if int(result[1].text) in rel_dict:
                doc_relevance.append([int(result.attrib['rank']),float(rel_dict[int(result[1].text)])])
        query_info[-1]['relevance'] = doc_relevance
        
        for click in interaction.iter('click'):
            if click is not None:
                clicked_rank = int(click[0].text)
                query_info[-1]['dwelltime_doc'].append(float(click.attrib['endtime']) - 
                                                   float(click.attrib['starttime']))
                query_info[-1]['click'].append(clicked_rank)
                query_info[-1]['click_time'].append(float(click.attrib['starttime']) - 
                                                    query_dict['starttime_query'])
                query_info[-1]['usefulness'].append([clicked_rank,int(click[2].attrib['score'])])
                query_info[-1]['starttime_act'].append(float(click.attrib['endtime']))
                query_info[-1]['action'].append('CLICK')
    query_info = [data_s|q for q in query_info]           
    data = pd.concat([data,pd.DataFrame(query_info,index=range(len(query_info)))], ignore_index=True)

#%%
data.to_csv('SIGIR18_UserStudy/ComparisonUserStudy/KDDext_rawfeatures.csv')                 

                
                
                
                