# -*- coding: utf-8 -*-
"""
Created on Fri Jan 21 14:51:42 2022

@author: Ben
"""
import pandas as pd
import xml.etree.ElementTree as ET

import os
import pandas as pd
import numpy as np
from itertools import groupby

os.chdir('data_path')

ju_df = pd.read_csv('track2014/judgments.txt', names = ["task","empty","result_id","score"], delimiter = " ")
ju_df['score_avg'] = ju_df.groupby(['task','result_id'])['score'].transform('mean')
ju_df = ju_df.drop_duplicates('result_id')
rel_dict = ju_df.set_index('result_id')['score_avg'].to_dict()


data_features = ['studentID','task_id','session','query','dwelltime_query',
                 'dwelltime_doc','click','click_time','relevance',
                 'usefulness','action']

#%%
data = pd.DataFrame(columns=data_features)

tree = ET.parse('track2014/sessiontrack2014.xml')
root = tree.getroot()

for s in root.iter('session'):
    data_s = {}
    data_s['session'] = s.attrib['num']
    data_s['user'] = s.attrib['userid']
    data_s['task'] = s[0].attrib['num']
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
            if result[1].text in rel_dict:
                doc_relevance.append([int(result.attrib['rank']),float(rel_dict[result[1].text])])
        query_info[-1]['relevance'] = doc_relevance
        
        for click in interaction.iter('click'):
            if click is not None:
                clicked_rank = int(click[0].text)
                query_info[-1]['dwelltime_doc'].append(float(click.attrib['endtime']) - 
                                                   float(click.attrib['starttime']))
                query_info[-1]['click'].append(clicked_rank)
                query_info[-1]['click_time'].append(float(click.attrib['starttime']) - 
                                                    query_dict['starttime_query'])
                query_info[-1]['starttime_act'].append(float(click.attrib['endtime']))
                query_info[-1]['action'].append('CLICK')
    for currentquery in s.iter('currentquery'):
        if currentquery is not None:
            data_s['endtime_session'] = float(currentquery.attrib['starttime'])
    query_info = [data_s|q for q in query_info]           
    data = data.append(query_info, ignore_index=True)
#%%
data.to_csv('track2014/track2014_rawfeatures.csv')                 
