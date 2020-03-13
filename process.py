# -*- coding: utf-8 -*-
"""
Created on Tue Mar 10 13:14:41 2020

@author: Administrator
"""

#——————————————————————处理数据————————————————————
#import datetime
#import numpy as np  
import pandas as pd  


#合并能耗和天气状况的函数
def process(building, wea):
    energy = pd.read_csv('energy consumption.csv', usecols=['timestamp', building]).dropna()
    energy['timestamp'] = pd.to_datetime(energy['timestamp'])    #转化为时间戳
    energy.rename(columns={building:'energy'}, inplace=True)    #改列名

    weather = pd.read_csv('./weather/' + wea)
    weather['timestamp'] = pd.to_datetime(weather['timestamp'])    #转化为时间戳
    weather['timestamp'] = pd.to_datetime(weather['timestamp'].apply(lambda x : x.strftime('%Y-%m-%d %H')))    #时间戳按小时取整
    weather.drop_duplicates(subset=['timestamp'], keep='first', inplace=True)    #去掉重复的时间戳
    weather['weekday'] = weather['timestamp'].apply(lambda x : x.isoweekday())    #生成周几
    weather['hour'] = weather['timestamp'].apply(lambda x : x.time().hour)    #生成小时

    data = pd.merge(energy, weather, how='inner', on='timestamp')
    
    return data

#主程序
building_data = pd.read_csv('building meta data.csv')
List_of_types = building_data['primaryspaceusage'].unique()
for types in List_of_types:
    names_files = building_data.loc[building_data['primaryspaceusage']==types]
    List_of_names = names_files['uid']
    Weather_files = names_files['newweatherfilename']
    DATA = pd.DataFrame()    #存储最终数据
    count = 0
    for building, weather in zip(List_of_names, Weather_files):
        DATA = DATA.append(process(building, weather), ignore_index=True, sort=False)
        count = count + 1
        print(count, building)
        DATA.to_csv('data of ' + types + '.csv')
        
DATA.to_csv('total data.csv')