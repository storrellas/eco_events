
# coding: utf-8

# In[1]:

#script updates all eco exclude and eco_significant after updates 


# In[2]:

import requests
import pandas as pd
from bs4 import *
import datetime
import time 
from engine_code import *
from pandas.tseries.offsets import BDay


# In[14]:

def update_eco_lists():
    
    G10 = ['USD','EUR','JPY','GBP','AUD','NZD','CAD','CHF','NOK','SEK','CNY']

    for i in G10:
        ccy = i
    
        #*** update exclude list section ***
        
        print("Update exclude list -> "+ccy)
        check1 = pd.read_sql('select id, event, check_1 from eco_events where region = "%s"' %(ccy), engine)
        check2 = pd.read_sql('select event from eco_exclude where region = "%s"' %(ccy), engine)
        #get data to check if its inlcuded and get ids to update

        check1['check_1'] =  check1['event'].isin(check2['event'])
        #pick values which need to be updated 
        
        update = check1[check1['check_1']==True]
        #list to update
        
        for y in update.id:
            engine.execute("UPDATE eco_events SET check_1 = 1 WHERE id = %s " %(y))
            
            
        #***  now update significant list section  ***
        
        print("Update significant list -> "+ccy)
        s_check1 = pd.read_sql('select id, event, significance from eco_events where region = "%s"' %(ccy), engine)
        s_check2 = pd.read_sql('select event from eco_significant where region = "%s"' %(ccy), engine)
        #get data to check if its inlcuded and get ids to update
        
        s_check1['significance'] =  s_check1['event'].isin(s_check2['event'])
        #pick values which need to be updated 
        
        s_update = s_check1[s_check1['significance']==True]
        #list to update
        
        for i in s_update.id:
            engine.execute("UPDATE eco_events SET significance = 1 WHERE id = %s " %(i))


# In[16]:

update_eco_lists()


# In[ ]:



