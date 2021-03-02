
# coding: utf-8

# In[1]:

# this script uses beautiful soup 4 to scrape and format economic
# events from tradingeconmics and produce data to put in a db


# In[2]:

import requests
import sys
import pandas as pd
from bs4 import *
import datetime
import time 
from engine_code import *
from pandas.tseries.offsets import BDay


# In[3]:

countries = {'USD':'https://tradingeconomics.com/united-states/calendar',
             'GBP':'https://tradingeconomics.com/united-kingdom/calendar',
             'AUD':'https://tradingeconomics.com/australia/calendar',
             'NZD':'https://tradingeconomics.com/new-zealand/calendar',
             'SEK':'https://tradingeconomics.com/sweden/calendar',
             'NOK':'https://tradingeconomics.com/norway/calendar',
             'JPY':'https://tradingeconomics.com/japan/calendar',
             'CNY':'https://tradingeconomics.com/china/calendar',
             'CHF':'https://tradingeconomics.com/switzerland/calendar',
             'KRW':'https://tradingeconomics.com/south-korea/calendar',
             'CAD':'https://tradingeconomics.com/canada/calendar',
             'MXN':'https://tradingeconomics.com/mexico/calendar',
             'ILS':'https://tradingeconomics.com/israel/calendar',
             'ZAR':'https://tradingeconomics.com/south-africa/calendar'
            }


# In[121]:

ccy = "USD"
#this should be the only bit you need to set


# In[122]:

page = requests.get(countries[ccy])
soup = BeautifulSoup(page.content, 'html.parser')


# In[123]:

o = soup.find_all('table')
table = o[1]  # contains data


# In[124]:

columnz2 = ['datetime', 'event', 'period', 'day_of_month', 'week_of_year','country']
indexz = []
df2 = pd.DataFrame(index = indexz, columns=columnz2)


# In[125]:

#print(table.prettify())
#table

#note there are here as you'd look at them to alter / find code


# In[126]:

#standard webscrape

def get_eco_data(table,df2,ccy):

    counter = 0

    for row in table.find_all('tr'):
        header = row.find_all('th')
        if len(header) > 0:
        #if header is not None:
            date = header[0].get_text(strip=True)
            datev = datetime.datetime.strptime(date, "%A %B %d %Y")

        else:
            timez = row.find(class_="calendar-date-2")
            timez2 = row.find(class_="calendar-date-1")
            timez3 = row.find(class_="calendar-date-3")
            event = row.find(class_="calendar-event")
            check = row.find_all("span")
            if len(check) > 0:
                period = row.find_all("span", limit=2)[1]
            else:
                period == None

            if event is None:
                continue
            else:
                if timez is not None:
                    time = timez.get_text(strip=True)
                    d1 = 1
                elif timez2 is not None:
                    time = timez2.get_text(strip=True)
                    d1 = 1
                elif timez3 is not None:
                    time = timez3.get_text(strip=True)
                    d1 = 1
                else:
                    time =""
                    d1 = 0

                event1 = event

                if period is not None:
                    period1 = period.get_text(strip=True)


                else:
  
                    period1 = ""

                if d1 > 0:

                    timev = datetime.datetime.strptime(time, "%I:%M %p")
                    timea = timev.time()

                    datetimev = datetime.datetime.combine(datev, timea)
                    dom = datetimev.day
                    woy = datetimev.isocalendar()[1]
                else:
                    datetimev = datev
                    dom = datetimev.day
                    woy = datetimev.isocalendar()[1]

                df2.at[counter,'datetime'] = datetimev
                df2.at[counter,'event'] = event1.get_text(strip=True)
                df2.at[counter,'period'] = period1
                df2.at[counter,'day_of_month'] = dom
                df2.at[counter,'week_of_year'] = woy

                # assign to df
                #print(date+" "+time+" "+event1+" "+period1)
                counter +=1
    df2['region'] = ccy


# In[127]:

#EUR webscrape to assign each country

def get_eco_data2(table,df2,ccy):
    #just for EUR data 

    counter = 0
    EUR = ['IT','DE','ES','EA','FR']

    for row in table.find_all('tr'):
        header = row.find_all('th')
        if len(header) > 0:
        #if header is not None:
            date = header[0].get_text(strip=True)
            datev = datetime.datetime.strptime(date, "%A %B %d %Y")

        else:
            EUR_iso = row.find(class_="calendar-iso").get_text(strip=True)
            if EUR_iso not in EUR:
                continue 
                
            else:
                country = row.find(class_="calendar-iso").get_text(strip=True)
                timez = row.find(class_="calendar-date-2")
                timez2 = row.find(class_="calendar-date-1")
                event = row.find(class_="calendar-event")
                check = row.find_all("span")
                if len(check) > 0:
                    period = row.find_all("span", limit=2)[1]
                else:
                    period == None

                if event is None:
                    continue
                else:
                    if timez is not None:
                        time = timez.get_text(strip=True)
                        d1 = 1
                    elif timez2 is not None:
                        time = timez2.get_text(strip=True)
                        d1 = 1
                    else:
                        time =""
                        d1 = 0
                    testr = country+" - "
                    event2 = event.get_text(strip=True)
                    event1 = testr+event2

                    if period is not None:
                        period1 = period.get_text(strip=True)


                    else:
                        period1 = ""

                    if d1 > 0:

                        timev = datetime.datetime.strptime(time, "%I:%M %p")
                        timea = timev.time()

                        datetimev = datetime.datetime.combine(datev, timea)
                        dom = datetimev.day
                        woy = datetimev.isocalendar()[1]
                    else:
                        datetimev = datev
                        dom = datetimev.day
                        woy = datetimev.isocalendar()[1]

                    df2.at[counter,'datetime'] = datetimev
                    df2.at[counter,'event'] = event1   #.get_text(strip=True)
                    df2.at[counter,'period'] = period1
                    df2.at[counter,'day_of_month'] = dom
                    df2.at[counter,'week_of_year'] = woy
                    df2.at[counter,'country'] = country

                    # assign to df
                    #print(date+" "+time+" "+event1+" "+period1)
                    counter +=1
    df2['region'] = ccy
        #  df2['day_of_month'] = df2['datetime'].day
        #  df2['week_of_year'] = df2['datetime'].isocalendar()[1]

        #insert values to sql     
        #with engine.connect() as conn, conn.begin():
        #    df2[['datetime', 'event', 'period', 'region']].to_sql('eco_events', engine, chunksize=1000, if_exists = 'append', index=False)


# In[128]:

current_date = datetime.date.today()


# In[129]:

old = pd.read_sql('select datetime, event, period, region, day_of_month, week_of_year, country from eco_events where region = "%s" and datetime >= "%s" and eco_type != 1 ' %(ccy, current_date), engine)
old['code'] = 1
num1 = old.event.count()


# In[130]:

#check if euro or not and get new scraped data

if ccy != "EUR":
    get_eco_data(table,df2,ccy)
else:
    get_eco_data2(table,df2,ccy)


# In[131]:

if num1 == 0:
    with engine.connect() as conn, conn.begin():
        df2[['datetime', 'event', 'period', 'region','day_of_month','week_of_year','country']].to_sql('eco_events', engine, chunksize=1000, if_exists = 'append', index=False)
    #insert_new_eco(table,df2,ccy)
    print("only new data added")
else:
    maxdt = pd.read_sql('select max(datetime) from eco_events where region = "%s"' %(ccy), engine).iloc[0,0]
    #new data which isnt alrady in db (greater than max date)
    newd = df2[df2['datetime'] > maxdt]
    
    if newd.event.count() >0:    
        #insert new data
        print("new data added")
        with engine.connect() as conn, conn.begin():
            newd[['datetime', 'event', 'period', 'region','day_of_month','week_of_year','country']].to_sql('eco_events', engine, chunksize=1000, if_exists = 'append', index=False)
    else:
        print("no new data")
    
    
    #new section
    check = df2[df2['datetime'] <= maxdt]   # new scraped values
    check['code'] = 2
    test4 = old.append(check)      # old db data  and appened new data
    uniq = test4.drop_duplicates(['datetime','event','region'],keep=False)   # leave 
    
    #split up new and old values
    remove = uniq[uniq['code']==1]
    keep_new = uniq[uniq['code']==2]
    
    if uniq.event.count() > 0:
        #dfi = pd.DataFrame(columns=columnz2)
        numb = uniq.event.count()
        print("check unique data")

        for index, row in remove.iterrows():
            dlt_dt = row.datetime
            dlt_event = row.event
            dlt_region = row.region

            print("deleted ",dlt_region,dlt_dt, dlt_event)
            with engine.connect() as connection:
                connection.execute("""DELETE FROM eco_events WHERE datetime = '%s' AND event = "%s" AND region = "%s" """ %(dlt_dt, dlt_event, dlt_region))


        with engine.connect() as conn, conn.begin():
            keep_new[['datetime', 'event', 'period', 'region','day_of_month','week_of_year','country']].to_sql('eco_events', engine, chunksize=1000, if_exists = 'append', index=False)
        print("updated data entered")
    
    else:
        print("empty - nothing to modify")
    


# In[ ]:



