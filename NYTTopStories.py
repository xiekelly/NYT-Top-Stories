#!/usr/bin/python3

'''

This code fetches data from the NYT Top Stories API,
and stores the data in a database "NYT_TopStories" in MySQL.
It stores the data in two separate tables according to
time-invariant data versus time-varying data.

Using cron, it fetches data every hour.

'''

import requests
import json

# fetch data from NYT Top Stories API
def getNYTTopStories():
    ''' 
    Connect to the API and return back a list
    of dictionaries that represent recent
    top articles
    '''
    # use API key to get the data in json format
    api_key = 'a143c58fdb33406db9ea1d80fa4d5d74'
    url = 'https://api.nytimes.com/svc/topstories/v2/home.json?api-key=' + api_key
    resp = requests.get(url)
    data = json.loads(resp.text)
    stories_dict = data["results"] # only store the results attribute
    return stories_dict

NYT_data = getNYTTopStories()


import MySQLdb as mdb
import sys

# set up a database to store the data in
def connectDatabase():
    con = mdb.connect(host = '34.199.88.98', # insert your EC2 instance IP address
                      user = 'root', 
                      passwd = 'dwdstudent2015', 
                      charset = 'utf8', use_unicode=True);
    return con

def createNYTdatabase(con, db_name):
    ''' 
    Connects to the database and creates (if it does not exist)
    the database and the tables needed to store the data
    '''
    # query to create a database in SQL
    create_db_query = "CREATE DATABASE IF NOT EXISTS {0} DEFAULT CHARACTER SET 'utf8'"        .format(db_name)

    cursor = con.cursor()
    cursor.execute(create_db_query)
    cursor.close()
    pass

def createTimeInvariantTable(con, db_name, table_name):
    cursor = con.cursor()
    # create a table using CREATE TABLE
    # {0} and {1} are placeholders for the parameters in the format() statement
    create_table_query = '''CREATE TABLE IF NOT EXISTS {0}.{1} 
                                    (URL varchar(50), 
                                    Section varchar(50), 
                                    Subsection varchar(50),
                                    Title varchar(250),
                                    Author varchar(50),
                                    Abstract varchar(250),
                                    PublishedDate datetime,
                                    PRIMARY KEY(URL)
                                    )'''.format(db_name, table_name)
    cursor.execute(create_table_query)
    cursor.close()
    
def createTimeVaryingTable(con, db_name, table_name):
    cursor = con.cursor()
    # Create a table
    # {0} and {1} are placeholders for the parameters in the format() statement
    create_table_query = '''CREATE TABLE IF NOT EXISTS {0}.{1} 
                                    (URL varchar(50),
                                    Title varchar(250),
                                    UpdatedDate datetime,
                                    PRIMARY KEY(URL, UpdatedDate),
                                    FOREIGN KEY(URL) 
                                            REFERENCES {0}.article_info(URL)
                                    )'''.format(db_name, table_name)
    cursor.execute(create_table_query)
    cursor.close()

con = connectDatabase()
db_name = 'NYT_TopStories'
createNYTdatabase(con, db_name)
article_table = 'article_info'
createTimeInvariantTable(con, db_name, article_table)
article_table = 'article_status'
createTimeVaryingTable(con, db_name, article_table)


from datetime import datetime

# store time-invariant data into table in the database
def storeTimeInvariantData(con, NYT_data):
    '''
    Accepts as a parameter a list of dictionaries, where
    each dictionary is an article from the NYT Top Stories section.
    Check if the article exists in the database already.
    If it does not, store in the database the entries 
    that are time invariant.
    '''
    db_name = 'NYT_TopStories'
    table_name = 'article_info'
    
    for article in NYT_data:
        article_url = article["url"] # 7 character unique URL identifier
        section = article["section"]
        subsection = article["subsection"]
        title = article["title"]
        author_str = article["byline"]
        author = author_str[3:].title() # format author names
        desc = article["abstract"]
        date_str = article["published_date"]
        date  =  datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S-05:00') # parse the date
        insertArticleInfo(con, db_name, table_name, 
                      article_url, section, subsection, title, author, desc, date)
    
    # writes the data in the database
    con.commit()
    return
 
# use INSERT command
def insertArticleInfo(con, db_name, table_name, 
                  article_url, section, subsection, title, author, desc, date):
    query_template = '''INSERT IGNORE INTO {0}.{1}
                                    (URL, 
                                    Section, 
                                    Subsection,
                                    Title,
                                    Author,
                                    Abstract,
                                    PublishedDate) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)'''.format(db_name, table_name)

    cursor = con.cursor()
    query_parameters = (article_url, section, subsection, title, author, desc, date)
    cursor.execute(query_template, query_parameters)
    cursor.close()


storeTimeInvariantData(con, NYT_data)


# store the time-varying data into the table in the database
def storeTimeVaryingData(con, NYT_data):
    '''
    Accepts as a parameter a list of dictionaries, where
    each dictionary is an article. Stores in the database
    the entries that are time varying. Check if the article
    entry and corresponding "updated_date" timestamp exists,
    and if it does not, store it in the database
    '''
    db_name = 'NYT_TopStories'
    table_name = 'article_status'
    
    for article in NYT_data:
        article_url = article["url"]
        title = article["title"]
        date_str = article["updated_date"]
        date  =  datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S-05:00') # parse the date
        insertArticleStatus(con, db_name, table_name, 
            article_url, title, date)

    con.commit()
    
    return

# use INSERT command
def insertArticleStatus(con, db_name, table_name, 
                  article_url, title, date):
    query_template = '''INSERT IGNORE INTO {0}.{1}
                                    (URL, 
                                    Title,
                                    UpdatedDate) 
                VALUES (%s, %s, %s)'''.format(db_name, table_name)

    cursor = con.cursor()
    query_parameters = (article_url, title, date)
    cursor.execute(query_template, query_parameters)
    cursor.close()

    return

storeTimeVaryingData(con, NYT_data)

