# Library to use

from bs4 import BeautifulSoup as bs
import pandas as pd
import urllib.request as r
import os
import urllib.parse as up
import psycopg2
import psycopg2.extras as extras

# Homepage Wikipedia (English)
wikipedia_url = "https://en.wikipedia.org/wiki/Main_Page"


# Function to remove html tag and get the text
def remove_tags(elem):
    for data in elem(['b>a']):
        data.decompose()
    return ' '.join(elem.stripped_strings)[7:]


# Function to get data from "On this Day" Section
def wiki_otd(url):
    page = r.urlopen(url)
    soup = bs(page.read())
    part_otd = soup.findChild('div', {'id': 'mp-otd'})

    date = part_otd.findChild('p').findChild('b').findChild('a').text

    list_otd = part_otd.find('ul').findChildren('li')

    event = {}

    for idx, i in enumerate(list_otd):
        year = i.findChild('a').text
        item = remove_tags(i)

        pic = None

        if item.find('(model pictured)') != -1:
            item = item.replace('(model pictured) ', '')
            pic = 'https:' + part_otd.select_one('div div img')['src']
        elif item.find('(pictured)') != -1:
            item = item.replace('(pictured) ', '')
            pic = 'https:' + part_otd.select_one('div div img')['src']

        event[idx + 1] = {'year': year, 'item': item, 'picture': pic}

    return {'date': date, 'event': event}


wiki_otd_list = wiki_otd(wikipedia_url)

data = pd.DataFrame.from_dict(wiki_otd_list['event'], orient="index")

data['date'] = wiki_otd_list['date']

# Rearange dataframe column
data = data[['date', 'year', 'item', 'picture']]

# Connect to PostgreSQL DB
up.uses_netloc.append("postgres")
url = up.urlparse(os.environ["DATABASE_URL"])
print(url)
conn = psycopg2.connect(database=url.path[1:],
                        user=url.username,
                        password=url.password,
                        host=url.hostname,
                        port=url.port
                        )


# Function to insert data into PGSQL DB
def insert_values(conn, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))

    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as ex:
        print("Error: %s" % ex)
        conn.rollback()
        cursor.close()
        return 1
    print("Success")
    cursor.close()
    conn.close()


insert_values(conn, data, "wikipedia_otd")
