# Library to use
from bs4 import BeautifulSoup as bs
import pandas as pd
import urllib.request as r
import os
import urllib.parse as up
import psycopg2
import psycopg2.extras as extras
from datetime import date

# Homepage Wikipedia (English)
wikipedia_url = "https://en.wikipedia.org/wiki/Main_Page"


# Function to get data from "Today's Featured Picture" Section
def wiki_tfd(url):
    page = r.urlopen(url)
    soup = bs(page.read())
    part_ftd = soup.findChild('div', {'id': 'mp-tfp'})
    img = "https:" + part_ftd.find('img')['src']

    item = ''
    contents = part_ftd.find_all('p')

    for idx, i in enumerate(contents):
        content = i.text.replace('\n', '')

        if len(item) > 280:
            splits = item.split('.')
            item = splits[0] + ". "

        item = item + " " + content

    today = date.today().strftime("%B %d, %Y")

    return [{'date': today, 'item': str(item), 'picture': img}]


tfd = wiki_tfd(wikipedia_url)
data = pd.DataFrame(tfd)

# Connect to PostgreSQL DB
up.uses_netloc.append("postgres")
url = up.urlparse(os.getenv("DATABASE_URL"))
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


insert_values(conn, data, "wikipedia_tfp")