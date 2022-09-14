from datetime import date
import os
import requests
import urllib.parse as up
import psycopg2
import pandas as pd
import tweepy


def main():

    today = date.today().strftime("%B %d")
    # Connect to PostgreSQL DB
    up.uses_netloc.append("postgres")
    url = up.urlparse(os.environ["DATABASE_URL"])
    conn = psycopg2.connect(database=url.path[1:],
                            user=url.username,
                            password=url.password,
                            host=url.hostname,
                            port=url.port
                            )

    cur = conn.cursor()

    cur.execute(
        """
        SELECT * FROM wikipedia_otd w WHERE w.date = %s;
        """,
        [today, ]
    )

    data = pd.DataFrame(cur.fetchall(),
                        columns=['id', 'date', 'year', 'item', 'picture'])

    cur.close()
    conn.close()

    # POST TO TWITTER
    auth = tweepy.OAuthHandler(
        os.environ['TWITTER_CONSUMER_API_KEY'],
        os.environ['TWITTER_CONSUMER_API_SECRET']
    )

    auth.set_access_token(
        os.environ['ACCESS_TOKEN'],
        os.environ['ACCESS_TOKEN_SECRET']
    )

    api = tweepy.API(auth)

    info_w_pic = data[data['picture'].values!=None]

    tweet = "Today (" + info_w_pic.iloc[0]['date'] + ") in " + \
        info_w_pic.iloc[0]['year'] + " " + \
        info_w_pic.iloc[0]['item'] + "\n" + \
        "#wikipedia #wikipediaonthisday"

    tweet_with_img(api, info_w_pic.iloc[0]['picture'], tweet)


def tweet_with_img(api, url, message):
    filename = 'temp.jpg'
    request = requests.get(url, stream=True)
    if request.status_code == 200:
        with open(filename, 'wb') as image:
            for chunk in request:
                image.write(chunk)

        api.update_status_with_media(status=message, filename=filename)
        os.remove(filename)
    else:
        print("Image not found")


if __name__ == "__main__":
    main()