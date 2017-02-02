# coding=utf-8
import time
import tweepy
import unicodedata
import mysql.connector
import xlrd
import numpy
import easygui
import plotly.offline as offline
import plotly.graph_objs as go
from datetime import date, datetime

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.neighbors import NearestNeighbors


def daydiff(date1, date2):
    return (date2 - date1).days


def populate_positive():
    positives = list()
    book = xlrd.open_workbook("PosLex.xls")
    sh = book.sheet_by_index(0)
    for rx in range(1, sh.nrows):
        positives.append(sh.row(rx)[2].value)
    return positives


def populate_stopwords():
    words = list()
    f = open("stopwords.txt")
    for rx in f.readlines():
        if rx != '' and rx != ' ':
            words.append(rx.decode('utf-8').strip())
    return words


def stopwordrem(string, words):
    remove = list()
    for word in string.split(" "):
        for word2 in words:
            if word == word2:
                remove.append(word)
    for word in remove:
        string = string.replace(word, '')
    return string.strip()


def populate_negative():
    negatives = list()
    book = xlrd.open_workbook("NegLex.xls")
    sh = book.sheet_by_index(0)
    for rx in range(1, sh.nrows):
        negatives.append(sh.row(rx)[2].value)
    return negatives


def extract_sentiment(string, sentiment):
    count = 0
    for word in sentiment:
        if word in string:
            count += 1
    return count


# If you find more than 2 greek letters then the string is greek
def greek_recognizer(string):
    oc = 0
    for char in string:
        if char == '\n' or char == ' ' or char == '\r':
            continue
        try:
            if 'GREEK' in unicodedata.name(char):
                oc += 1
        except ValueError:
            continue
        if oc > 2:
            return True
    return False


def text_strip(string):
    newstr = ''
    for char in string:
        if char == '\n' or char == ' ' or char == '\r':
            newstr += char
        if not set('[~!@#$%^&*()_+{}":;\']+$').intersection(char):
            try:
                if 'GREEK' in unicodedata.name(char):
                    newstr += char
            except ValueError:
                continue
    return newstr.strip()


# Remove accent and convert t uppercase. Check the decode function
def formatter(string):
    return ''.join(c for c in unicodedata.normalize('NFD', string.upper())
                   if unicodedata.category(c) != 'Mn')


def suffixremove(string):
    if len(string) < 4:
        return string.strip()

    if string.endswith(u"ΟΥΣ") or string.endswith(u"ΕΙΣ") or string.endswith(u"ΕΩΝ") or string.endswith(u"ΟΥΝ"):
        return (string[:len(string) - 3]).strip()
    elif string.endswith(u"ΟΣ") or string.endswith(u"ΗΣ") or string.endswith(u"ΕΣ") or string.endswith(u"ΩΝ") \
            or string.endswith(u"ΟΥ") or string.endswith(u"ΟΙ") or string.endswith(u"ΑΣ") or string.endswith(
        u"ΩΣ") or string.endswith(u"ΑΙ") or string.endswith(u"ΥΣ") or string.endswith(u"ΟΝ") or string.endswith(u"ΑΝ") \
            or string.endswith(u"ΕΙ"):
        return (string[:len(string) - 2]).strip()
    elif string.endswith(u"Α") or string.endswith(u"Η") or string.endswith(u"Ο") or string.endswith(u"Ε") or \
            string.endswith(u"Ω") or string.endswith(u"Υ") or string.endswith(u"Ι"):
        return (string[:len(string) - 1]).strip()
    return string.strip()


# Remove accent and convert t uppercase. Check the decode function
def suffix_remover(string):
    splitted = string.split(' ')
    newstr = ''
    for stri in splitted:
        if stri != ' ' and stri != '':
            newstr += suffixremove(stri.strip()) + ' '

    return newstr.strip()


def extract_categories(string):
    hashtags = ["#ND", "#SYRIZA", "@atsipras", "@mitsotakis"]
    categories = ''
    for tag in hashtags:
        if tag.lower() in string.lower():
            categories += tag + ','
    return categories[:len(categories) - 1]


# If the twitter API send error due to use limit, wait for it to end
def handle_limit(cursor):
    while True:
        try:
            yield cursor.next()
        except tweepy.error.TweepError:
            time.sleep(15 * 60)

            # Replace the data below with your own connection settings


def db_connection():
    return mysql.connector.connect(user='root', password='root',
                                   host='127.0.0.1',
                                   database='tweets')


def mine():
    # access settings for tweeter
    auth = tweepy.OAuthHandler("4AvlgKXr073UYo0qy7NlYAk7J", "yvep2vscsGSvnBTUcqUqTdvT97dyYB8zRY6VNJNVULDcEaGEuk")
    auth.set_access_token("490652039-nzZWjVbS8GheGc1Gh3PL0RKndoNitiJsu1PnSFUk",
                          "RkCCI3SFyj0si7MJ2kXpG5jf5bQWkmXoitoKRcsQITtyb")
    # New database connection
    cnx = db_connection()

    # Create a cursor
    cursor = cnx.cursor()

    # add new tweet SQL statement
    add_tweet = ("INSERT INTO tweet_data "
                 "(tweet_text, date, tweeter_id) "
                 "VALUES (%s, %s, %s)")

    # Hashtags to search
    hashtags = ["#ND", "#SYRIZA"]
    # persons to search
    persons = ["@atsipras", "@mitsotakis"]
    end = hashtags + persons
    ids = list()
    api = tweepy.API(auth)
    # Query to find last tweet
    query = "SELECT tweeter_id FROM tweet_data ORDER BY tweeter_id DESC LIMIT 1"
    cursor.execute(query)
    last_id = ""
    for id in cursor:
        last_id = id[0]
    cursor.close()
    cursor = cnx.cursor()

    # Iterate through hashtags
    for hash_ in end:
        for tweet in handle_limit(tweepy.Cursor(api.search, q=hash_, include_entities=True).items()):
            # If there are tweets in db
            if last_id is not "":
                # check that you only store newer tweets
                if tweet.id > last_id:
                    # if tweet is not already saved during this session
                    if tweet.id not in ids:
                        if greek_recognizer(tweet.text):
                            try:
                                cursor.execute(add_tweet,
                                               (tweet.text, date(tweet.created_at.year, tweet.created_at.month,
                                                                 tweet.created_at.day), tweet.id))
                                ids.append(tweet.id)
                            except mysql.connector.DatabaseError:
                                try:
                                    cursor.execute(add_tweet,
                                                   (tweet.text.encode('utf-8'),
                                                    date(tweet.created_at.year, tweet.created_at.month,
                                                         tweet.created_at.day), tweet.id))
                                    ids.append(tweet.id)
                                except mysql.connector.DatabaseError:
                                    print "String not in utf"
                else:
                    break
            # if tweet is not already saved during this session
            elif tweet.id not in ids:
                if greek_recognizer(tweet.text):
                    try:
                        cursor.execute(add_tweet, (tweet.text, date(tweet.created_at.year, tweet.created_at.month,
                                                                    tweet.created_at.day), tweet.id))
                        ids.append(tweet.id)
                    except mysql.connector.DatabaseError:
                        try:
                            cursor.execute(add_tweet,
                                           (tweet.text.encode('utf-8'),
                                            date(tweet.created_at.year, tweet.created_at.month,
                                                 tweet.created_at.day), tweet.id))
                            ids.append(tweet.id)
                        except mysql.connector.DatabaseError:
                            print "String not in utf"
            else:
                break
    # Commit tweet inserts
    cnx.commit()

    # close the connections and free the cursor
    cursor.close()
    cnx.close()


def proc():
    cnx = db_connection()
    cursor = cnx.cursor(buffered=True)
    # Get all tweets query
    query = "SELECT * FROM tweet_data"
    cursor.execute(query)

    # Delete query
    delete_tweet = "DELETE FROM tweet_data WHERE id = %(_id)s"
    print "Retrieving data..."
    print "Removing duplicates, checking for data that should be processed"
    found = 0
    tweets = list()
    remove = list()
    unpro = False
    for row in cursor:
        if row[4] is None:
            unpro = True
        if row[1] in tweets:
            found += 1
            remove.insert(0, row[0])
        else:
            tweets.insert(0, row[1])
    if not unpro:
        return
    cursor.close()
    cursor = cnx.cursor(buffered=True)

    print "Found", found, "duplicates removing..."

    for _id in remove:
        cursor.execute(delete_tweet, {'_id': _id})

    cnx.commit()
    cursor.close()

    print "Duplicates removed"
    print "Retrieving data..."

    cursor = cnx.cursor(buffered=True)
    cursor.execute(query)

    print "Removing non-greek tweets..."
    rows = list()
    for row in cursor:
        rows.insert(0, row)
    cursor.close()
    cursor = cnx.cursor(buffered=True)
    # Search and count tweets containing emoticons
    for row in rows:
        if not greek_recognizer(row[1]):
            cursor.execute(delete_tweet, {'_id': row[0]})

    cnx.commit()
    cursor.close()
    print "Retrieving data..."
    print "Removing special characters and non-greek characters, converting to uppercase and striping accents"
    cursor = cnx.cursor(buffered=True)
    cursor.execute(query)
    uppercase = "UPDATE tweet_data SET tweets.tweet_data.cleaned_text =%(_text)s WHERE id = %(_id)s"
    rows = list()
    for row in cursor:
        rows.insert(0, row)
    stopwords = populate_stopwords()
    for row in rows:
        cursor.execute(uppercase,
                       {'_id': row[0],
                        '_text': suffix_remover(stopwordrem(formatter(text_strip(row[1])), stopwords)).strip()})

    cnx.commit()
    cursor.close()
    cursor = cnx.cursor(buffered=True)
    cursor.execute(query)
    uppercase = "UPDATE tweet_data SET tweets.tweet_data.positive =%(_pos)s," \
                "tweets.tweet_data.negative =%(_neg)s, tweets.tweet_data.categories =%(_cat)s WHERE id = %(_id)s"
    rows = list()
    positive = populate_positive()
    negative = populate_negative()
    for row in cursor:
        rows.insert(0, row)
    for row in rows:
        cursor.execute(uppercase, {'_id': row[0],
                                   '_pos': extract_sentiment(row[4], positive),
                                   '_neg': extract_sentiment(row[4], negative),
                                   '_cat': extract_categories(row[1])})

    cnx.commit()
    cursor.close()


def frequency():
    cnx = db_connection()
    cursor = cnx.cursor(buffered=True)

    choice = easygui.choicebox("Choose a category", choices=["#SYRIZA", "#ND", "@atsipras", "@mitsotakis"])

    query = "SELECT DISTINCT date FROM tweet_data ORDER BY date"
    cursor.execute(query)
    dates = list()
    for row in cursor:
        dates.append(row[0])
    cursor.close()
    x = list()
    y = list()
    y1 = list()
    for date1 in dates:
        x.append(date1)
        cursor = cnx.cursor(buffered=True)
        query = "SELECT positive, negative FROM tweet_data WHERE categories LIKE '%" + choice \
                + "%' AND date = \'" + date1.strftime("%Y-%m-%d %H:%M:%S") + "\'"
        cursor.execute(query)
        positive = 0
        negative = 0
        rowcount = 0
        for row in cursor:
            positive += row[0]
            negative += row[1]
            rowcount += 1
        if cursor.rowcount != 0:
            y.append((positive / float(cursor.rowcount)))
            y1.append(negative / float(cursor.rowcount))
        else:
            y.append(0)
            y1.append(0)
        cursor.close()
    trace0 = go.Scatter(
        x=x,
        y=y,
        name='Positive',
        line=dict(
            color=('rgb(205,12,24)'),
            width=4
        )
    )
    trace1 = go.Scatter(
        x=x,
        y=y1,
        name='Negatice',
        line=dict(
            color=('rgb(22,96,167)'),
            width=4
        )
    )
    data = [trace0, trace1]

    offline.plot({'data': data, 'layout': {'title': 'Test Plot', 'font': dict(size=16)}})


def dailyamount():
    cnx = db_connection()
    cursor = cnx.cursor(buffered=True)
    choices = ["#SYRIZA", "#ND", "@atsipras", "@mitsotakis"]

    query = "SELECT DISTINCT date FROM tweet_data ORDER BY date"
    cursor.execute(query)
    dates = list()
    for row in cursor:
        dates.append(row[0])
    cursor.close()
    x = list()
    y = [list(), list(), list(), list()]
    y1 = [list(), list(), list(), list()]
    for date1 in dates:
        x.append(date1)
        cursor = cnx.cursor(buffered=True)
        query = "SELECT positive, negative, categories FROM tweet_data WHERE date = \'" + date1.strftime(
            "%Y-%m-%d %H:%M:%S") + "\'"
        cursor.execute(query)
        for choice in choices:
            i = choices.index(choice)
            y[i].append(0)
            y1[i].append(0)
        for row in cursor:
            positive = False
            if row[0] >= row[1]:
                positive = True
            for choice in choices:
                if choice in row[2]:
                    i = choices.index(choice)
                    if positive:
                        y[i][len(y[i]) - 1] += 1
                    else:
                        y1[i][len(y1[i]) - 1] += 1
        cursor.close()

    data = list()
    i = 0
    for positive in y:
        data.append(go.Bar(
            x=x,
            y=positive,
            name='Positive-' + choices[i]))
        i += 1
    i = 0
    for negative in y1:
        data.append(go.Bar(
            x=x,
            y=negative,
            name='Negative-' + choices[i]))
        i += 1

    offline.plot({'data': data, 'layout': {'title': 'Test Plot', 'font': dict(size=16)}})


def weekly():
    cnx = db_connection()
    cursor = cnx.cursor(buffered=True)
    choice = easygui.choicebox("Choose a category", choices=["#SYRIZA", "#ND", "@atsipras", "@mitsotakis"])
    query = "SELECT DISTINCT date FROM tweet_data ORDER BY date"
    cursor.execute(query)
    dates = list()
    for row in cursor:
        dates.append(row[0])
    cursor.close()
    x = list()
    y = [list(), list(), list(), list()]
    startdate = ''
    total = 0
    for yi in y:
        yi.append(0)
    positives = list()
    negatives = list()
    for date1 in dates:
        if startdate == '':
            startdate = date1
        cursor = cnx.cursor(buffered=True)
        query = "SELECT positive, negative, categories FROM tweet_data WHERE date = \'" + date1.strftime(
            "%Y-%m-%d %H:%M:%S") + "\'"
        cursor.execute(query)

        if daydiff(startdate, date1) >= 7:
            x.append(
                str(startdate.day) + ' - ' + str(date1.day) + "/" + str(date1.month) + "/" + str(
                    date1.year) + "\nTotal Tweets: " + str(total))
            startdate = date1
            total = 0

            y[0][len(y[0]) - 1] = numpy.mean(positives)
            y[1][len(y[0]) - 1] = numpy.mean(negatives)
            y[2][len(y[1]) - 1] = numpy.std(positives)
            y[3][len(y[1]) - 1] = numpy.std(negatives)

            positives = list()
            negatives = list()
            for yi in y:
                yi.append(0)
        positives.append(0)
        negatives.append(0)
        for row in cursor:
            total += 1
            positive = False
            if row[0] >= row[1]:
                positive = True
            if choice in row[2]:
                if positive:
                    positives[len(positives) - 1] += 1

                else:
                    negatives[len(negatives) - 1] += 1
        cursor.close()

    if total != 0:
        y[0].pop(len(y[0]) - 1)
        y[1].pop(len(y[0]) - 1)
        y[2].pop(len(y[0]) - 1)
        y[3].pop(len(y[0]) - 1)

    data = list()
    data.append(go.Bar(
        x=x,
        y=y[0],
        name='Positive-Mean'))
    data.append(go.Bar(
        x=x,
        y=y[1],
        name='Negative-Mean'))
    data.append(go.Bar(
        x=x,
        y=y[2],
        name='Positive Deviation'))
    data.append(go.Bar(
        x=x,
        y=y[3],
        name='Negative Deviation'))

    offline.plot({'data': data, 'layout': {'title': 'Weekly Summary' + choice, 'font': dict(size=16)}})


def svd():
    cnx = db_connection()
    cursor = cnx.cursor(buffered=True)
    query = "SELECT cleaned_text FROM tweet_data ORDER BY date"
    cursor.execute(query)
    rows = list()
    i = 0
    for row in cursor:
        if i > 1000:
            break
        rows.append(row[0])
        i += 1
    cv = CountVectorizer(min_df=2)
    X = cv.fit_transform(rows)
    U, s, Vh = numpy.linalg.svd(X.toarray().T, full_matrices=False)
    print numpy.dot(U, numpy.dot(numpy.diag(s), Vh))
    norm = numpy.linalg.norm(U, axis=0)
    print NearestNeighbors(n_neighbors=5, algorithm='ball_tree').fit(norm).kneighbors(norm)

    # data = list()
    # data.append(go.Bar(
    #     x=x,
    #     y=y[0],
    #     name='Positive-Mean'))
    # offline.plot({'data': data, 'layout': {'title': 'Weekly Summary' + choice, 'font': dict(size=16)}})


def analyze():
    while True:
        choice = easygui.buttonbox(
            "There are several analysis options\n" +
            "1: Frequency: choose a category to display frequency for each date",
            choices=("Frequency", "Amount", "Weekly", "SVD", "back"))
        if choice is "Frequency":
            frequency()
        elif choice is "Amount":
            dailyamount()
        elif choice is "Weekly":
            weekly()
        elif choice is "SVD":
            svd()
        else:
            return


def operation_choice():
    while True:
        choice = easygui.buttonbox(
            "Welcome to tweeter miner v2.0\nThis software will mine tweets\ncontaining the following hashtags\n"
            "#syriza, #siriza, #ΣΥΡΙΖΑ and the following mentions @atsipras, @tsipras_eu\n"
            "Tweets that contain #nd, #neadimokratia, #neadhmokratia, #anel, #ΑΝΕΛ, #ΝΔ \n"
            "will be excluded from final counting", choices=("Mine", "Process", "Analyze"))
        if choice is "Mine":
            print "Beginning mining...."
            mine()
            print "Mining finished!"
        elif choice is "Process":
            proc()
        elif choice is "Analyze":
            proc()
            analyze()
        else:
            print "exiting..."
            exit(1)


if __name__ == "__main__":
    operation_choice()
