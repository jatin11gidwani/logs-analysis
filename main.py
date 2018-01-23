#!/usr/bin/env python2.7

import psycopg2


def popular_article():
    """Print most popular articles."""
    POSTS = psycopg2.connect("dbname=news")
    c = POSTS.cursor()
    c.execute("""select articles.title, count(*) as views from log,articles
            where substr(log.path, 10) = articles.slug
            group by articles.title order by views desc limit 3;""")
    articles = c.fetchall()
    for i in range(len(articles)):
        print str(i+1) + ". " + articles[i][0] + " = " + str(articles[i][1]) + " views."  # noqa
    POSTS.close()


def log_error():
    POSTS = psycopg2.connect("dbname=news")
    c = POSTS.cursor()
    c.execute("""SELECT
             COUNT(*),
             TO_CHAR(time,'YYYY-MM-DD') as timee
             FROM log where status like '%NOT%'
             GROUP BY TO_CHAR(time,'YYYY-MM-DD')
             ORDER BY TO_CHAR(time,'YYYY-MM-DD') desc;""")
    error = c.fetchall()
    c.execute("""SELECT
             COUNT(*),
             TO_CHAR(time,'YYYY-MM-DD') as timee
             FROM log where status like '%OK'
             GROUP BY TO_CHAR(time,'YYYY-MM-DD')
             ORDER BY TO_CHAR(time,'YYYY-MM-DD') desc;""")
    ok = c.fetchall()
    POSTS.close()
    for i in range(len(ok)):
        count = 1
        suma = float(ok[i][0])+float(error[i][0])
        if 100 - (ok[i][0]/suma * 100) > 1:
            print count, "." + ok[i][1] + " =", 100 - (ok[i][0]/suma * 100), "percent error."  # noqa
            count += 1


def author_views():
    """Print Authors's name with views."""
    POSTS = psycopg2.connect("dbname=news")
    c = POSTS.cursor()
    c.execute("""select authors.name, sum(views) from authors, articles,
             ( select articles.title as a, count(*) as views from log,articles
             where substr(log.path, 10) = articles.slug group by articles.title
             order by views desc) as b where authors.id = articles.author and a
             = articles.title group by authors.name
             order by sum(views) desc;""")
    articles = c.fetchall()
    for i in range(len(articles)):
        print str(i+1) + ". " + articles[i][0] + " = " + str(articles[i][1]) + " views."  # noqa


print "*" * 60
print "1. What are the most popular three articles of all time?"
print
popular_article()
print
print "*" * 60
print "2. Who are the most popular article authors of all time?"
print
author_views()
print
print "*" * 60
print "3. On which days did more than 1% of requests lead to errors?"
print
log_error()
print
print "*" * 60
print
