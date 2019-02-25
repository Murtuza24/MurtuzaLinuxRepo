#References:https://clasense4.wordpress.com/2012/07/29/python-redis-how-to-cache-python-mysql-result-using-redis/
#https://opensource.com/article/18/4/how-build-hello-redis-with-python
#https://docs.microsoft.com/en-us/azure/redis-cache/cache-python-get-started
#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
from flask import Flask, redirect, render_template, request
import urllib
import datetime
import json
import pypyodbc
import time
import random
import _pickle as pickle
import hashlib
import redis
import pandas as pd
import csv

server = 'sample2401.database.windows.net'
database = 'Sample2401'
username = 'murtuza'
password = 'Maverick123'
driver = '{ODBC Driver 13 for SQL Server}'
app = Flask(__name__)
cacheName = 'testQueryRes'
rd = redis.StrictRedis(host='Earthquake321.redis.cache.windows.net', port=6380, db=0,
                           password='1WjoZK7KGw8H6VUOPROqHpUm19ge2L+y1FvAf5OMUV8=', ssl=True)

@app.route('/')
def home():
    return render_template('index.html')\

# @app.route('/uploadData')
# def uploadData():
#     cnxn = pypyodbc.connect('DRIVER=' + driver + ';SERVER=' + server
#                           + ';PORT=1443;DATABASE=' + database
#                           + ';UID=' + username + ';PWD=' + password)
#     cursor = cnxn.cursor()
#
#     df = pd.read_csv('all_month.csv')
#     for index,row in df.iterrows():
#
#         sql = "Insert into [dbo].[earthquake] \
#         ([time], [latitude], [longitude], [depth], [mag], [magType], [nst], [gap], [dmin], [rms], [net],\
#         [id], [updated], [place], [type], [horizontalError], [depthError], [magError], [magNst], [status],\
#         [locationSource], [magSource]) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
#
#         values = [row['time'], row['latitude'], row['longitude'], row['depth'],
#                   row['mag'], row['magType'], row['nst'], row['gap'], row['dmin'], row['rms'], row['net'],
#                   row['id'], row['updated'], row['place'], row['type'], row['horizontalError'], row['depthError'],
#                   row['magError'], row['magNst'], row['status'], row['locationSource'], row['magSource']]
#
#         # values =[row['time']
#         #         , float(row['latitude']) if row['latitude'] else 0.0
#         #                , float(row['longitude']) if row['longitude'] else 0.0
#         #                , float(row['depth']) if row['depth'] else 0.0
#         #                , float(row['mag']) if row['mag'] else 0.0
#         #                , row['magType']
#         #                , row['nst']
#         #                , float(row['gap']) if row['gap'] else 0.0
#         #                , float(row['dmin']) if row['dmin'] else 0.0
#         #                , float(row['rms']) if row['rms'] else 0.0
#         #                , row['net']
#         #                , row['id']
#         #                , row['updated']
#         #                , row['place']
#         #                , row['type']
#         #                , float(row['horizontalError']) if row['horizontalError'] else 0.0
#         #                , float(row['depthError']) if row['depthError'] else 0.0
#         #                , float(row['magError']) if row['magError'] else 0.0
#         #                , row['magNst'] if row['magNst'] else 0
#         #                , row['status']
#         #                , row['locationSource']
#         #                , row['magSource']]
#
#         cursor.execute(sql,values)
#
#         # cursor.execute("Insert into [dbo].[earthquake] \
#         # ([time], [latitude], [longitude], [depth], [mag], [magType], [nst], [gap], [dmin], [rms], [net],\
#         # [id], [updated], [place], [type], [horizontalError], [depthError], [magError], [magNst], [status],\
#         # [locationSource], [magSource]) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
#         #                , row['time']
#         #                , float(row['latitude']) if row['latitude'] else 0.0
#         #                , float(row['longitude']) if row['longitude'] else 0.0
#         #                , float(row['depth']) if row['depth'] else 0.0
#         #                , float(row['mag']) if row['mag'] else 0.0
#         #                , row['magType']
#         #                , row['nst']
#         #                , float(row['gap']) if row['gap'] else 0.0
#         #                , float(row['dmin']) if row['dmin'] else 0.0
#         #                , float(row['rms']) if row['rms'] else 0.0
#         #                , row['net']
#         #                , row['id']
#         #                , row['updated']
#         #                , row['place']
#         #                , row['type']
#         #                , float(row['horizontalError']) if row['horizontalError'] else 0.0
#         #                , float(row['depthError']) if row['depthError'] else 0.0
#         #                , float(row['magError']) if row['magError'] else 0.0
#         #                , row['magNst'] if row['magNst'] else 0
#         #                , row['status']
#         #                , row['locationSource']
#         #                , row['magSource'])
#     cnxn.commit()
#     cursor.close()
#     cnxn.close()
#     print("Success..!!")
#     return render_template('index.html')

@app.route('/uploadData', methods=['GET'])
def uploadData():
    with open('quakes.csv', mode='r') as csv_file:
        cnxn = pypyodbc.connect('DRIVER=' + driver + ';SERVER=' + server
                              + ';PORT=1443;DATABASE=' + database
                              + ';UID=' + username + ';PWD=' + password)
        cursor = cnxn.cursor()
        csv_reader = csv.DictReader(csv_file)
        line_count = 0
        starttime = time.time()
        for row in csv_reader:
            sql = "Insert into [dbo].[earthquake] \
                    ([time], [latitude], [longitude], [depth], [mag], [magType], [nst], [gap], [dmin], [rms], [net],\
                    [id], [updated], [place], [type], [horizontalError], [depthError], [magError], [magNst], [status],\
                    [locationSource], [magSource]) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
            values = [row['time'], row['latitude'], row['longitude'], row['depth'],
                        row['mag'], row['magType'], row['nst'], row['gap'], row['dmin'], row['rms'], row['net'],
                        row['id'], row['updated'], row['place'], row['type'], row['horizontalError'], row['depthError'],
                                row['magError'], row['magNst'], row['status'], row['locationSource'], row['magSource']]
            cursor.execute(sql, values)
            cursor.commit()
            line_count = line_count + 1
            print("updated record number {}".format(line_count))
        endtime = time.time()
        duration = endtime - starttime
        print(duration)

    return render_template("index.html", time=duration)


@app.route('/search', methods=['GET'])
def search():
    stateCode = request.args.get('stateCode')
    year = request.args.get('year')
    cnxn = pypyodbc.connect('DRIVER=' + driver + ';SERVER=' + server
                            + ';PORT=1443;DATABASE=' + database
                            + ';UID=' + username + ';PWD=' + password)

    cursor = cnxn.cursor()
    starttime = time.time()
    sql = "select STATECODE.STATENAME,["+year+"] \
                    FROM STATECODE \
                    INNER JOIN POPULATION \
                    ON STATECODE.STATENAME = POPULATION.STATE \
                    where stateid = '"+stateCode+"'"
    cursor.execute(sql)
    rows = cursor.fetchall()
    endtime = time.time()
    duration = endtime - starttime
    cursor.close()
    cnxn.close()
    return render_template('population.html', ci=rows, time=duration)

@app.route('/yearAndPop', methods=['GET'])
def yearAndPop():
    pop1 = request.args.get('pop1')
    pop2 = request.args.get('pop2')
    year = request.args.get('year')
    cnxn = pypyodbc.connect('DRIVER=' + driver + ';SERVER=' + server
                            + ';PORT=1443;DATABASE=' + database
                            + ';UID=' + username + ';PWD=' + password)

    cursor = cnxn.cursor()
    starttime = time.time()
    sql = "select state,["+year+"] FROM population where ["+year+"] between "+pop1+" and "+pop2

    cursor.execute(sql)
    rows = cursor.fetchall()
    endtime = time.time()
    duration = endtime - starttime
    cursor.close()
    cnxn.close()
    return render_template('population.html', ci=rows, time=duration)

@app.route('/quakeRangeRedis', methods=['GET'])
def quakeRangeRedis():
    if rd.exists(cacheName):
        print('Found Cache!')
        start_time = time.time()
        results = pickle.loads(rd.get(cacheName))
        end_time = time.time()
        message = 'This is from cache'
        rd.flushdb()
    else:
        print('Cache Not Found!')
        cnxn = pypyodbc.connect('DRIVER=' + driver + ';SERVER=' + server
                                + ';PORT=1443;DATABASE=' + database
                                + ';UID=' + username + ';PWD=' + password)
        cursor = cnxn.cursor()

        start_time = time.time()
        cursor.execute("select STATECODE.STATENAME,POPULATION.POP11 \
        FROM STATECODE INNER JOIN POPULATION \
        ON STATECODE.STATENAME = POPULATION.STATE")



        columns = [column[0] for column in cursor.description]

        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
        end_time = time.time()

        cursor.close()
        cnxn.close()

        rd.delete(cacheName)
        #r.set( cacheName, results)
        #r.get('foo')
        rd.set(cacheName, pickle.dumps(results))
        message = "This is from database"

    total_time = end_time - start_time
    return render_template('population.html', ci=results, time=total_time, msg= message)

@app.route('/multipleRedis', methods=['GET'])
def multipleRedis():
    stateCode = request.args.get('stateCode')
    year = request.args.get('year')
    count = request.args.get('count')
    if rd.exists(cacheName):
        print('Found Cache!')
        start_time = time.time()
        for i in range(0, int(count)):
            results = pickle.loads(rd.get(cacheName))
        end_time = time.time()
        message = 'This is from cache'
        rd.flushdb()
    else:
        print('Cache Not Found!')

        cnxn = pypyodbc.connect('DRIVER=' + driver + ';SERVER=' + server
                                + ';PORT=1443;DATABASE=' + database
                                + ';UID=' + username + ';PWD=' + password)

        cursor = cnxn.cursor()
        start_time = time.time()
        for i in range(0, int(count)):
            # random1 = round(random.uniform(float(magn), float(magn1)), 3)
            # print(random1)
            sql = "select STATECODE.STATENAME,[" + year + "] \
                                          FROM STATECODE \
                                          INNER JOIN POPULATION \
                                          ON STATECODE.STATENAME = POPULATION.STATE \
                                          where stateid = '" + stateCode + "'"
            cursor.execute(sql)

        columns = [column[0] for column in cursor.description]

        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
        end_time = time.time()

        cursor.close()
        cnxn.close()

        # rd.delete(cacheName)
        # r.set( cacheName, results)
        # r.get('foo')
        rd.set(cacheName, pickle.dumps(results))
        message = "This is from database"

    total_time = end_time - start_time
    return render_template('population.html', ci=results, time=total_time, msg= message)



@app.route('/quakerange', methods=['GET'])
def quakerange():
    # connect to DB2
    sql="select * from earthquake".encode('utf-8')
    magn = float(request.args.get('mag'))
    magn1 = float(request.args.get('mag1'))

    cnxn = pypyodbc.connect('DRIVER=' + driver + ';SERVER=' + server
                            + ';PORT=1443;DATABASE=' + database
                            + ';UID=' + username + ';PWD=' + password)

    cursor = cnxn.cursor()



    starttime = time.time()
    for i in range(0,1500):
        random1 = round(random.uniform(float(magn),float(magn1)),3)
        hash = hashlib.sha224(sql).hexdigest()
        key = "sql_cache:" + hash
        if (rd.get(key)):
            print ("This was return from redis")
        else:
            cursor.execute("select * from earthquake where mag>'"+ str(random1) +"'")
            data = cursor.fetchall()

            rows1=[]
            for x in data:
                rows1.append(str(x))
                rd.set(key,pickle.dumps(list(rows1)))
        # Put data into cache for 1 hour
                rd.expire(key, 36)
                print ("This is the cached data")
    endtime = time.time()
        # Note that for security reasons we are preparing the statement first,
        # then bind the form input as value to the statement to replace the
        # parameter marker.

    duration = endtime - starttime
    return render_template('city.html',ci=rows1, time=duration)

@app.route('/quakeMultipleQuery', methods=['GET'])
def quakeMultipleQuery():
    # connect to DB2
    stateCode = request.args.get('stateCode')
    year = request.args.get('year')
    count = request.args.get('count')

    cnxn = pypyodbc.connect('DRIVER=' + driver + ';SERVER=' + server
                            + ';PORT=1443;DATABASE=' + database
                            + ';UID=' + username + ';PWD=' + password)


    cursor = cnxn.cursor()
    starttime = time.time()
    for i in range(0,int(count)):
          # random1 = round(random.uniform(float(magn),float(magn1)),3)
          # print(random1)
          sql = "select STATECODE.STATENAME,[" + year + "] \
                              FROM STATECODE \
                              INNER JOIN POPULATION \
                              ON STATECODE.STATENAME = POPULATION.STATE \
                              where stateid = '" + stateCode + "'"
          cursor.execute(sql)

    rows=[]
    rows = cursor.fetchall()
    print(rows)
    endtime = time.time()
    duration = endtime - starttime
    return render_template('population.html', ci=rows, time=duration)

if __name__ == '__main__':
    app.run(debug=True)
