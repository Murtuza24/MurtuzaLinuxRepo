#References:https://clasense4.wordpress.com/2012/07/29/python-redis-how-to-cache-python-mysql-result-using-redis/
#https://opensource.com/article/18/4/how-build-hello-redis-with-python
#https://docs.microsoft.com/en-us/azure/redis-cache/cache-python-get-started
#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
from flask import Flask, redirect, render_template, request, make_response
import urllib
import datetime
import json
import pyodbc
import time
import random
import pickle
import hashlib
import redis
import csv
import pandas as pd
import pylab as pl
from sklearn.cluster import KMeans
import numpy as np
import matplotlib.pyplot as plt
from io import BytesIO
import base64

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
    return render_template('index.html')

# @app.route('/uploadData')
# def uploadData():
#     cnxn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server
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
        cnxn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server
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

@app.route('/getDemoGraph', methods=['GET'])
def getDemoGraph():
    return render_template('')

@app.route('/yearAndPop', methods=['GET'])
def yearAndPop():
    pop1 = request.args.get('pop1')
    pop2 = request.args.get('pop2')
    year = request.args.get('year')
    cnxn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server
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
        cnxn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server
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

        cnxn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server
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


@app.route('/countCounty')
def getCountCounty():
    code = request.args.get('code', '')

    cnxn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server
                            + ';PORT=1443;DATABASE=' + database
                            + ';UID=' + username + ';PWD=' + password)
    cursor = cnxn.cursor()

    # sql = "SELECT county.state, COUNT(county.county) FROM county, statecode\
    #        where statecode.code = \'"+code+"\' and county.state = statecode.state GROUP BY county.state"

    sql = "SELECT counties.statename, COUNT(counties.countyname) as count1 FROM counties, statecode \
           where statecode.stateid = \'"+code+"\' and counties.statename = statecode.statename GROUP BY counties.statename"

    cursor.execute(sql)

    results = cursor.fetchall()
    cursor.close()
    cnxn.close()

    return render_template('populationn.html', state=code, ci=results[0][1])

@app.route('/quakerange', methods=['GET'])
def quakerange():
    # connect to DB2
    sql="select * from earthquake".encode('utf-8')
    magn = float(request.args.get('mag'))
    magn1 = float(request.args.get('mag1'))

    cnxn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server
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

    cnxn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server
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


#  functions based on quiz4
@app.route('/searchByState', methods=['GET'])
def search():
    stateCode = request.args.get('stateCode')
    # year = request.args.get('year')
    cnxn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server
                            + ';PORT=1443;DATABASE=' + database
                            + ';UID=' + username + ';PWD=' + password)

    cursor = cnxn.cursor()
    starttime = time.time()
    # sql = "select STATECODE.STATENAME,["+year+"] \
    #                 FROM STATECODE \
    #                 INNER JOIN POPULATION \
    #                 ON STATECODE.STATENAME = POPULATION.STATE \
    #                 where stateid = '"+stateCode+"'"

    # sql = "select [2010],[2011],[2012],[2013],[2014],[2015],[2016],[2017],[2018] FROM POPULATION \
    #                 INNER JOIN STATECODE \
    #                 ON STATECODE.STATENAME = POPULATION.STATE \
    #                 where stateid = '"+stateCode+"'"

    sql = "select * from population where state = 'Alabama'"

    cursor.execute(sql)
    rows = cursor.fetchall()
    endtime = time.time()
    duration = endtime - starttime

    # print(rows)
    # data to plot
    n_groups = 9
    # means_frank = (90, 55, 40, 65, 50, 35, 55, 25, 60)
    # means_frank = tuple(rows)
    data = []
    for records in rows:
        # data.append([x for x in records])
        data.append(records['2010'])
        data.append(records['2011'])
        data.append(records['2012'])
        data.append(records['2013'])
        data.append(records['2014'])
        data.append(records['2015'])
        data.append(records['2016'])
        data.append(records['2017'])
        data.append(records['2018'])

    print(data)

    img = BytesIO()
    # create plot
    fig, ax = plt.subplots()
    index = np.arange(n_groups)
    bar_width = 0.45
    opacity = 0.5
    plt.rcParams['figure.figsize'] = (16, 9)
    plt.style.use('ggplot')
    rects1 = plt.bar(index, data, bar_width,
                     alpha=opacity,
                     color='b',
                     label='Pop')


    plt.xlabel('Year')
    plt.ylabel('Population')
    title = 'Population of {}'.format(stateCode)
    plt.title(title)
    plt.xticks(index + bar_width, ('2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018'))
    plt.legend()

    plt.tight_layout()
    # plt.show()
    plt.savefig(img, format='png')
    img.seek(0)
    plot_url = base64.b64encode(img.getvalue())

    response = make_response(img.getvalue())
    response.headers['Content-Type'] = 'image/png'

    cursor.close()
    cnxn.close()

    # return render_template('population.html', ci=rows, time=duration)
    return response

@app.route('/searchByYear')
def searchByYear():
    cnxn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server
                            + ';PORT=1443;DATABASE=' + database
                            + ';UID=' + username + ';PWD=' + password)
    cursor = cnxn.cursor()
    cursor.execute("select [state], [2010] from population")
    rows = cursor.fetchall()

    '''
    # Convert pyodbc row to list
    data = []
    for row in rows:
        data.append([x for x in row])
    '''
    cursor.close()
    cnxn.close()

    x_labels = ()
    y_axis = []
    for i, row in enumerate(rows):
        #print(rows[i][1])
        x_labels = x_labels + (row['state'],)
        y_axis.append(rows[i][1])
    #print(y_axis)



    #objects = ('Python', 'C++', 'Java', 'Perl', 'Scala', 'Lisp')
    y_pos = np.arange(len(x_labels))
    #performance = [10, 8, 6, 4, 2, 1]
    performance = y_axis

    plt.bar(y_pos, performance, align='center', alpha=0.5)
    plt.xticks(y_pos, x_labels)
    plt.ylabel('Population')
    plt.title('Census')
    # plt.show()

    img3 = BytesIO()
    plt.savefig(img3, format='png')

    img3.seek(0)
    plot_url = base64.b64encode(img3.getvalue())
    response = make_response(img3.getvalue())
    response.headers['Content-Type'] = 'image/png'
    return response
    # return render_template('population.html', ci=rows)

@app.route('/kmeansClustering')
def kmeansClustering():

    numberOfClusters = int(request.args.get('numberOfClusters'))

    data_frame = pd.read_csv("quakes.csv")
    img = BytesIO()
    data_frame.head()
    data_frame[['mag', 'depth']].hist()
    plt.show()
    x = data_frame[['mag', 'depth']]
    x = np.array(x)
    print(x)

    kmeans = KMeans(n_clusters=numberOfClusters)

    kmeansoutput_x = kmeans.fit(x)
    print(type(kmeansoutput_x))

    pl.figure('5 Cluster K-Means')

    pl.scatter(x[:, 0], x[:, 1], c=kmeansoutput_x.labels_, cmap='rainbow')

    pl.title('5 Cluster K-Means')
    pl.xlabel('Magnitude')

    pl.ylabel('Depth')

    # pl.show()

    plt.savefig(img, format='png')
    img.seek(0)
    plot_url = base64.b64encode(img.getvalue())

    response = make_response(img.getvalue())
    response.headers['Content-Type'] = 'image/png'

    return response





if __name__ == '__main__':
    app.run(debug=True)
