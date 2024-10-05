import mariadb
import pymongo
import sys
import pandas as pd
import time
import datetime
import re
import dateutil



#Παναγιώτης Χρονόπουλος , ΑΜ : 321/2015222

def ConvertSqlFileToCsvFileUser():

    firstcsvfile = open("yelp_academic_dataset_user_part_2.csv" , "w" , encoding = "utf-8")

    firstconnection = mariadb.connect(user = "root" , password = "mypass" , host = "localhost" , port = 3306 , database = "MariaDB")

    firstcursor = firstconnection.cursor()

    firstcursor.execute("SELECT * FROM user")

    colnames = [desc[0] for desc in firstcursor.description]

    while True:

        firstdataframe = pd.DataFrame(firstcursor.fetchmany(1000))

        if len(firstdataframe) == 0:

            break

        else:

            firstdataframe.to_csv(firstcsvfile , header = colnames , encoding = "utf-8" , chunksize = 1000 )

    firstcsvfile.close()

    print("\n Convert was successfully done!")

    firstcursor.close()

    firstconnection.close()

def ConvertSqlFileToCsvFileReview():

    secondcsvfile = open("yelp_academic_dataset_review_part_2.csv" , "w" , encoding = "utf-8")

    secondconnection = mariadb.connect(user = "root" , password = "mypass" , host = "localhost" , port = 3306 , database = "MariaDB")

    secondcursor = secondconnection.cursor()

    secondcursor.execute("SELECT * FROM review")

    colnames = [desc[0] for desc in secondcursor.description]

    while True:

        seconddataframe = pd.DataFrame(secondcursor.fetchmany(1000))

        if len(seconddataframe) == 0:

            break

        else:

            seconddataframe.to_csv(secondcsvfile , header = colnames , encoding = "utf-8" , escapechar = "\\" , chunksize = 1000 )

    secondcsvfile.close()

    print("\n Convert was successfully done!")

    secondcursor.close()

    secondconnection.close()

def MariaDBConnectorUser():

    try:

        thirdconnection = mariadb.connect(user = "root" , password = "mypass" , host = "localhost" , port = 3306)

        print("\n Connected to MariaDB database!")

    except mariadb.Error as e:

        print("\n Error connecting to MariaDB database!")

        sys.exit(1)

    thirdcursor = thirdconnection.cursor()

    thirdcursor.execute("CREATE DATABASE IF NOT EXISTS MariaDB CHARACTER SET 'utf8mb4' COLLATE 'utf8mb4_general_ci'")

    thirdcursor.close()

    thirdconnection.close()

    try:

        thirdcsvfile = pd.read_csv("yelp_academic_dataset_user_part_2.csv" , sep = "," , engine = "python")

        thirddataframe = pd.DataFrame(thirdcsvfile)

        print("\n" , thirddataframe)

        fourthconnection = mariadb.connect(user = "root" , password = "mypass" , host = "localhost" , port = 3306 , database = "MariaDB")

    except mariadb.Error as e:

        print("\n Error connecting to MariaDB database!")

        sys.exit(1)

    fourthcursor = fourthconnection.cursor()

    fourthcursor.execute("CREATE TABLE IF NOT EXISTS user(user_id VARCHAR(50) NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',name VARCHAR(200) NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',review_count INT(11) NULL DEFAULT NULL,yelping_since DATETIME NULL DEFAULT NULL,useful INT(11) NULL DEFAULT NULL,funny INT(11) NULL DEFAULT NULL,cool INT(11) NULL DEFAULT NULL,elite VARCHAR(200) NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',friends TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',fans INT(11) NULL DEFAULT NULL,average_stars FLOAT NULL DEFAULT NULL,compliment_hot INT(11) NULL DEFAULT NULL,compliment_more INT(11) NULL DEFAULT NULL,compliment_profile INT(11) NULL DEFAULT NULL,compliment_cute INT(11) NULL DEFAULT NULL,compliment_list INT(11) NULL DEFAULT NULL,compliment_note INT(11) NULL DEFAULT NULL,compliment_plain INT(11) NULL DEFAULT NULL,compliment_cool INT(11) NULL DEFAULT NULL,compliment_funny INT(11) NULL DEFAULT NULL,compliment_writer INT(11) NULL DEFAULT NULL,compliment_photos INT(11) NULL DEFAULT NULL)COLLATE='utf8mb4_general_ci'ENGINE=InnoDB")

    try:

        for datafile in thirddataframe.itertuples():

            fourthcursor.execute("INSERT IGNORE INTO user(user_id,name,review_count,yelping_since,useful,funny,cool,elite,friends,fans,average_stars,compliment_hot,compliment_more,compliment_profile,compliment_cute,compliment_list,compliment_note,compliment_plain,compliment_cool,compliment_funny,compliment_writer,compliment_photos) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",(datafile.user_id,datafile.name,datafile.review_count,datafile.yelping_since,datafile.useful,datafile.funny,datafile.cool,datafile.elite,datafile.friends,datafile.fans,datafile.average_stars,datafile.compliment_hot,datafile.compliment_more,datafile.compliment_profile,datafile.compliment_cute,datafile.compliment_list,datafile.compliment_note,datafile.compliment_plain,datafile.compliment_cool,datafile.compliment_funny,datafile.compliment_writer,datafile.compliment_photos))

            print("\n Documents inserted successfully!")

            break

    except mariadb.Error as e:

        print("\n Error!")

    fourthconnection.commit()

    fourthcursor.execute("SELECT * FROM user")

    for datamariadb0 in fourthcursor:

        print("\n" , datamariadb0)

        break

    fourthcursor.close()

    fourthconnection.close()

def MariaDBConnectorReview():

    try:

        fifthconnection = mariadb.connect(user = "root" , password = "mypass" , host = "localhost" , port = 3306)

        print("\n Connected to MariaDB database!")

    except mariadb.Error as e:

        print("\n Error connecting to MariaDB database!")

        sys.exit(1)

    fifthcursor = fifthconnection.cursor()

    fifthcursor.execute("CREATE DATABASE IF NOT EXISTS MariaDB CHARACTER SET 'utf8mb4' COLLATE 'utf8mb4_general_ci'")

    fifthcursor.close()

    fifthconnection.close()

    try:

        fourthcsvfile = pd.read_csv("yelp_academic_dataset_review_part_2.csv" , sep = "," , engine = "c")

        fourthdataframe = pd.DataFrame(fourthcsvfile)

        print("\n" , fourthdataframe)

        sixthconnection = mariadb.connect(user = "root" , password = "mypass" , host = "localhost" , port = 3306 , database = "MariaDB")

    except mariadb.Error as e:

        print("\n Error connecting to MariaDB database!")

        sys.exit(1)

    sixthcursor = sixthconnection.cursor()

    sixthcursor.execute("CREATE TABLE IF NOT EXISTS review(review_id VARCHAR(50) NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',user_id VARCHAR(50) NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',business_id VARCHAR(50) NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',stars INT(11) NULL DEFAULT NULL,useful INT(11) NULL DEFAULT NULL,funny INT(11) NULL DEFAULT NULL,cool INT(11) NULL DEFAULT NULL,text TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',date DATETIME NULL DEFAULT NULL)COLLATE='utf8mb4_general_ci'ENGINE=InnoDB")

    try:

        for datafile in fourthdataframe.itertuples():

            sixthcursor.execute("INSERT IGNORE INTO review(review_id,user_id,business_id,stars,useful,funny,cool,text,date) VALUES (?,?,?,?,?,?,?,?,?)",(datafile.review_id,datafile.user_id,datafile.business_id,datafile.stars,datafile.useful,datafile.funny,datafile.cool,datafile.text,datafile.date))

            print("\n Documents inserted successfully!")

            break

    except mariadb.Error as e:

        print("\n Error!")

    sixthconnection.commit()

    sixthcursor.execute("SELECT * FROM review")

    for datamariadb1 in sixthcursor:

        print("\n" , datamariadb1)

        break

    sixthcursor.close()

    sixthconnection.close()

def MongoDBConnectorUser():

    client = pymongo.MongoClient()

    MongoDB = client["MongoDB"]

    user = MongoDB["user"]

    print("\n Connected to MongoDB database!")

    if MongoDB.user.count_documents({}) == 0:

        firstdataframe = pd.read_json("yelp_academic_dataset_user_part_1.json" , lines = True)

        firstdatafile = firstdataframe.to_dict(orient = "records")

        MongoDB.user.insert_many(firstdatafile)

        print("\n Documents inserted successfully!")

    else:

        print("\n Documents are already exist!")

def MongoDBConnectorReview():

    client = pymongo.MongoClient()

    MongoDB = client["MongoDB"]

    review = MongoDB["review"]

    print("\n Connected to MongoDB database!")

    if MongoDB.review.count_documents({}) == 0:

        seconddataframe = pd.read_json("yelp_academic_dataset_review_part_1.json" , lines = True)

        seconddatafile = seconddataframe.to_dict(orient = "records")

        MongoDB.review.insert_many(seconddatafile)

        print("\n Documents inserted successfully!")

    else:

        print("\n Documents are already exist!")

def MongoDBConnectorBusiness():

    client = pymongo.MongoClient()

    MongoDB = client["MongoDB"]

    business = MongoDB["business"]

    print("\n Connected to MongoDB database!")

    if MongoDB.business.count_documents({}) == 0:

        thirddataframe = pd.read_json("yelp_academic_dataset_business.json" , lines = True)

        thirddatafile = thirddataframe.to_dict(orient = "records")

        MongoDB.business.insert_many(thirddatafile)

        print("\n Documents inserted successfully!")

    else:

        print("\n Documents are already exist!")

def MongoDBConnectorCheckin():

    client = pymongo.MongoClient()

    MongoDB = client["MongoDB"]

    checkin = MongoDB["checkin"]

    print("\n Connected to MongoDB database!")

    if MongoDB.checkin.count_documents({}) == 0:

        fourthdataframe = pd.read_json("yelp_academic_dataset_checkin.json" , lines = True)

        fourthdatafile = fourthdataframe.to_dict(orient = "records")

        MongoDB.checkin.insert_many(fourthdatafile)

        print("\n Documents inserted successfully!")

    else:

        print("\n Documents are already exist!")

def MongoDBConnectorTip():

    client = pymongo.MongoClient()

    MongoDB = client["MongoDB"]

    tip = MongoDB["tip"]

    print("\n Connected to MongoDB database!")

    if MongoDB.tip.count_documents({}) == 0:

        fifthdataframe = pd.read_json("yelp_academic_dataset_tip.json" , lines = True)

        fifthdatafile = fifthdataframe.to_dict(orient = "records")

        MongoDB.tip.insert_many(fifthdatafile)

        print("\n Documents inserted successfully!")

    else:

        print("\n Documents are already exist!")

    #for datamongodb0 in MongoDB.tip.find():

        #print("\n The collections are :" , datamongodb0)

        #break

def print_q1():

    client = pymongo.MongoClient()

    MongoDB = client["MongoDB"]

    tip = MongoDB["tip"]

    print("\n Connected to MongoDB database!")

    #for datamongodb1 in MongoDB.tip.find({"compliment_count": {"$gte": 4}}):

        #print("\n The documents of tip is :" , datamongodb1)

    if MongoDB.tip.count_documents({}) != 0:

        starttime = time.time()

        numberofdocuments = MongoDB.tip.count_documents({"compliment_count": {"$gte": 4}})

        endtime = time.time()

        totaltime = str(datetime.timedelta(seconds = (endtime - starttime)))

        print("\n The documents of tip is :" , numberofdocuments)

        print("\n The time running of search is :" , totaltime)

    else:

        print("\n There aren't any documents or the collection isn't exist!")

def print_q2():

    client = pymongo.MongoClient()

    MongoDB = client["MongoDB"]

    tip = MongoDB["tip"]

    business = MongoDB["business"]

    print("\n Connected to MongoDB database!")

    if MongoDB.tip.count_documents({}) != 0 and MongoDB.business.count_documents({}) != 0:

        starttime = time.time()

        for datamongodb2 in MongoDB.tip.find({"compliment_count": {"$gte": 4}} , {"_id": 0 , "business_id": 1}):

            #print("\n The company of tip is :" , datamongodb2)

            for datamongodb3 in MongoDB.business.find(datamongodb2 , {"_id": 0 , "name": 1}):

                print("\n The company of business is :" , datamongodb3)

        endtime = time.time()

        totaltime = str(datetime.timedelta(seconds = (endtime - starttime)))

        print("\n The time running of search is :" , totaltime)

    else:

        print("\n There aren't any documents or the collection isn't exist!")

def print_q3():

    client = pymongo.MongoClient()

    MongoDB = client["MongoDB"]

    tip = MongoDB["tip"]

    print("\n Connected to MongoDB database!")

    if MongoDB.tip.count_documents({}) != 0:

        #for datamongodb4 in MongoDB.tip.find({"compliment_count": {"$gte": 4}} , {"_id": 0 , "compliment_count": 1}):

            #print("\n The compliment of tip is :" , datamongodb4)

        starttime = time.time()

        for datamongodb5 in MongoDB.tip.aggregate([{"$match": {"compliment_count": {"$gte": 4}}} , {"$group": {"_id": "null" , "compliment_count": {"$sum": "$compliment_count"}}}]):

            print("\n The total compliments of tip is :" , datamongodb5)

            datamongodb6 = re.sub("[^0-9]" , "" , str(datamongodb5))

            print("\n The total compliments of tip is :" , int(datamongodb6))

        endtime = time.time()

        totaltime = str(datetime.timedelta(seconds = (endtime - starttime)))

        print("\n The time running of search is :" , totaltime)

    else:

        print("\n There aren't any documents or the collection isn't exist!")

def print_q4():

    client = pymongo.MongoClient()

    MongoDB = client["MongoDB"]

    business = MongoDB["business"]

    print("\n Connected to MongoDB database!")

    if MongoDB.business.count_documents({}) != 0:

        starttime = time.time()

        for datamongodb7 in MongoDB.business.find({"attributes.HappyHour": "True"} , {"_id": 0 , "name": 1 , "latitude": 1 , "longitude": 1}):

            print("\n The company of business is :" , datamongodb7)

        endtime = time.time()

        totaltime = str(datetime.timedelta(seconds = (endtime - starttime)))

        print("\n The time running of search is :" , totaltime)

    else:

        print("\n There aren't any documents or the collection isn't exist!")

def print_q5():

    client = pymongo.MongoClient()

    MongoDB = client["MongoDB"]

    checkin = MongoDB["checkin"]

    business = MongoDB["business"]

    print("\n Connected to MongoDB database!")

    if MongoDB.checkin.count_documents({}) != 0 and MongoDB.business.count_documents({}) != 0:

        starttime = time.time()

        for datamongodb8 in MongoDB.checkin.find({} , {"_id": 0 , "business_id": 1}).limit(12):

            #print("\n The checkin is :" , datamongodb8)

            for datamongodb9 in MongoDB.business.find(datamongodb8 , {"_id": 0 , "name": 1}):

                print("\n The name of business is :" , datamongodb9)

        endtime = time.time()

        totaltime = str(datetime.timedelta(seconds = (endtime - starttime)))

        print("\n The time running of search is :" , totaltime)

    else:

        print("\n There aren't any documents or the collection isn't exist!")

def print_q6():

    client = pymongo.MongoClient()

    MongoDB = client["MongoDB"]

    review = MongoDB["review"]

    print("\n Connected to MongoDB database!")

    if MongoDB.review.count_documents({}) != 0:

        starttime = time.time()

        datamongodb10 = MongoDB.review.count_documents({"stars": {"$gt": 4}})

        endtime = time.time()

        #totaltime = str(datetime.timedelta(seconds = (endtime - starttime)))

        #print("\n The users of review is :" , datamongodb10)

        #print("\n The time running of search is :" , totaltime)

    else:

        print("\n There aren't any documents or the collection isn't exist!")

    try:

        seventhconnection = mariadb.connect(user = "root" , password = "mypass" , host = "localhost" , port = 3306 , database = "MariaDB")

        print("\n Connected to MariaDB database!")

    except mariadb.Error as e:

        print("\n Error connecting to MariaDB database!")

        sys.exit(1)

    seventhcursor = seventhconnection.cursor()

    starttime1 = time.time()

    seventhcursor.execute("SELECT COUNT(user_id) FROM review WHERE stars>4")

    endtime1 = time.time()

    for datamariadb2 in seventhcursor:

        datamariadb3 = re.sub("[^0-9]" , "" , str(datamariadb2))

        #print("\n" , datamariadb2)

    #totaltime1 = str(datetime.timedelta(seconds = (endtime1 - starttime1)))

    #print("\n The users of review is :" , int(datamariadb3))

    #print("\n The time running of search is :" , totaltime1)

    seventhcursor.close()

    seventhconnection.close()

    totalnumberofusers = datamongodb10 + int(datamariadb3) 

    totaltime2 = str(datetime.timedelta(seconds = (endtime1 - starttime1) + (endtime - starttime)))

    print("\n The users of review is :" , totalnumberofusers)

    print("\n The time running of search is :" , totaltime2)

def print_q7():

    client = pymongo.MongoClient()

    MongoDB = client["MongoDB"]

    review = MongoDB["review"]

    print("\n Connected to MongoDB database!")

    if MongoDB.review.count_documents({}) != 0:

        #for datamongodb11 in MongoDB.review.find({} , {"_id": 0 , "stars": 1}):

            #print("\n The stars of review is :" , datamongodb11)

        starttime = time.time()

        for datamongodb12 in MongoDB.review.aggregate([{"$group": {"_id": "null" , "stars": {"$avg": "$stars"}}}]):

            #print("\n The average stars of review is :" , datamongodb12)

            datamongodb13 = re.sub("[^0-9]" , "" , str(datamongodb12))

            #print("\n The average stars of review is :" , int(datamongodb13))

        endtime = time.time()

        totaltime = str(datetime.timedelta(seconds = (endtime - starttime)))

        #print("\n The time running of search is :" , totaltime)

    else:

        print("\n There aren't any documents or the collection isn't exist!")

    try:

        eighthconnection = mariadb.connect(user = "root" , password = "mypass" , host = "localhost" , port = 3306 , database = "MariaDB")

        print("\n Connected to MariaDB database!")

    except mariadb.Error as e:

        print("\n Error connecting to MariaDB database!")

        sys.exit(1)

    eighthcursor = eighthconnection.cursor()

    starttime1 = time.time()

    eighthcursor.execute("SELECT AVG(stars) FROM review")

    endtime1 = time.time()

    for datamariadb4 in eighthcursor:

        datamariadb5 = re.sub("[^0-9]" , "" , str(datamariadb4))

        #print("\n" , datamariadb4)

    totaltime1 = str(datetime.timedelta(seconds = (endtime1 - starttime1)))

    #print("\n The average stars of review is :" , int(datamariadb5))

    #print("\n The time running of search is :" , totaltime1)

    eighthcursor.close()

    eighthconnection.close()

    totalnumberofaveragestars = int(datamongodb13) + int(datamariadb5) 

    totaltime2 = str(datetime.timedelta(seconds = (endtime1 - starttime1) + (endtime - starttime)))

    print("\n The total average stars of review is :" , totalnumberofaveragestars)

    print("\n The time running of search is :" , totaltime2)

def print_q8():
    
    client = pymongo.MongoClient()

    MongoDB = client["MongoDB"]

    user = MongoDB["user"]

    print("\n Connected to MongoDB database!")

    if MongoDB.user.count_documents({}) != 0:

        #for datamongodb14 in MongoDB.user.find({"$or": [{"useful": {"$gte": 1}} , {"funny": {"$gte": 1}} , {"cool": {"$gte": 1}}]} , {"_id": 0 , "average_stars": 1}):

            #print("\n The  average stars of user is :" , datamongodb14)

        starttime = time.time()

        for datamongodb15 in MongoDB.user.aggregate([{"$match": {"$or": [{"useful": {"$gte": 1}} , {"funny": {"$gte": 1}} , {"cool": {"$gte": 1}}]}} , {"$group": {"_id": "null" , "average_stars": {"$sum": "$average_stars"}}}]):

            #print("\n The average stars of user is :" , datamongodb15)

            datamongodb16 = re.sub("[^0-9]" , "" , str(datamongodb15))

            #print("\n The average stars of user is :" , float(datamongodb16))

        endtime = time.time()

        totaltime = str(datetime.timedelta(seconds = (endtime - starttime)))

        #print("\n The time running of search is :" , totaltime)
        
    else:

        print("\n There aren't any documents or the collection isn't exist!")

    try:

        ninethconnection = mariadb.connect(user = "root" , password = "mypass" , host = "localhost" , port = 3306 , database = "MariaDB")

        print("\n Connected to MariaDB database!")

    except mariadb.Error as e:

        print("\n Error connecting to MariaDB database!")

        sys.exit(1)

    ninethcursor = ninethconnection.cursor()

    starttime1 = time.time()

    ninethcursor.execute("SELECT SUM(average_stars) FROM user WHERE useful>=1 OR funny>=1 OR cool>=1")

    endtime1 = time.time()

    for datamariadb6 in ninethcursor:

        datamariadb7 = re.sub("[^0-9]" , "" , str(datamariadb6))

        #print("\n" , datamariadb6)

    totaltime1 = str(datetime.timedelta(seconds = (endtime1 - starttime1)))

    #print("\n The average stars of user is :" , float(datamariadb7))

    #print("\n The time running of search is :" , totaltime1)

    ninethcursor.close()

    ninethconnection.close()

    totalnumberofaveragestars = float(datamongodb16) + float(datamariadb7) 

    totaltime2 = str(datetime.timedelta(seconds = (endtime1 - starttime1) + (endtime - starttime)))

    print("\n The total average stars of user is :" , totalnumberofaveragestars)

    print("\n The time running of search is :" , totaltime2)

def print_q9():

    client = pymongo.MongoClient()

    MongoDB = client["MongoDB"]

    review = MongoDB["review"]

    user = MongoDB["user"]

    print("\n Connected to MongoDB database!")

    if MongoDB.review.count_documents({}) != 0 and MongoDB.user.count_documents({}) != 0:

        starttime = time.time()

        for datamongodb17 in MongoDB.review.find({"$and": [{"stars": 5} , {"useful": {"$gte": 3}}]} , {"_id": 0 , "user_id": 1}).limit(10):

            #print("\n The id of user is :" , datamongodb17)

            for datamongodb18 in MongoDB.user.find(datamongodb17 , {"_id": 0 , "name": 1}).limit(10):

                print("\n The name of user is :" , datamongodb18)

        endtime = time.time()

        #totaltime = str(datetime.timedelta(seconds = (endtime - starttime)))

        #print("\n The time running of search is :" , totaltime)

    else:

        print("\n There aren't any documents or the collection isn't exist!")

    try:

        tenthconnection = mariadb.connect(user = "root" , password = "mypass" , host = "localhost" , port = 3306 , database = "MariaDB")

        print("\n Connected to MariaDB database!")

    except mariadb.Error as e:

        print("\n Error connecting to MariaDB database!")

        sys.exit(1)

    tenthcursor = tenthconnection.cursor()

    starttime1 = time.time()

    tenthcursor.execute("SELECT u.name FROM user u INNER JOIN review r ON u.user_id=r.user_id WHERE r.stars=5 AND r.useful>=3 LIMIT 10")

    endtime1 = time.time()

    for datamariadb8 in tenthcursor:

        datamariadb9 = re.sub(r"[^\w\s]" , "" , str(datamariadb8))

        print("\n The name of user is :" , datamariadb9)

    #totaltime1 = str(datetime.timedelta(seconds = (endtime1 - starttime1)))

    #print("\n The time running of search is :" , totaltime1)

    tenthcursor.close()

    tenthconnection.close()

    totaltime2 = str(datetime.timedelta(seconds = (endtime1 - starttime1) + (endtime - starttime)))

    print("\n The time running of search is :" , totaltime2)

def print_q10():

    friendlistmongodb = list()

    friendlistmariadb = list()

    client = pymongo.MongoClient()

    MongoDB = client["MongoDB"]

    review = MongoDB["review"]

    user = MongoDB["user"]

    print("\n Connected to MongoDB database!")

    if MongoDB.review.count_documents({}) != 0 and MongoDB.user.count_documents({}) != 0:

        starttime = time.time()

        for datamongodb19 in MongoDB.review.find({"$and": [{"stars": 5} , {"useful": {"$gte": 3}}]} , {"_id": 0 , "user_id": 1}).limit(10):

            #print("\n The id of user is :" , datamongodb19)

            for datamongodb20 in MongoDB.user.find(datamongodb19 , {"_id": 0 , "friends": 1}).limit(10):

                datamongodb21 = str(datamongodb20)

                datamongodb22 = datamongodb21.split(",")

                datamongodb23 = len(datamongodb22)

                #print("\n The id of friends are:" , datamongodb21)

                #print("\n The number of friends is:" , int(datamongodb23))

                friendlistmongodb.append(int(datamongodb23))

        endtime = time.time()

        #print("\n The length of list is:" , len(friendlistmongodb))

        #print("\n The sum of list is:" , sum(friendlistmongodb))

        #print("\n The average of friends is :" , float(sum(friendlistmongodb)/len(friendlistmongodb)))

        #totaltime = str(datetime.timedelta(seconds = (endtime - starttime)))

        #print("\n The time running of search is :" , totaltime)

    else:

        print("\n There aren't any documents or the collection isn't exist!")

    try:

        eleventhconnection = mariadb.connect(user = "root" , password = "mypass" , host = "localhost" , port = 3306 , database = "MariaDB")

        print("\n Connected to MariaDB database!")

    except mariadb.Error as e:

        print("\n Error connecting to MariaDB database!")

        sys.exit(1)

    eleventhcursor = eleventhconnection.cursor()

    starttime1 = time.time()

    eleventhcursor.execute("SELECT u.friends FROM user u INNER JOIN review r ON u.user_id=r.user_id WHERE r.stars=5 AND r.useful>=3 LIMIT 10")

    endtime1 = time.time()

    for datamariadb10 in eleventhcursor:

        datamariadb11 = str(datamariadb10)

        datamariadb12 = datamariadb11[0:-2]

        datamariadb13 = datamariadb12.split(",")

        datamariadb14 = len(datamariadb13)

        #print("\n The id of friends are:" , datamariadb12)

        #print("\n The number of friends is:" , int(datamariadb14))

        friendlistmariadb.append(int(datamariadb14))

    #print("\n The length of list is:" , len(friendlistmariadb))

    #print("\n The sum of list is:" , sum(friendlistmariadb))

    #print("\n The average of friends is :" , float(sum(friendlistmariadb)/len(friendlistmariadb)))

    #totaltime1 = str(datetime.timedelta(seconds = (endtime1 - starttime1)))

    #print("\n The time running of search is :" , totaltime1)

    eleventhcursor.close()

    eleventhconnection.close()

    totalaverageoffriends = (float(sum(friendlistmongodb))/(len(friendlistmongodb))) + (float(sum(friendlistmariadb))/(len(friendlistmariadb)))

    friendlistmongodb.clear()

    friendlistmariadb.clear()

    print("\n The total average of friends is :" , totalaverageoffriends)

    totaltime2 = str(datetime.timedelta(seconds = (endtime1 - starttime1) + (endtime - starttime)))

    print("\n The time running of search is :" , totaltime2)

def print_q11():

    client = pymongo.MongoClient()

    MongoDB = client["MongoDB"]

    review = MongoDB["review"]

    user = MongoDB["user"]

    print("\n Connected to MongoDB database!")

    if MongoDB.review.count_documents({}) != 0 and MongoDB.user.count_documents({}) != 0:

        date = "2013-01-01T00:00:00.000Z"

        convertdate = dateutil.parser.parse(date)

        starttime = time.time()

        for datamongodb24 in MongoDB.review.find({"$and": [{"stars": 5} , {"date": {"$lt": convertdate}}]} , {"_id": 0 , "user_id": 1}).limit(10):

            #print("\n The id of user is :" , datamongodb24)

            for datamongodb25 in MongoDB.user.find(datamongodb24 , {"_id": 0 , "name": 1 , "yelping_since": 1 , "friends": 1}).limit(10):

                print("\n The name , yelping_since and his/her friends of user is :" , datamongodb25)

        endtime = time.time()

        #totaltime = str(datetime.timedelta(seconds = (endtime - starttime)))

        #print("\n The time running of search is :" , totaltime)

    else:

        print("\n There aren't any documents or the collection isn't exist!")

    try:

        twelvethconnection = mariadb.connect(user = "root" , password = "mypass" , host = "localhost" , port = 3306 , database = "MariaDB")

        print("\n Connected to MariaDB database!")

    except mariadb.Error as e:

        print("\n Error connecting to MariaDB database!")

        sys.exit(1)

    twelvethcursor = twelvethconnection.cursor()

    starttime1 = time.time()

    twelvethcursor.execute("SELECT u.name,DATE_FORMAT(u.yelping_since,'%Y-%m-%d %T'),u.friends FROM user u INNER JOIN review r ON u.user_id=r.user_id WHERE r.stars=5 AND r.date<STR_TO_DATE('2013-01-01 00:00:00','%Y-%m-%d %T') LIMIT 10")

    endtime1 = time.time()

    for datamariadb15 in twelvethcursor:

        print("\n The name , yelping_since and his/her friends of user is :" , datamariadb15)

    #totaltime1 = str(datetime.timedelta(seconds = (endtime1 - starttime1)))

    #print("\n The time running of search is :" , totaltime1)

    twelvethcursor.close()

    twelvethconnection.close()

    totaltime2 = str(datetime.timedelta(seconds = (endtime1 - starttime1) + (endtime - starttime)))

    print("\n The time running of search is :" , totaltime2)

def print_q12():

    client = pymongo.MongoClient()

    MongoDB = client["MongoDB"]

    user = MongoDB["user"]

    review = MongoDB["review"]

    business = MongoDB["business"]

    print("\n Connected to MongoDB database!")

    if MongoDB.user.count_documents({}) != 0 and MongoDB.review.count_documents({}) != 0 and MongoDB.business.count_documents({}) != 0:

        starttime = time.time()

        for datamongodb26 in MongoDB.user.find({"review_count": {"$gt": 200}} , {"_id": 0 , "user_id": 1}).limit(10):

            print("\n The id of user is :" , datamongodb26)

            for datamongodb27 in MongoDB.review.find(datamongodb26 , {"_id": 0 , "business_id": 1}).limit(3):

                #print("\n The business id of review is :" , datamongodb27)

                for datamongodb28 in MongoDB.business.find(datamongodb27 , {"_id": 0 , "address": 1 , "city": 1 , "state": 1 , "postal_code": 1 , "latitude": {"$round": ["$latitude" , 0]} , "longitude": {"$round": ["$longitude" , 0]}}).limit(3):

                    print("\n The geographical location in which they are most active is :" , datamongodb28)

        endtime = time.time()

        #totaltime = str(datetime.timedelta(seconds = (endtime - starttime)))

        #print("\n The time running of search is :" , totaltime)

    else:

        print("\n There aren't any documents or the collection isn't exist!")

    try:

        thirteenthconnection = mariadb.connect(user = "root" , password = "mypass" , host = "localhost" , port = 3306 , database = "MariaDB")

        print("\n Connected to MariaDB database!")

    except mariadb.Error as e:

        print("\n Error connecting to MariaDB database!")

        sys.exit(1)

    thirteenthcursor = thirteenthconnection.cursor()

    starttime1 = time.time()

    thirteenthcursor.execute("SELECT r.business_id FROM review r INNER JOIN user u ON r.user_id=u.user_id WHERE u.review_count>200 LIMIT 10")

    for datamariadb16 in thirteenthcursor:

        #print("\n The id of business is :" , datamariadb16)

        datamariadb17 = str(datamariadb16)

        datamariadb18 = datamariadb17.replace("," , "").replace("'" , "").replace("(" , "").replace(")" , "")

        #print("\n The id of business is :" , datamariadb18)

        for datamongodb29 in MongoDB.business.find({"business_id": datamariadb18} , {"_id": 0 , "address": 1 , "city": 1 , "state": 1 , "postal_code": 1 , "latitude": {"$round": ["$latitude" , 0]} , "longitude": {"$round": ["$longitude" , 0]}}).limit(3):

            print("\n The geographical location in which they are most active is :" , datamongodb29)

    endtime1 = time.time()

    #totaltime1 = str(datetime.timedelta(seconds = (endtime1 - starttime1)))

    #print("\n The time running of search is :" , totaltime1)

    thirteenthcursor.close()

    thirteenthconnection.close()

    totaltime2 = str(datetime.timedelta(seconds = (endtime1 - starttime1) + (endtime - starttime)))

    print("\n The time running of search is :" , totaltime2)

def print_q13():

    client = pymongo.MongoClient()

    MongoDB = client["MongoDB"]

    business = MongoDB["business"]

    print("\n Connected to MongoDB database!")

    if MongoDB.business.count_documents({}) != 0:

        starttime = time.time()

        for datamongodb29 in MongoDB.business.find({"is_open": 1} , {"_id": 0 , "name": 1}).limit(10):

            print("\n The company of business is :" , datamongodb29)

        endtime = time.time()

        totaltime = str(datetime.timedelta(seconds = (endtime - starttime)))

        print("\n The time running of search is :" , totaltime)

    else:

        print("\n There aren't any documents or the collection isn't exist!")

def print_q14():

    client = pymongo.MongoClient()

    MongoDB = client["MongoDB"]

    business = MongoDB["business"]

    print("\n Connected to MongoDB database!")

    if MongoDB.business.count_documents({}) != 0:

        starttime = time.time()

        for datamongodb30 in MongoDB.business.find({"review_count": {"$gt": 10}} , {"_id": 0 , "name": 1}).limit(10):

            print("\n The company of business is :" , datamongodb30)

        endtime = time.time()

        totaltime = str(datetime.timedelta(seconds = (endtime - starttime)))

        print("\n The time running of search is :" , totaltime)

    else:

        print("\n There aren't any documents or the collection isn't exist!")
  
def main():
    
    while(True):
        
        print("\n Applied Structure and Database Issues project.\n")
        print(" 00 - Converting sql file to csv file(yelp academic dataset user part 2).")
        print(" 01 - Converting sql file to csv file(yelp academic dataset review part 2).")
        print(" 02 - Inserting yelp academic dataset user part 2 in MariaDB database.")
        print(" 03 - Inserting yelp academic dataset review part 2 in MariaDB database.")
        print(" 04 - Inserting yelp academic dataset user part 1 in MongoDB database.")
        print(" 05 - Inserting yelp academic dataset review part 1 in MongoDB database.")
        print(" 06 - Inserting yelp academic dataset business in MongoDB database.")
        print(" 07 - Inserting yelp academic dataset checkin in MongoDB database.")
        print(" 08 - Inserting yelp academic dataset tip in MongoDB database.")
        print(" 09 - Πόσα tips έχουν τέσσερα ή περισσότερα compliments;")
        print(" 10 - Ποια είναι τα ονόματα των εταιριών , που έχουν τέσσερα ή περισσότερα compliments;")
        print(" 11 - Για τις παραπάνω επιχειρήσεις (που έχουν τέσσερα ή περισσότερα compliments) , ποιο είναι το άθροισμα όλων των compliments που έχουν.")
        print(" 12 - Ποιο είναι το όνομα και οι συντεταγμένες των επιχειρήσεων που έχουν υπηρεσία “Happy Hour”.")
        print(" 13 - Ποια είναι τα ονόματα των 12 πρώτων επιχειρήσεων σε αριθμό checkin;")
        print(" 14 - Πόσοι χρήστες έχουν κάνει review με περισσότερα από τέσσερα αστεράκια;")
        print(" 15 - Πόσα αστεράκια δίνουν κατά μέσο όρο οι χρήστες στα review τους;")
        print(" 16 - Πόσα κατά μέσο όρο αστεράκια έχουν δώσει οι χρήστες που έχουν τουλάχιστον ένα vote σε useful , funny ή cool;")
        print(" 17 - Ποια είναι τα ονόματα των χρηστών που έχουν δώσει πέντε αστεράκια και το review αυτό ήταν τουλάχιστον τρεις φορές useful;")
        print(" 18 - Πόσους φίλους έχουν κατά μέσο όρο οι χρήστες του ερωτήματος 9;")
        print(" 19 - Εμφανίστε το όνομα , τους φίλους και από πότε βρίσκονται στο Yelp , όσων έχουν κάνει 5 star review , πριν το 2013 , σε επιχειρήσεις στην Αμερική.")
        print(" 20 - Για όσους χρήστες έχουν πάνω από 200 reviews , εμφανίστε τη γεωγραφική τοποθεσία στην οποία είναι περισσότερο ενεργοί.Πιο συγκεκριμένα , τις τρεις πρώτες τοποθεσίες με βάση laktude , longitude , χωρίς τα δεκαδικά ψηφία.Δηλαδή , εάν ένας χρήστης έχει κάνει review σε τοποθεσία (38.551126 , -110.880452) και (38.999999 , -110.000000) , θεωρούμε ότι έχει κάνει δυο reviews στην τοποθεσία (38.000000 , -110.000000) , ή γενικότερα (38 , -110).")
        print(" 21 - Ποια είναι τα ονόματα των επιχειρήσεων που είναι ανοιχτά;")
        print(" 22 - Ποια είναι τα ονόματα των επιχειρήσεων που έχουν review πάνω από 10;")
        print(" 23 - Closing program.")

        try:
            
            choice = str(input("\n Please give a number you want : "))
            
        except:
            
            print("\n You gave an invalid number , please try again.")

        if choice == "0":

            ConvertSqlFileToCsvFileUser()
            
        elif choice == "1":

            ConvertSqlFileToCsvFileReview()

        elif choice == "2":

            MariaDBConnectorUser()

        elif choice == "3":

            MariaDBConnectorReview()

        elif choice == "4":

            MongoDBConnectorUser()

        elif choice == "5":

            MongoDBConnectorReview()

        elif choice == "6":

            MongoDBConnectorBusiness()

        elif choice == "7":

            MongoDBConnectorCheckin()

        elif choice == "8":

            MongoDBConnectorTip()

        elif choice == "9":

            print_q1()

        elif choice == "10":

            print_q2()

        elif choice == "11":

            print_q3()

        elif choice == "12":

            print_q4()

        elif choice == "13":

            print_q5()

        elif choice == "14":

            print_q6()

        elif choice == "15":

            print_q7()

        elif choice == "16":

            print_q8()

        elif choice == "17":

            print_q9()

        elif choice == "18":

            print_q10()

        elif choice == "19":

            print_q11()

        elif choice == "20":

            print_q12()

        elif choice == "21":

            print_q13()

        elif choice == "22":

            print_q14()

        elif choice == "23":
            
            print("\n Closing program.")
            
            sys.exit(0)

        else:
            
            print("\n You gave an invalid number , please try again.")
            
main()
