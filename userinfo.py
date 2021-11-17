from datetime import datetime
from pymongo import MongoClient
import json
from pprint import pprint

client = MongoClient('localhost', 27017)
db = client['userinfo']
db_collection = db['users']


with open('data.json',encoding="utf8") as f:

    file_data = json.load(f)

db_collection.insert_many(file_data)

client.close()

# print(db_collection.count())

find1 = db_collection.find({"$and":[{"dob.age":{"$gt":50}},{"location.city":"گلستان"}]}, {"name.first":1, "name.last":1, "_id":0}) #OK
#for i in find1:
     #pprint(i)



find2 = db_collection.find({"registered.age":{"$gt":20}},{"_id":0, "name.last":1, "location":1,"phone":1})
"""
According to this query, there is no one who has registered more than 20 years.
"""
find2 = db_collection.aggregate([{"$group":{"_id":"$registered.age", "total:":{"$sum":1}}}]) #OK

# for i in find2:
#     pprint(i)



find3=db_collection.aggregate([{ '$match': {'$expr': {'$and': [{ '$eq': [{ '$dayOfMonth': {'date': {'$dateFromString': {'dateString': '$dob.date'}}} }, datetime.now().day] },{ '$eq': [{ '$month':  {'date': {'$dateFromString': {'dateString': '$dob.date'}}} },datetime.now().month ] }]}}}, {'$project':{'name':1, 'email':1, 'location':1, 'dob':1,'_id':0}}])

# for i in find3:
#      pprint(i)



find4= db_collection.aggregate([{"$group":{"_id":"$location.state", "total":{"$sum":1}}}])  #OK
# for i in find4:
#     pprint(i)



find5= db_collection.aggregate([{'$group':{'_id':"$location.city", "Result":{'$sum':1}}},{"$sort":{'Result':1}}])
# for i in find5 :
#     pprint(i)





#find6:
average_age_tehran=db_collection.aggregate([{'$match':{"location.city":"تهران"}},{'$group':{'_id':"$location.city", 'avarage_age':{'$avg':"$dob.age"}}}])
result_average_age_tehran="{'_id': 'تهران', 'avarage_age': 46.36842105263158}"

"""
     param: $cmp
     -1 if the first value is less than the second.
     1 if the first value is greater than the second.
     0 if the two values are equivalent.
"""
find6 = db_collection.aggregate([{'$group':{'_id' :"$location.city",'avg_age':{'$avg':"$dob.age"}}},
                              {'$project':{'compare_avg_age_tehran': {' $cmp': [ "$avg_age", 46.36842105263158 ] }}},
                              {'$sort':{"compare_avg_age_tehran":1}}])

# for i in find6:
#     pprint(i)

# find6 second way :
find6_b = db_collection.aggregate([{'$addFields':{"city_group":{'$cond':[{'$eq': ["$location.city","تهران"]}, "Tehran", "Other_Cities"]}}}, {'$group':{'_id':"$city_group", 'compare_avg_age':{'$avg':"$dob.age"}}}])

# for i in find6_b:
#      print(i)



find7=db_collection.aggregate([{'$project' : {'_id':0,"name.first":1,"name.last":1,"youth":{'$lt':["$dob.age" , 16]},"middle_aged":{'$and':[{'$gt':["$dob.age",16]},{'$lt':["$dob.age",40]}]},"old":{'$gt':["$dob.age",40]}}}])

# for i in find7:
#      print(i)




# Unfortunately, i couldn't handle this part :(
db = client['information']
new_collection = db['info']
# print(new_collection.count())  # first it gave 0 

# new_collection.insert_many(file_data)
# print(new_collection.count())



# # count = new_collection.aggregate([{"$count":"Total documents"}])
# # print(count)

