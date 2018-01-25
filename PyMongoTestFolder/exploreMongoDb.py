from pymongo import MongoClient
from pprint import pprint
from bson.objectid import ObjectId

client = MongoClient(port=27017)
db = client.business


def find_one_five_star():
    # finds 1 entry with rating of 5
    fivestar = db.reviews.find_one({'rating': 5})
    print(fivestar)


def find_count_five_star():
    # find number of ratings with 5
    fivestarcount = db.reviews.find({'rating': 5}).count()
    print(fivestarcount)


def find_all_five_star():
    # find all data with rating of 5
    fivestarall = db.reviews.find({'rating': 5})
    for entry in fivestarall:
        print(entry)


def aggregate_counting_thingy():
    # To my understanding the aggregate function will allow for functionality like grouping and summing grouped sets
    # of data like in the example below.  So in the instance below all the ratings are grouped and the instances of
    # each rating is summed.  Finally the sums are sorted based on their _id value.
    # Showcasing the count() method of find, count the total number of 5 ratings
    print('The number of 5 star reviews:')
    fivestarcount = db.reviews.find({'rating': 5}).count()
    print(fivestarcount)
    # Not let's use the aggregation framework to sum the occurrence of each rating across the entire data set
    print('\nThe sum of each rating occurance across all data grouped by rating ')
    stargroup = db.reviews.aggregate(
        # The Aggregation Pipeline is defined as an array of different operations
        [
            # The first stage in this pipe is to group data
            {'$group':
                 {'_id': "$rating",
                  "count":
                      {'$sum': 1}
                  }
             },
            # The second stage in this pipe is to sort the data
            {"$sort": {"_id": 1}
             }
            # Close the array with the ] tag
        ])
    # Print the result
    for group in stargroup:
        print(group)


def update_mongo_entry():
    # update_one seems similar to alter in SQL
    ASingleReview = db.reviews.find_one({})
    print('A sample document:')
    pprint(ASingleReview)

    result = db.reviews.update_one({'_id': ASingleReview.get('_id')}, {'$inc': {'likes': 1}})
    print('Number of documents modified : ' + str(result.modified_count))

    UpdatedDocument = db.reviews.find_one({'_id': ASingleReview.get('_id')})
    print('The updated document:')
    pprint(UpdatedDocument)

def delete_mongo_entry():
    #delete the one we used update_one on previously
    result = db.reviews.delete_one({"_id": ObjectId('5a6966bbc79d2a2ef4cb636a')}) #needed to import bson.objectid ObjectId
    pprint(result.deleted_count)

delete_mongo_entry()

