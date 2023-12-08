import pymongo

# Function to connect to MongoDB
def connect_to_mongo():
    cluster = pymongo.MongoClient("")
    db = cluster[""]  # Update with your database name
    collection = db[""]  # Update with your collection name
    return collection

# Function to update the "Data" field to an array format
def update_data_to_array(collection):
    documents_to_update = collection.find({})  # Find all documents in the collection

    for doc in documents_to_update:
        # Update the document with the modified "Grade" field ["9"]
        collection.update_one({"_id": doc["_id"]}, {"$set": {"Grade": ["9"]}})
        print(f"Updated document {doc['_id']} successfully")



# Connect to MongoDB
mongo_collection = connect_to_mongo()

# Update "Data" field to an array format in MongoDB documents
update_data_to_array(mongo_collection)
