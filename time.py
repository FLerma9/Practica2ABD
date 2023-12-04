import time
from pymongo.mongo_client import MongoClient

uri = "mongodb://localhost:27017/"
client = MongoClient(uri)
db = client.biblioteca

# Primera consulta
pipeline = [ 
	{ '$lookup': { 'from': 'authors', 'localField': 'authorIds', 'foreignField': '_id', 'as': 'authorIds' } }, 
	{ '$match': { 'Year': { '$gt': 2020 }, 'authorIds.author': 'Rich Shapero' } },
	{ '$project': { 'title': '$title', 'ISBN': '$ISBN', 'Year': '$Year','authorIds.author': '$authorIds.author'}}
]

start_time = time.process_time()
results = db.books.aggregate(pipeline)
print('Tiempo primera consulta sin index:', time.process_time()-start_time)

#Segunda consulta
pipeline = [
	{ '$lookup': { 'from': 'authors', 'localField': 'authorIds', 'foreignField': '_id', 'as': 'authorIds' } }, 
	{ '$match': {'title': {'$regex': '^The'} , 'authorIds.author': 'Simon Mawer'}},
	{ '$project': { 'title': '$title', 'ISBN': '$ISBN', 'Year': '$Year','authorIds.author': '$authorIds.author'}}
]

start_time = time.process_time()
results = db.books.aggregate(pipeline)
print('Tiempo segunda consulta sin index:', time.process_time()-start_time)

#Tercera consulta
start_time = time.process_time()
results = db.books.find({'ISBN': '0399135782'})
print('Tiempo tercera consulta sin index:', time.process_time()-start_time)

#Creación de los diferentes índices
db.authors.create_index('author')
db.books.create_index('title')
db.books.create_index('ISBN')
db.books.create_index('Year')

#Primera consulta con índices
pipeline = [ 
	{ '$lookup': { 'from': 'authors', 'localField': 'authorIds', 'foreignField': '_id', 'as': 'authorIds' } }, 
	{ '$match': { 'Year': { '$gt': 2020 }, 'authorIds.author': 'Rich Shapero' } },
	{ '$project': { 'title': '$title', 'ISBN': '$ISBN', 'Year': '$Year','authorIds.author': '$authorIds.author'}}
]


start_time = time.process_time()
results = db.books.aggregate(pipeline)
print('Tiempo primera consulta CON index:', time.process_time()-start_time)

#Seguna consulta con índices
pipeline = [
	{ '$lookup': { 'from': 'authors', 'localField': 'authorIds', 'foreignField': '_id', 'as': 'authorIds' } }, 
	{ '$match': {'title': {'$regex': '^The'} , 'authorIds.author': 'Simon Mawer'}},
	{ '$project': { 'title': '$title', 'ISBN': '$ISBN', 'Year': '$Year','authorIds.author': '$authorIds.author'}}
]

start_time = time.process_time()
results = db.books.aggregate(pipeline)
print('Tiempo segunda consulta CON index:', time.process_time()-start_time)

#Tercera consulta con índices
start_time = time.process_time()
results = db.books.find({'ISBN': '0399135782'})
print('Tiempo tercera consulta CON index:', time.process_time()-start_time)


