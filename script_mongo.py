from pymongo.mongo_client import MongoClient
import pandas as pd

#Leemos el csv con pandas
df = pd.read_csv("books.csv", sep=";", encoding="latin-1", nrows=100)
df = df[["ISBN","Book-Title","Book-Author","Year-Of-Publication"]]
#Creamos un id unico por autor
df["author_id"] = df.groupby(["Book-Author"]).ngroup()

uri = "mongodb://localhost:27017/"

#Creamos listas de diccionarios o documentos para insertar con mongo
books = [{"title": row[0], "ISBN": row[1], "Year": row[2], "authorIds": [row[3]]} 
		  for row in zip(df["Book-Title"], df["ISBN"], df["Year-Of-Publication"], df["author_id"])]
authors = [{"_id": row[0], "author": row[1] } for row in zip(df["author_id"], df["Book-Author"])]

#Conectamos
client = MongoClient(uri)
db = client.biblioteca

#Insertamos los libros
books_id = db.books.insert_many(books).inserted_ids
for author, book_id in zip(authors, books_id):
	#Si no existe el autor lo crea y si no lo actualiza y le agrega la nueva id del libro
	db.authors.update_one({"_id": author["_id"]}, 
						  {"$set": {"_id": author["_id"], 
						            "author": author["author"]},
						   "$push": {"booksIds": book_id}},
						   upsert=True)