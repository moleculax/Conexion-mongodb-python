#http://www.moleculax.com.ve/
import pprint
import pymongo

def main():

    # 1. Conecta con MongoDB 
    client = pymongo.MongoClient()

    # Accede a la coleccion 'restaurants' collection en la base de datos 'test'
    collection = client.test.restaurants

    # 2. Inserta datos 
    new_documents = [
        {"name":"Sun Bakery Trattoria", "stars":4, "categories":["Pizza","Pasta","Italian","Coffee","Sandwiches"]},
        {"name":"Blue Bagels Grill", "stars":3, "categories":["Bagels","Cookies","Sandwiches"]},
        {"name":"Hot Bakery Cafe","stars":4,"categories":["Bakery","Cafe","Coffee","Dessert"]},
        {"name":"XYZ Coffee Bar","stars":5,"categories":["Coffee","Cafe","Bakery","Chocolates"]},
        {"name":"456 Cookies Shop","stars":4,"categories":["Bakery","Cookies","Cake","Coffee"]}]

    collection.insert_many(new_documents)

    # 3. Consulta los datos 
    for restaurant in collection.find():
        pprint.pprint(restaurant)

    # 4. Crea Index 
    collection.create_index([('name', pymongo.ASCENDING)])

    # 5. Performance de la agregación
    pipeline = [
        {"$match": {"categories": "Bakery"}},
        {"$group": {"_id": "$stars", "count": {"$sum": 1}}}]
    pprint.pprint(list(collection.aggregate(pipeline)))

if __name__ == '__main__':
    main()
