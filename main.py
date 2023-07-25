from pymongo import MongoClient
from random import randint, choice
from faker import Faker
import datetime

fake = Faker()

client = MongoClient('mongodb://localhost:27017/')
db = client['car_booking']


def check_db_and_collection(db_name: str, collection_name: str):
    # Проверка наличия базы данных и коллекции. В случае их отсуствия - создает их
    if db_name in client.list_database_names():
        print(f'База данных {db_name} уже существует')
    else:
        print(f'База данных {db_name} не найдена. Создаем...')
    if collection_name in client[db_name].list_collection_names():
        print(f'Коллекция {collection_name} уже существует.')
    else:
        print(f'Коллекция {collection_name} не найдена. Создаем...')
        client[db_name][collection_name]

    return client[db_name][collection_name]


def create_cars(collection):
    # Создание коллекции автомобилей
    if collection.count_documents({}) == 0:
        collection.insert_one({
            'brand': 'Toyota',
            'model': 'Camry',
            'year': 2020,
            'body_type': 'sedan',
            'price_per_day': 50,
            'quantity': 2
        })
        collection.insert_one({
            'brand': 'BMW',
            'model': 'X5',
            'year': 2021,
            'body_type': 'crossover',
            'price_per_day': 60,
            'quantity': 3
        })
        collection.insert_one({
            'brand': 'Audi',
            'model': 'Q5',
            'year': 2022,
            'body_type': 'crossover',
            'price_per_day': 40,
            'quantity': 1
        })

    return collection


def create_clients(collection):
    # Создание коллекции клиентов
    if collection.count_documents({}) == 0:
        collection.insert_one({
            'name': 'Hailey Martinez',
            'age': randint(18, 30),
            'email': fake.email() if choice([True, False]) else None,
            'address': {
                'city': {
                    'name': fake.city(),
                    'postal_code': fake.zipcode(),
                },
                'street': {
                    'name': fake.street_name(),
                    'building': fake.building_number(),
                }
            }
        })
        collection.insert_one({
            'name': 'Nicholas Bartlett',
            'age': randint(18, 30),
            'email': fake.email() if choice([True, False]) else None,
            'address': {
                'city': {
                    'name': fake.city(),
                    'postal_code': fake.zipcode(),
                },
                'street': {
                    'name': fake.street_name(),
                    'building': fake.building_number(),
                },
            }
        })
        collection.insert_one({
            'name': 'Chad Ray',
            'age': randint(18, 30),
            'email': fake.email() if choice([True, False]) else None,
            'address': {
                'city': {
                    'name': fake.city(),
                    'postal_code': fake.zipcode(),
                },
                'street': {
                    'name': fake.street_name(),
                    'building': fake.building_number(),
                },
            }
        })

    return collection


def all_cars(collection):
    # Все автомобили из коллекции.
    results = collection.find({})
    for result in results:
        print(result)


def all_clients(collection):
    # Все клиенты из коллекции.
    results = collection.find({})
    for result in results:
        print(result)


def all_bookings(collection):
    # Все бронирования автомобилей из коллекции.
    results = collection.find({})
    for result in results:
        print(result)


def delete_booking(collection, collection_cars, name: str, model: str, start_date: str):
    # Удаление бронирования автомобиля клиентом.
    start_date_obj = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    booking = collection.find_one({'name': name, 'model': model, 'start_date': start_date_obj})
    if booking:
        collection.delete_one({'name': name, 'model': model, 'start_date': start_date_obj})
        collection_cars.update_one({'model': model}, {'$inc': {'quantity': 1}})
        print(f'Бронирование автомобиля {model} {start_date} клиентом {name} было удалено!')
    else:
        print(f'Не найдено бронирования для автомобиля {model} {start_date} по запросу клиента {name}!')


def count_cars(collection):
    # Подсчет общего количества автомобилей в наличии.
    total_quantity = collection.aggregate([
        {'$group': {'_id': None, 'total': {'$sum': '$quantity'}}}
    ])
    for result in total_quantity:
        print(f"Общее количество автомобилей в наличии: {result['total']}")


def update_client_name(collection, name: str, new_name: str):
    # Использование оператора $set для обновления имени указанного клиента.
    collection.update_one({'name': name}, {'$set': {'name': new_name}})
    print(f'Имя клиента {name} успешно обнавлен на {new_name}')


def find_by_body_type(collection, body_type: str):
    # Использование оператора $all для поиска машин, которые имеют все указанные типы кузова.
    results = collection.find({'body_type': {'$all': body_type}})
    return list(results)


def find_by_price(collection, price_per_day: int):
    # Использование оператора $eq для поиска автомобилей указанной цены.
    results = collection.find({'price_per_day': {'$eq': price_per_day}})
    return list(results)


def find_by_price_range(collection, min_price: int, max_price: int):
    # Использование оператора $and для поиска автомобилей в указанном диапазоне цен.
    results = collection.find({'$and': [{'price_per_day': {'$gt': min_price}}, {'price_per_day': {'$lt': max_price}}]})
    return list(results)


def demonstrate_sort_and_pagination(collection, page_number, page_size):
    # Функция демонстрирует сортировку и пагинацию в МонгоДБ
    # Сортирует автомобили по убыванию цены, затем применяет пагинацию
    results = collection.find().sort('price_per_day', 1).skip(page_size * (page_number - 1)).limit(page_size)
    return list(results)


def create_text_index(collection):
    # Создает текстовый индекс по полю 'name' и демонстрирует его использование.
    print(f'\nCreating text index on "name" field...')
    collection.create_index([('name', 'text')])

    print('\n-----After Creating Text Index------')
    print('\nClients with the subject "Ray":')
    clients = list(collection.find({'$text': {'$search': 'Ray'}}))
    for client in clients:
        print(client)


def drop_all_indexes(collection):
    # Удаляет все индексы, созданные на коллекции
    print(f'\nDropping all indexes in the collection...')
    collection.drop_indexes()
    print('Indexes dropped successfully')


def aggregate_single(cars_collection, bookings_collection):
    # Подсчитывает общее количество автомобилей и бронирований автомобилей
    num_cars = cars_collection.distinct('_id')
    num_bookings = bookings_collection.distinct('_id')
    print('Всего автомобилей: ', len(num_cars))
    print('Всего бронирований: ', len(num_bookings))


if __name__ == '__main__':
    cars_collection = check_db_and_collection('car_booking', 'cars')
    clients_collection = check_db_and_collection('car_booking', 'clients')
    bookings_collection = check_db_and_collection('car_booking', 'bookings')

    create_cars(cars_collection)
    create_clients(clients_collection)
    print('Все автомобили успешно добавлены в коллекцию.')
    print('Все клиенты успешно добавлены в коллекцию.')
    print('Все бронирования успешно добавлены в коллекцию.')
