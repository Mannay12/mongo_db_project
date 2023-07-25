from main import *

client = MongoClient('mongodb://localhost:27017/')
db = client['car_booking']
cars_collection = db['cars']
clients_collection = db['clients']
bookings_collection = db['bookings']

if __name__ == '__main__':
    # # Все автомобили из коллекции.
    # all_cars(cars_collection)

    # # Все клиенты из коллекции.
    # all_clients(clients_collection)

    # # Все бронирования автомобилей из коллекции.
    # all_bookings(bookings_collection)

    # # Подсчет общего количества автомобилей в наличии.
    # count_cars(cars_collection)

    # # Удаление бронирования автомобиля клиентом.
    # delete_booking(bookings_collection, cars_collection, 'Hailey Martinez', 'Camry', '2023-07-15')

    # # Обновление имени указанного клиента.
    # update_client_name(clients_collection, 'Nicholas Bartlett', 'Иванов Иван')

    # # Проверка обновленного имени клиента
    # update_client = clients_collection.find_one({'name': 'Иванов Иван'})
    # print(update_client)

    # # Поиск автомобилей, которые имеют указанный тип кузова.
    # cars_with_body_type = find_by_body_type(cars_collection, ['sedan'])
    # print(cars_with_body_type)

    # Поиск автомобилей указанной цены.
    # cars_50_price = find_by_price(cars_collection, 40)
    # print(cars_50_price)

    # # Поиска автомобилей в указанном диапазоне цен.
    # cars_40_to_80_price = find_by_price_range(cars_collection, 40, 80)
    # print(cars_40_to_80_price)

    # # Сортирует автомобили по возрастанию цены, затем применяет пагинацию
    # page_number = 3
    # page_size = 1
    #
    # results = demonstrate_sort_and_pagination(cars_collection, page_number, page_size)
    #
    # for car in results:
    #     print(car)

    # # Создает текстовый индекс по полю 'name' и демонстрирует его использование.
    # create_text_index(clients_collection)

    # # Удаляет все индексы, созданные на коллекции
    # drop_all_indexes(clients_collection)

    # # Подсчитывает общее количество автомобилей и бронирований автомобилей
    # aggregate_single(cars_collection, bookings_collection)
