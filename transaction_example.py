import datetime
from datetime import timedelta
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['car_booking']


def book_car(db, client_name, model_car, start_date, booking_duration):
    client = db.clients.find_one({'name': client_name})
    car = db.cars.find_one({'model': model_car})

    if not client or not car:
        print("Недействительный клиент или автомобиль")
        return

    elif booking_duration <= 0:
        print("Неприемлемый срок бронирования")
        return

    elif car['quantity'] == 0:
        print("Автомобиль недоступен")
        return

    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")

    existing_booking = db.bookings.find_one({'name': client_name, 'model': model_car, 'start_date': start_date})
    if existing_booking:
        print("Вы уже забронировали автомобиль")
        return

    end_date = start_date + timedelta(days=booking_duration)
    total_price = car['price_per_day'] * booking_duration
    db.cars.update_one({'model': model_car}, {'$inc': {'quantity': -1}})

    booking_details = {
        'name': client_name,
        'model': model_car,
        'start_date': start_date,
        'end_date': end_date,
        'booking_duration': booking_duration,
        'total_price': total_price
    }

    db.bookings.insert_one(booking_details)
    return booking_details


if __name__ == '__main__':
    client_name = 'Hailey Martinez'
    model_car = 'Camry'
    start_date = '2023-07-16'
    booking_duration = 2

    input_booking = book_car(db, client_name, model_car, start_date, booking_duration)
    print(input_booking)