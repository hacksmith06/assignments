import os
import json
import random
from datetime import datetime, timedelta

num_files = 5000  # number of files to be generated
min_flights_per_file = 50
max_flights_per_file = 100
num_cities = random.randint(100, 200)  # total cities
null_probability = random.uniform(0.005, 0.001)

cities = [f"City_{i}" for i in range(1, num_cities + 1)]

base_folder = "/tmp/flights"


def random_date(start, end):
    return start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds()))
    )


def generate_flight_record():
    record = {
        "date": random_date(
            datetime.now() - timedelta(days=365), datetime.now()
        ).strftime("%Y-%m-%d"),
        "origin_city": random.choice(cities),
        "destination_city": random.choice(cities),
        "flight_duration_secs": random.randint(3600, 14400),  # 1 to 4 hours in seconds
        "passengers_on_board": random.randint(50, 300),  # Number of passengers
    }

    for key in record:
        if random.random() < null_probability:
            record[key] = None

    return record


for i in range(num_files):
    num_flights = random.randint(min_flights_per_file, max_flights_per_file)

    origin_city = random.choice(cities)
    month_year = datetime.now().strftime("%m-%y")

    folder_path = os.path.join(base_folder, f"{month_year}-{origin_city}")
    os.makedirs(folder_path, exist_ok=True)

    file_path = os.path.join(folder_path, f"{month_year}-{origin_city}-flights.json")

    flights = [generate_flight_record() for _ in range(num_flights)]

    with open(file_path, "w") as f:
        json.dump(flights, f, indent=4)

print("Flight data generation complete.")
