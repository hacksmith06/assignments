import os
import json
import random
from datetime import datetime, timedelta

class FlightDataGenerator:
    def __init__(self, base_folder, num_files, min_flights_per_file, max_flights_per_file, null_probability):
        self.base_folder = base_folder
        self.num_files = num_files
        self.min_flights_per_file = min_flights_per_file
        self.max_flights_per_file = max_flights_per_file
        self.null_probability = null_probability
        self.cities = self.generate_cities()
    
    def generate_cities(self):
        num_cities = random.randint(100, 200)
        return [f"City_{i}" for i in range(1, num_cities + 1)]

    def random_date(self, start, end):
        return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

    def generate_flight_record(self):
        record = {
            "date": self.random_date(datetime.now() - timedelta(days=365), datetime.now()).strftime("%Y-%m-%d"),
            "origin_city": random.choice(self.cities),
            "destination_city": random.choice(self.cities),
            "flight_duration_secs": random.randint(3600, 14400),  # 1 to 4 hours in seconds
            "passengers_on_board": random.randint(50, 300),  # Number of passengers
        }

        for key in record:
            if random.random() < self.null_probability:
                record[key] = None

        return record

    def generate_flights(self, num_flights):
        return [self.generate_flight_record() for _ in range(num_flights)]

    def save_flights_to_file(self, folder_path, file_name, flights):
        os.makedirs(folder_path, exist_ok=True)
        file_path = os.path.join(folder_path, file_name)
        with open(file_path, "w") as f:
            json.dump(flights, f, indent=4)

    def generate_data(self):
        for i in range(self.num_files):
            num_flights = random.randint(self.min_flights_per_file, self.max_flights_per_file)
            origin_city = random.choice(self.cities)
            month_year = datetime.now().strftime("%m-%y")

            folder_path = os.path.join(self.base_folder, f"{month_year}-{origin_city}")
            file_name = f"{month_year}-{origin_city}-flights.json"

            flights = self.generate_flights(num_flights)
            self.save_flights_to_file(folder_path, file_name, flights)

        print("Flight data generation complete.")

# Initialize the FlightDataGenerator and generate the data
if __name__ == "__main__":
    generator = FlightDataGenerator(
        base_folder="/tmp/flights",
        num_files=5000,
        min_flights_per_file=50,
        max_flights_per_file=100,
        null_probability=random.uniform(0.005, 0.001)
    )
    generator.generate_data()
