import os
import json
import time
import numpy as np
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor

class FlightDataAnalyzer:
    def __init__(self, base_folder):
        self.base_folder = base_folder
        self.total_records = 0
        self.dirty_records = 0
        self.flight_durations = defaultdict(list)
        self.passenger_count = Counter()

    def process_file(self, file_path):
        file_total_records = 0
        file_dirty_records = 0
        local_durations = defaultdict(list)
        local_passenger_count = Counter()

        with open(file_path, "r") as f:
            data = json.load(f)
            for record in data:
                file_total_records += 1
                if any(value is None for value in record.values()):
                    file_dirty_records += 1

                if all(value is not None for value in record.values()):
                    dest_city = record["destination_city"]
                    local_durations[dest_city].append(record["flight_duration_secs"])
                    local_passenger_count[dest_city] += record["passengers_on_board"]
                    local_passenger_count[record["origin_city"]] -= record["passengers_on_board"]

        return file_total_records, file_dirty_records, local_durations, local_passenger_count

    def update_global_data(self, result):
        file_total_records, file_dirty_records, local_durations, local_passenger_count = result
        self.total_records += file_total_records
        self.dirty_records += file_dirty_records

        for city, durations in local_durations.items():
            self.flight_durations[city].extend(durations)

        for city, passengers in local_passenger_count.items():
            self.passenger_count[city] += passengers

    def analyze_files(self):
        start_time = time.time()

        with ThreadPoolExecutor() as executor:
            all_files = [
                os.path.join(root, file)
                for root, _, files in os.walk(self.base_folder)
                for file in files if file.endswith(".json")
            ]

            for result in executor.map(self.process_file, all_files):
                self.update_global_data(result)

        end_time = time.time()
        self.total_duration = end_time - start_time

    def generate_report(self):
        top_25_cities = sorted(
            self.flight_durations, key=lambda x: sum(self.flight_durations[x]), reverse=True
        )[:25]

        avg_durations = {city: np.mean(self.flight_durations[city]) for city in top_25_cities}
        p95_durations = {city: np.percentile(self.flight_durations[city], 95) for city in top_25_cities}

        city_with_max_arrivals = self.passenger_count.most_common(1)[0][0]
        city_with_max_departures = self.passenger_count.most_common()[-1][0]

        print("Results:")
        print(f"Total records processed: {self.total_records}")
        print(f"Total dirty records: {self.dirty_records}")
        print(f"Total run duration: {self.total_duration:.2f} seconds")

        print("\nTop 25 Destination Cities - AVG and P95 Flight Durations:")
        for city in top_25_cities:
            print(
                f"{city}: AVG={avg_durations[city]:.2f} secs, P95={p95_durations[city]:.2f} secs"
            )

        print("\nCities with MAX Passengers Arrived and Left:")
        print(f"City with MAX arrivals: {city_with_max_arrivals}")
        print(f"City with MAX departures: {city_with_max_departures}")


if __name__ == "__main__":
    analyzer = FlightDataAnalyzer(base_folder="/tmp/flights")
    analyzer.analyze_files()
    analyzer.generate_report()
