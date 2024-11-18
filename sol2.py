import os
import json
import time
import numpy as np
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor

# Folder containing JSON files
base_folder = "/tmp/flights"

total_records = 0
dirty_records = 0
flight_durations = defaultdict(list)
passenger_count = Counter()

# Start timer
start_time = time.time()


def process_file(file_path):
    global total_records, dirty_records
    file_total_records = 0
    file_dirty_records = 0
    local_durations = defaultdict(list)
    local_passenger_count = Counter()

    with open(file_path, "r") as f:
        data = json.load(f)
        for record in data:
            file_total_records += 1
            # Count dirty records
            if any(value is None for value in record.values()):
                file_dirty_records += 1

            # Collect flight duration and passenger data
            if all(value is not None for value in record.values()):
                dest_city = record["destination_city"]
                local_durations[dest_city].append(record["flight_duration_secs"])
                local_passenger_count[dest_city] += record["passengers_on_board"]
                local_passenger_count[record["origin_city"]] -= record[
                    "passengers_on_board"
                ]  # Subtracting to for origin city

    return (
        file_total_records,
        file_dirty_records,
        local_durations,
        local_passenger_count,
    )


def update_global_data(result):
    global total_records, dirty_records
    file_total_records, file_dirty_records, local_durations, local_passenger_count = (
        result
    )
    total_records += file_total_records
    dirty_records += file_dirty_records

    # Update global flight duration
    for city, durations in local_durations.items():
        flight_durations[city].extend(durations)

    # Update global passenger counts
    for city, passengers in local_passenger_count.items():
        passenger_count[city] += passengers


# parallel processing to be done
with ThreadPoolExecutor() as executor:
    # fetch all files
    all_files = [
        os.path.join(root, file)
        for root, _, files in os.walk(base_folder)
        for file in files
        if file.endswith(".json")
    ]
    # Process each file concurrently
    for result in executor.map(process_file, all_files):
        update_global_data(result)

end_time = time.time()
total_duration = end_time - start_time

# Find the top 25 destination cities by total duration count
top_25_cities = sorted(
    flight_durations, key=lambda x: sum(flight_durations[x]), reverse=True
)[:25]

# Calculate AVG and P95 flight duration for the top 25 destination cities
avg_durations = {city: np.mean(flight_durations[city]) for city in top_25_cities}
p95_durations = {
    city: np.percentile(flight_durations[city], 95) for city in top_25_cities
}

# Find cities with max passengers arrived and left
city_with_max_arrivals = passenger_count.most_common(1)[0][0]
city_with_max_departures = passenger_count.most_common()[-1][0]

# Output
print("Results:")
print(f"Total records processed: {total_records}")
print(f"Total dirty records: {dirty_records}")
print(f"Total run duration: {total_duration:.2f} seconds")

print("\nTop 25 Destination Cities - AVG and P95 Flight Durations:")
for city in top_25_cities:
    print(
        f"{city}: AVG={avg_durations[city]:.2f} secs, P95={p95_durations[city]:.2f} secs"
    )

print("\nCities with MAX Passengers Arrived and Left:")
print(f"City with MAX arrivals: {city_with_max_arrivals}")
print(f"City with MAX departures: {city_with_max_departures}")
