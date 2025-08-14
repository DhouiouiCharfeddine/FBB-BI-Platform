import os
import json
from pymongo import MongoClient

# MongoDB Connection Setup
client = MongoClient('mongodb://localhost:27017')
db = client.Router_Ingested
unknown_files_collection = db.Unknown_Files
router_traffic_collection = db.Router_Traffic
router_configuration_collection = db.Router_Configuration

# Create indexes
router_traffic_collection.create_index([("routers", 1), ("Measure_Time", 1)])
router_configuration_collection.create_index([("routers", 1), ("Measure_Time", 1)])

# Directories path 
json_dir = r'C:\Users\hp\OneDrive\Bureau\End_Studies_Project\Directory_Json\QoS_CPE'
text_files_dir = r'C:\Users\hp\OneDrive\Bureau\End_Studies_Project\Text_Files'

# Ensure the directory for text files exists
os.makedirs(text_files_dir, exist_ok=True)

# File Paths
ingested_files_path = os.path.join(text_files_dir, 'Ingested_Files.txt')
unknown_files_path = os.path.join(text_files_dir, 'Unknown_Files.txt')

# Function to validate "Stat" JSON files
def validate_stat_json(json_data):
    try:
        routers = json_data.get("routers")
        if routers is None:
            return False
        
        for router_ip, router_data in routers.items():
            measure_time = router_data.get("Measure_Time")
            if measure_time is None:
                return False
            
            for measure_timestamp, measure_values in measure_time.items():
                if not isinstance(measure_values, dict) or not measure_values:
                    return False
        
        return True
    except Exception as e:
        print(f"Error during schema validation: {e}")
        return False


# Function to validate "Config" JSON files
def validate_config_json(json_data):
    required_keys = ["routers"]
    for key in required_keys:
        if key not in json_data:
            return False
    
    routers = json_data["routers"]
    for router_ip, router_data in routers.items():
        if "Measure_Time" not in router_data:
            return False
        
        measure_time = router_data["Measure_Time"]
        for timestamp, metrics in measure_time.items():
            if not isinstance(metrics, dict):
                return False
            for metric_name, metric_value in metrics.items():
                # Assuming all metric values are strings
                if not isinstance(metric_value, str):
                    return False
    
    return True



# Function to ingest JSON files
def ingest_json_file(file_path, ingested_files, unknown_files):
    file_name = os.path.basename(file_path)
    print(f"Processing file: {file_name}")  # For debugging
    with open(file_path, 'r') as f:
        try:
            json_data = json.load(f)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON in file '{file_name}': {e}")
            return
    
    if file_name.startswith("Stat") or file_name.startswith("Config"):
        if file_name in ingested_files:
            print(f"File '{file_name}' already ingested.")
            return
        
        if file_name.startswith("Stat"):
            if validate_stat_json(json_data):
                router_traffic_collection.insert_one(json_data)
                with open(ingested_files_path, 'a') as f:
                    f.write(file_name + '\n')
            else:
                unknown_files_collection.insert_one(json_data)
                with open(unknown_files_path, 'a') as f:
                    f.write(file_name + '\n')
        elif file_name.startswith("Config"):
            if validate_config_json(json_data):
                router_configuration_collection.insert_one(json_data)
                with open(ingested_files_path, 'a') as f:
                    f.write(file_name + '\n')
            else:
                unknown_files_collection.insert_one(json_data)
                with open(unknown_files_path, 'a') as f:
                    f.write(file_name + '\n')
    else:
        if file_name in unknown_files:
            print(f"File '{file_name}' already in unknown files.")
            return
        
        unknown_files_collection.insert_one(json_data)
        with open(unknown_files_path, 'a') as f:
            f.write(file_name + '\n')

# Main Function
def main():
    # Check if the files exist, if not, create them
    if not os.path.exists(ingested_files_path):
        with open(ingested_files_path, 'w'): pass
    if not os.path.exists(unknown_files_path):
        with open(unknown_files_path, 'w'): pass
    
    # Load ingested and unknown files' names into sets for efficient lookup
    ingested_files = set()
    with open(ingested_files_path, 'r') as f:
        ingested_files = {line.strip() for line in f}
    
    unknown_files = set()
    with open(unknown_files_path, 'r') as f:
        unknown_files = {line.strip() for line in f}
    
    # Iterate over files in the directory and ingest JSON files
    for file_name in os.listdir(json_dir):
        file_path = os.path.join(json_dir, file_name)
        if os.path.isfile(file_path) and file_name.endswith('.json'):
            ingest_json_file(file_path, ingested_files, unknown_files)

if __name__ == "__main__":
    main()
