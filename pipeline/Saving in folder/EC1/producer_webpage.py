from flask import Flask, request, jsonify, send_from_directory
import pandas as pd
import time
import os

app = Flask(__name__)

# Initial configuration for the waiting period
wait_seconds = 5  # Default time to wait between sending batches, in seconds

# Directory to save the data frames
data_folder = "./common_data_folder"
if not os.path.exists(data_folder):
    os.makedirs(data_folder)

@app.route('/')
def serve_index():
    return send_from_directory(app.static_folder, 'producer.html')

@app.route('/set_wait_time', methods=['POST'])
def set_wait_time():
    global wait_seconds
    data = request.get_json()
    if 'wait_seconds' in data:
        wait_seconds = int(data['wait_seconds'])
        return jsonify({"message": f"Wait time set to {wait_seconds} seconds"}), 200
    else:
        return jsonify({"error": "Missing 'wait_seconds' in request"}), 400

@app.route('/start_producer', methods=['GET'])
def start_producer():
    # Read the dataset
    df = pd.read_csv("./dataset/STL_QUERY/sample.csv")
    cols = [f'column{i}' for i in range(len(df.columns))]  # Renaming columns for simplicity
    df.columns = cols
    time_column_name = df.columns[7]
    df[time_column_name] = pd.to_datetime(df[time_column_name])
    df.set_index(time_column_name, inplace=True)

    # Group by 1-hour intervals
    groups = df.resample('H')

    # File numbering
    file_number = 1

    # Iterate over each group and save it as a file
    for name, group in groups:
        if len(group) > 0:
            file_path = f"{data_folder}/data_{file_number}.csv"
            group.to_csv(file_path, index=False)
            print(f"Saved group to file: {file_path}")
            file_number += 1
            time.sleep(wait_seconds)  # Wait based on the dynamic variable

    return jsonify({"message": "Producer started and data saved"}), 200

if __name__ == '__main__':
    app.run(port=8080, debug=True)
