from flask import Flask, jsonify
from flask_cors import CORS  # Import CORS
import os
import json

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes and origins

# Directory containing the JSON files
directory_path = "./data"  # Update this path to your JSON files directory

@app.route('/get-data', methods=['GET'])
def get_data():
    print("Inside the API")
    combined_data = []
    # Loop through each JSON file in the directory
    for filename in os.listdir(directory_path):
        if filename.endswith(".json"):
            print("File name ", filename)
            file_path = os.path.join(directory_path, filename)
            # Read the content of the file
            with open(file_path, 'r') as file:
                try:
                    data = json.load(file)
                    # Assuming each file contains a list of items
                    combined_data.extend(data)  # Use extend to flatten the list
                except json.JSONDecodeError as e:
                    print(f"Error reading {filename}: {e}")
    print("data= ", combined_data)
    return jsonify(combined_data)

if __name__ == '__main__':
    app.run(debug=True)
