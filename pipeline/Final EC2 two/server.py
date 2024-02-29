from flask import Flask, jsonify
from flask_cors import CORS
import os
import json

app = Flask(__name__)
CORS(app)

directory_path = "./data"


@app.route('/get-processed-data', methods=['GET'])
def get_processed_data():
    all_files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith(".json")]
    if len(all_files) != 0:
        latest_file = max(all_files, key=os.path.getctime)

        with open(latest_file, 'r') as file:
            data = json.load(file)

        return jsonify(data)
    return {}

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
