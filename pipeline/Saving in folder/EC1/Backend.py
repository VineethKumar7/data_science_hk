from flask import Flask, jsonify
from flask_cors import CORS
from query_procesing import QueryProcessor
import pandas as pd
import os
import json

app = Flask(__name__)
CORS(app)

# Assuming QueryProcessor is defined somewhere in your project

directory_path = "./common_data_folder"

@app.route('/get-processed-data', methods=['GET'])
def get_processed_data():
    all_files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith(".csv")]
    query_counts = []

    for file_path in all_files:
        df = pd.read_csv(file_path)
        print(df)
        cols = [f'column{i}' for i in range(len(df.columns))]
        qp = QueryProcessor(df=df)
        df.columns = cols
        qp.find_query_instance_count(table="instance_count", df=df)
        query_counts.append(len(qp.QUERY_HISTORY))  # Now works correctly since QUERY_HISTORY is instance-specific

    return jsonify({"file_counts": query_counts})

if __name__ == '__main__':
    app.run(debug=True)
