import os
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, render_template
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

app = Flask(__name__)

# MongoDB configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "webhook_db")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "events")

try:
    # Initialize MongoDB client
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    # Check if the connection is successful
    client.admin.command('ping')
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    print(f"Connected to MongoDB at {MONGO_URI}")
except (ConnectionFailure, OperationFailure) as e:
    print(f"Failed to connect to MongoDB: {e}")
    db = None
    collection = None


@app.route('/')
def index():
    """Serve the frontend index.html."""
    return render_template('index.html')


@app.route('/webhook', methods=['POST'])
def handle_webhook():
    """
    Endpoint to receive GitHub webhook payloads.
    Parses and stores the payload in MongoDB based on event type.
    """
    if collection is None:
        return jsonify({"error": "Database connection unavailable"}), 503

    payload = request.json
    if not payload:
        return jsonify({"error": "No payload received"}), 400

    event_type = request.headers.get('X-GitHub-Event')
    parsed_data = {}

    if event_type == 'push':
        head_commit = payload.get('head_commit', {})
        parsed_data['request_id'] = head_commit.get('id')
        parsed_data['author'] = payload.get('pusher', {}).get('name')
        parsed_data['action'] = "PUSH"
        parsed_data['from_branch'] = None
        # Extract the to_branch by stripping 'refs/heads/'
        ref = payload.get('ref', '')
        parsed_data['to_branch'] = ref.replace('refs/heads/', '') if ref else None

        timestamp_raw = head_commit.get('timestamp')
        if timestamp_raw:
            try:
                dt = datetime.fromisoformat(timestamp_raw.replace('Z', '+00:00'))
                # Convert to IST (UTC+5:30)
                dt_ist = dt + timedelta(hours=5, minutes=30)
                parsed_data['timestamp'] = dt_ist.strftime("%Y-%m-%d %H:%M:%S IST")
            except ValueError:
                parsed_data['timestamp'] = timestamp_raw
        else:
            parsed_data['timestamp'] = None

    elif event_type == 'pull_request':
        pr = payload.get('pull_request', {})
        action_val = payload.get('action')

        parsed_data['request_id'] = str(pr.get('id'))
        parsed_data['author'] = pr.get('user', {}).get('login')
        parsed_data['from_branch'] = pr.get('head', {}).get('ref')
        parsed_data['to_branch'] = pr.get('base', {}).get('ref')

        if action_val == 'opened':
            parsed_data['action'] = "PULL_REQUEST"
        elif action_val == 'closed' and pr.get('merged') is True:
            parsed_data['action'] = "MERGE"
        else:
            # Ignore other pull request actions
            return jsonify({"message": f"Action '{action_val}' ignored"}), 200

        timestamp_raw = pr.get('updated_at') or pr.get('created_at')
        if timestamp_raw:
            try:
                dt = datetime.fromisoformat(timestamp_raw.replace('Z', '+00:00'))
                # Convert to IST (UTC+5:30)
                dt_ist = dt + timedelta(hours=5, minutes=30)
                parsed_data['timestamp'] = dt_ist.strftime("%Y-%m-%d %H:%M:%S IST")
            except ValueError:
                parsed_data['timestamp'] = timestamp_raw
        else:
            parsed_data['timestamp'] = None
    else:
        return jsonify({"message": f"Event type '{event_type}' ignored"}), 200

    # Internal tracking for API filtering
    parsed_data['received_at'] = datetime.utcnow()

    try:
        collection.insert_one(parsed_data)
        return jsonify({"message": "Webhook received and stored"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/events', methods=['GET'])
def get_events():
    """
    Endpoint to fetch the latest records from MongoDB.
    Accepts a 'last_timestamp' query parameter for filtering.
    """
    if collection is None:
        return jsonify({"error": "Database connection unavailable"}), 503

    last_timestamp_str = request.args.get('last_timestamp')
    query = {}

    if last_timestamp_str:
        try:
            # Expecting ISO format timestamp
            last_timestamp = datetime.fromisoformat(last_timestamp_str)
            query["received_at"] = {"$gt": last_timestamp}
        except ValueError:
            return jsonify({"error": "Invalid timestamp format. Use ISO format."}), 400

    try:
        # Fetch records, excluding the MongoDB _id field for JSON serialization
        cursor = collection.find(query, {"_id": 0}).sort("received_at", -1)
        events = list(cursor)
        return jsonify(events), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    # PEP 8 suggests imports at the top, which we've done.
    # The application runs on port 5000 by default.
    app.run(debug=True, host='0.0.0.0', port=5000)
