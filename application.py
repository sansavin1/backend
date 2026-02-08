from flask import Flask, jsonify, request
import os
import pymysql
from pymysql.err import OperationalError
import logging
from flask_cors import CORS

application = Flask(__name__)
CORS(application)
logging.basicConfig(level=logging.INFO)

#Endpoint: Health Check
@application.route('/health', methods=['GET'])
def health():
    """
    This endpoint is used by the autograder to confirm that the backend deployment is healthy.
    """
    return jsonify({"status": "healthy"}), 200

#Endpoint: Data Insertion
@application.route('/events', methods=['POST'])
def create_event():
    """
    This endpoint should eventually insert data into the database.
    The database communication is currently stubbed out.
    You must implement insert_data_into_db() function to integrate with your MySQL RDS Instance.
    """
    try:
        payload = request.get_json()
        required_fields = ["title", "date"]
        if not payload or not all(field in payload for field in required_fields):
            return jsonify({"error": "Missing required fields: 'title' and 'date'"}), 400

        insert_data_into_db(payload)
        return jsonify({"message": "Event created successfully"}), 201
    except NotImplementedError as nie:
        return jsonify({"error": str(nie)}), 501
    except Exception as e:
        logging.exception("Error occurred during event creation")
        return jsonify({
            "error": "During event creation",
            "detail": str(e)
        }), 500

#Endpoint: Data Retrieval
@application.route('/data', methods=['GET'])
def get_data():
    try:
        data = fetch_data_from_db()
        return jsonify({"data": data}), 200   # ✅ must be dict with "data"
    except NotImplementedError as nie:
        return jsonify({"error": str(nie)}), 501
    except Exception as e:
        logging.exception("Error occurred during data retrieval")
        return jsonify({
            "error": "During data retrieval",
            "detail": str(e)
        }), 500
    
    
def get_db_connection():
    """
    Establish and return a connection to the RDS MySQL database.
    The following variables should be added to the Elastic Beanstalk Environment Properties for better security. Follow guidelines for more info.
      - DB_HOST
      - DB_USER
      - DB_PASSWORD
      - DB_NAME
    """
    required_vars = ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME"]
    missing = [var for var in required_vars if not os.environ.get(var)]
    if missing:
        msg = f"Missing environment variables: {', '.join(missing)}"
        logging.error(msg)
        raise EnvironmentError(msg)
    try:
        connection = pymysql.connect(
            host=os.environ.get("DB_HOST"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD"),
            db=os.environ.get("DB_NAME")
        )
        return connection
    except OperationalError as e:
        raise ConnectionError(f"Failed to connect to the database: {e}")

def create_db_table():
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            # Create table if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    description TEXT,
                    image_url TEXT,
                    date DATE NOT NULL,
                    location VARCHAR(255)
                )
            """)

            # Upgrade schema if table already existed with VARCHAR(255)
            cursor.execute("""
                ALTER TABLE events
                MODIFY COLUMN image_url TEXT
            """)
        connection.commit()
        logging.info("Events table created/verified and schema updated")
    except Exception as e:
        connection.rollback()
        logging.exception("Failed to create or verify the events table")
        raise RuntimeError(f"Table creation failed: {str(e)}")
    finally:
        connection.close()

def insert_data_into_db(payload):
    """
    Insert a single event into the events table.
    """
    create_db_table()
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            insert_sql = """
                INSERT INTO events (title, description, image_url, date, location)
                VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(insert_sql, (
                payload.get("title"),
                payload.get("description"),
                payload.get("image_url"),
                payload.get("date"),
                payload.get("location")
            ))
        connection.commit()
        logging.info("Event inserted successfully")
    except Exception:
        connection.rollback()
        logging.exception("Insert failed")
        raise
    finally:
        connection.close()

#Database Function Stub
def fetch_data_from_db():
    """
    Fetch all event records ordered by date ascending.
    """
    create_db_table()
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            select_sql = """
                SELECT id, title, description, image_url, date, location
                FROM events
                ORDER BY date ASC
            """
            cursor.execute(select_sql)
            results = cursor.fetchall()

        data = []
        for row in results:
            data.append({
                "id": row[0],
                "title": row[1],
                "description": row[2],
                "image_url": row[3],
                "date": row[4].strftime("%Y-%m-%d") if row[4] else None,
                "location": row[5]
            })
        return data
    finally:
        connection.close()
        

if __name__ == '__main__':
    application.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
