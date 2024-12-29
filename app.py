from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json

app = Flask(__name__)

# Set up Kafka Producer
kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092',
                               value_serializer=lambda v: json.dumps(v).encode('utf-8'))

@app.route('/send', methods=['POST'])
def send_data():
    try:
        data = request.get_json()
        print(f"Received data: {data}")  # Debug: Print received data
        kafka_producer.send('market_data', value=data)
        kafka_producer.flush()
        return jsonify({"message": "MarketData sent successfully"}), 200
    except Exception as e:
        print(f"Error sending data: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(port=5001,debug=True)