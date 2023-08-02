#!/usr/bin/python3
from flask import Flask, request, jsonify
from flask_jwt_extended import JWTManager, jwt_required, create_access_token
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
import os
from dotenv import load_dotenv
import json

# Load the environment variables from the .env file
load_dotenv()

app = Flask(__name__)
app.config['JWT_SECRET_KEY'] = os.getenv("SECRET_KEY")
jwt = JWTManager(app)
conf = {
                    'bootstrap.servers': os.getenv("BOOTSTRAP_SERVER"),
                    'security.protocol': 'SASL_SSL',
                    'sasl.mechanisms': 'SCRAM-SHA-512',
                    'key.serializer': StringSerializer('utf_8'),
                    'value.serializer': StringSerializer('utf_8'),
                    'sasl.username': os.getenv("SASL_USERNAME"),
                    'sasl.password': os.getenv("SASL_PASSWORD"),
                    'partitioner': 'random',
                    'linger.ms': '500',
                    'compression.type': 'lz4'
                }

producer = SerializingProducer(conf)
currTop = os.getenv("CURRTOP")

@app.route('/kafka/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    # authentication logic
    if username == os.getenv("PROXY_USERNAME") and password == os.getenv("PROXY_PASSWORD"):
        access_token = create_access_token(identity=username)
        return jsonify({'access_token': access_token}), 200

    return jsonify({'message': 'Invalid credentials'}), 401

@app.route('/kafka/produce', methods=['POST'])
@jwt_required()
def produce_message():
    try:
        data = request.get_json()
        collection = data['collection']
        val = json.dumps(data['data'])

        try:
            producer.produce(currTop, key=collection, value=val)
        except BufferError as e:
            producer.poll(1000)
            producer.produce(currTop, key=collection, value=val)
        producer.flush()

        return jsonify({'status': 'success', 'message': 'Message sent to Kafka topic successfully.'}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8082)
