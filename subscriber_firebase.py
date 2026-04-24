import json
import firebase_admin
from firebase_admin import credentials, firestore
import paho.mqtt.client as mqtt

cred = credentials.Certificate("firebase-key.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

HOST = "aec31a90bbda48f3b180689b08b9e33b.s1.eu.hivemq.cloud"
PORT = 8883
USER = "pablo"
PASSWORD = "Test1234"

def on_connect(client, userdata, flags, rc):
    print("MQTT conectado")
    client.subscribe("iot/casas/+/telemetria")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        db.collection("telemetria").add(payload)
        print("Guardado en Firestore")
    except Exception as e:
        print("Error:", e)

client = mqtt.Client()
client.username_pw_set(USER, PASSWORD)
client.tls_set()
client.on_connect = on_connect
client.on_message = on_message

client.connect(HOST, PORT)
client.loop_forever()