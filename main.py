from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
import paho.mqtt.client as mqtt
import ssl
import json
import threading

# Inicializar FastAPI
app = FastAPI()

# Configuración de CORS
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Parámetros de conexión MQTT
broker = "mqtt34.victronenergy.com"
port = 8883
username = "camilo@olenergies.com"
password = "Ju@n-C,2023"
ca_cert = "broker-certs.pem"  # Ruta al archivo de certificados

# Diccionario global para almacenar los valores recibidos
latest_values = {
    "soc": None,
    "output": None,
    "grid_power": None,
    "pv_power": None,
    "current_grid_limit": None
}

# Tópicos MQTT
topics = {
    "soc": "N/c0619ab2f68d/battery/512/Soc",
    "output": "N/c0619ab2f68d/vebus/276/Ac/Out/P",
    "grid_power": "N/c0619ab2f68d/vebus/276/Ac/ActiveIn/P",
    "pv_power": "N/c0619ab2f68d/solarcharger/279/Yield/Power",
    "current_grid_limit": "N/c0619ab2f68d/vebus/276/Ac/ActiveIn/CurrentLimit"
}

# Función de callback para la conexión
def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected with result code {reason_code}")
    # Suscribirse a todos los tópicos
    for topic in topics.values():
        client.subscribe(topic)

# Función de callback para recibir mensajes
def on_message(client, userdata, msg):
    global latest_values
    try:
        # Decodificar el payload y convertirlo de JSON a dict
        data = json.loads(msg.payload.decode())
        # Encontrar el tópico y actualizar el valor correspondiente
        for key, topic in topics.items():
            if msg.topic == topic:
                latest_values[key] = data.get("value")
                print(f"Received message: {msg.topic} -> {latest_values[key]}")
                break
    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON: {e}")

# Crear una instancia del cliente MQTT con la nueva API
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

# Configurar la autenticación
client.username_pw_set(username, password)

# Configurar TLS/SSL
client.tls_set(ca_certs=ca_cert)

# Asignar las funciones de callback
client.on_connect = on_connect
client.on_message = on_message

# Conectar al broker
try:
    client.connect(broker, port, 60)
except Exception as e:
    print(f"Could not connect to MQTT broker: {e}")
    sys.exit(-1)

# Ejecutar el cliente MQTT en un hilo separado
def mqtt_loop():
    client.loop_forever()

mqtt_thread = threading.Thread(target=mqtt_loop)
mqtt_thread.start()

# Endpoint de FastAPI para obtener los últimos valores recibidos
@app.get(
    path="/",
    status_code=status.HTTP_200_OK,
    summary="Get latest MQTT values",
    tags=["MQTT"]
)
async def home():
    global latest_values
    return latest_values
