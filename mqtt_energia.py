import paho.mqtt.client as mqtt
import logging
import random
import time
import datetime
import json
import asyncio
import ssl
import os
from dotenv import load_dotenv

load_dotenv()
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class MQTTClient:
    def __init__(self, broker, port, topic, username, password):
        self.client = mqtt.Client(protocol=mqtt.MQTTv311)
        self.broker = broker
        self.port = port
        self.topic = topic
        self.username = username
        self.password = password
        self.client_id = f"CLIENT_ID_{random.randint(0, 1000)}"
        self.client = mqtt.Client(self.client_id)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message

    def connect(self):
        self.client.will_set(
            self.topic,
            payload=f"Client {self.client_id} Disconnected",
            qos=1,
            retain=False,
        )
        self.client.tls_set(ca_certs="./ca.crt", tls_version=ssl.PROTOCOL_TLSv1_2)
        self.client.tls_insecure_set(False)
        self.client.username_pw_set(self.username, self.password)
        self.client.connect(self.broker, self.port, keepalive=60)
        self.client.loop_start()
        self.client.subscribe("#")
        time.sleep(20)

    def disconnect(self):
        self.client.disconnect()
        self.client.loop_stop()
        logging.info("Disconnected from MQTT Broker!")

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0 and client.is_connected():
            logging.info("Connected to MQTT Broker!")
            client.subscribe(self.topic)
            self.client.publish(
                self.topic,
                payload=f"Client {self.client_id} Connected",
                qos=1,
                retain=False,
            )
        else:
            logging.error(f"Failed to connect, return code {rc}")

    def on_disconnect(self, client, userdata, rc):
        logging.info("Disconnected with result code: %s", rc)
        if rc != 0:
            logging.info("Publishing Last Will message...")
            self.publish(f"{self.client_id} Disconnected")
        reconnect_count, reconnect_delay = 0, 1
        while reconnect_count < 12:
            logging.info("Reconnecting in %d seconds...", reconnect_delay)
            time.sleep(reconnect_delay)

            try:
                client.reconnect()
                logging.info("Reconnected successfully!")
                return
            except Exception as err:
                logging.error("%s. Reconnect failed. Retrying...", err)

            reconnect_delay *= 2
            reconnect_delay = min(reconnect_delay, 60)
            reconnect_count += 1
        logging.info("Reconnect failed after %s attempts. Exiting...", reconnect_count)
        global FLAG_EXIT
        FLAG_EXIT = True

    def on_message(self, client, userdata, msg):
        topic_prefixes = [
            "NVZ/SGATE",
        ]

        payload = msg.payload.decode()
        topic = msg.topic

        for prefix in topic_prefixes:
            if topic.startswith(prefix):
                if prefix == "NVZ/SGATE":
                    data = payload.split(":")
                    if len(data) == 7:
                        json_data = {
                            "FatorPotenciaInstante": data[0],
                            "DemandaAtivaInstante": data[1],
                            "DemandaReativaInstante": data[2],
                            "ConsumoAtivoTotal": data[3],
                            "ConsumoAtivoPeriodo": data[4],
                            "PeriodoMedicao": data[5],
                            "DemandaAtivaMedia15min": data[6],
                        }
                        formatted_message = (
                            f"-t {topic} -m {json.dumps(json_data, indent=4)}"
                        )
                        logging.info(formatted_message)
                    else:
                        logging.error(
                            f"Received invalid message from {msg.topic}: {payload}"
                        )
                else:
                    logging.info(
                        f"Received `{msg.payload.decode()}` from `{msg.topic}` "
                    )
                break

    def publish(self, payload):
        if not self.client.is_connected():
            logging.error("publish: MQTT client is not connected!")
            return
        msg = json.dumps(payload)
        result = self.client.publish(self.topic, msg)
        status = result[0]
        if status == mqtt.MQTT_ERR_SUCCESS:
            logging.info(f"Send `{msg}` to topic `{self.topic}`")
        else:
            logging.error(f"Failed to send message to topic {self.topic}")

    async def start(self):
        logging.basicConfig(
            filename="Energia.log",
            format="%(asctime)s - %(levelname)s: %(message)s",
            level=logging.DEBUG,
        )
        self.connect()
        while not FLAG_EXIT:
            await asyncio.sleep(20)


# Função para executar os comandos git
def execute_git_commands():
    os.system("git add .")
    os.system('git commit -m "update log mqtt"')
    os.system("git push origin main")


# Função para lidar com eventos de alteração de arquivos
class MyHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if event.is_directory:
            return
        # Se um arquivo for modificado, execute os comandos git
        execute_git_commands()


# Função para criar o observador e iniciar o loop de monitoramento
def start_file_monitoring():
    folder_to_watch = "./"  # Pasta atual

    event_handler = MyHandler()
    observer = Observer()
    observer.schedule(event_handler, path=folder_to_watch, recursive=True)
    observer.start()

    try:
        while True:
            # Verifica as alterações a cada minuto no arquivo
            time.sleep(60)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


if __name__ == "__main__":
    BROKER = os.getenv("BROKER")
    PORT = int(os.getenv("PORT"))
    TOPIC = os.getenv("TOPIC")
    USERNAME_CLIENT = os.getenv("USERNAME_CLIENT")
    PASSWORD = os.getenv("PASSWORD")
    # print(f'Broker: {BROKER}, Port: {PORT}, Topic: {TOPIC}, Username: {USERNAME_CLIENT}, Password: {PASSWORD}')

    FLAG_EXIT = False

    # Inicia o monitoramento de arquivos em segundo plano
    """
    import threading
    file_monitoring_thread = threading.Thread(target=start_file_monitoring)
    file_monitoring_thread.daemon = True
    file_monitoring_thread.start()
    """

    # Inicia o cliente MQTT em um loop de eventos asyncio
    mqtt_client = MQTTClient(BROKER, PORT, TOPIC, USERNAME_CLIENT, PASSWORD)
    print("Client Mqtt is Running")
    asyncio.run(mqtt_client.start())
