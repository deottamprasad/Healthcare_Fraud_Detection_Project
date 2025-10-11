import paho.mqtt.client as mqtt
import pandas as pd
import time
import configparser
import sys

def get_mqtt_config():
    """Reads MQTT configuration from the config file."""
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config['MQTT']

def connect_mqtt(broker, port):
    """Connects to the MQTT broker and returns a client instance."""
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="network_simulator")
    try:
        client.connect(broker, int(port), 60)
        client.loop_start() # Start a background thread to handle network traffic
        print(f"✅ Connected to MQTT Broker at {broker}:{port}")
        return client
    except ConnectionRefusedError:
        print(f"❌ MQTT Connection Error: Connection was refused.")
        print(f"   Please ensure the Mosquitto container is running ('docker-compose up -d').")
        sys.exit(1)
    except Exception as e:
        print(f"❌ An unexpected error occurred during MQTT connection: {e}")
        sys.exit(1)

def publish_data(client, topic, file_path):
    """Reads data from a CSV and publishes it to an MQTT topic."""
    try:
        df = pd.read_csv(file_path)
        print(f"\n🚀 Starting network & authentication log simulation...")
        print(f"   Publishing to topic: '{topic}'")
        print("   Press Ctrl+C to stop the simulation.")
        
        while True:
            for index, row in df.iterrows():
                # Convert the row to a comma-separated string payload
                payload = ','.join(map(str, row.values))
                result = client.publish(topic, payload)
                
                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    print(f"   -> Published: {payload}")
                else:
                    print(f"   ⚠️ Failed to publish message: {mqtt.error_string(result.rc)}")

                time.sleep(3) # Simulate a 3-second interval between logs
                
    except FileNotFoundError:
        print(f"❌ Error: The file '{file_path}' was not found.")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n🛑 Simulation stopped by user.")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")

def main():
    """Main function to run the network simulator."""
    mqtt_config = get_mqtt_config()
    client = connect_mqtt(mqtt_config['BROKER_ADDRESS'], mqtt_config['PORT'])
    
    if client:
        try:
            publish_data(client, mqtt_config['NETWORK_TOPIC'], 'data/raw/auth_network_logs.csv')
        finally:
            print("   Disconnecting MQTT client...")
            client.loop_stop()
            client.disconnect()
            print("--- Network Simulator Shut Down ---")

if __name__ == '__main__':
    main()
