import paho.mqtt.client as mqtt
import configparser
import json
import sys
from ..utils.db_connector import DatabaseConnector
from . import enricher

# --- MQTT Client Setup ---

def on_connect(client, userdata, flags, reason_code, properties):
    """Callback function for when the client connects to the MQTT broker."""
    if reason_code == 0:
        print("✅ Successfully connected to MQTT Broker!")
        # Subscribe to both topics upon successful connection
        client.subscribe(userdata['device_topic'])
        client.subscribe(userdata['network_topic'])
        print(f"   -> Subscribed to topic: {userdata['device_topic']}")
        print(f"   -> Subscribed to topic: {userdata['network_topic']}")
    else:
        print(f"❌ Failed to connect to MQTT, return code {reason_code}\n")
        sys.exit(1) # Exit if connection fails

def on_message(client, userdata, msg):
    """Callback function for when a message is received from the broker."""
    payload = msg.payload.decode('utf-8')
    print(f"\n📩 Message received on topic '{msg.topic}'")
    
    db_connector = userdata['db_connector']
    enriched_event = None

    # Dispatch to the correct processing function based on the topic
    if msg.topic == userdata['device_topic']:
        enriched_event = enricher.process_device_message(payload, db_connector)
    elif msg.topic == userdata['network_topic']:
        enriched_event = enricher.process_network_message(payload, db_connector)
    
    if enriched_event:
        # Print the final, enriched JSON to the console
        print("--- ✨ Enriched Event JSON ✨ ---")
        print(json.dumps(enriched_event, indent=2))
        print("---------------------------------")
    else:
        print("   -> ⚠️ Event could not be enriched. Skipping.")


def main():
    """Main function to start the Layer 2 processing service."""
    print("--- Starting Layer 2: Data Enrichment Service ---")
    
    # --- Configuration ---
    config = configparser.ConfigParser()
    config.read('config.ini')
    mqtt_config = config['MQTT']

    # --- Database Connection ---
    db_connector = DatabaseConnector()

    # --- MQTT Client Initialization ---
    # Store config and db_connector in userdata to make them accessible in callbacks
    user_data = {
        "db_connector": db_connector,
        "device_topic": mqtt_config['DEVICE_TOPIC'],
        "network_topic": mqtt_config['NETWORK_TOPIC']
    }
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, userdata=user_data)
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        print("🔌 Attempting to connect to MQTT broker...")
        client.connect(mqtt_config['BROKER_ADDRESS'], int(mqtt_config['PORT']), 60)
        
        # loop_forever() is a blocking call that processes network traffic
        # and dispatches callbacks automatically.
        print("👂 Listening for messages... Press Ctrl+C to stop.")
        client.loop_forever()

    except ConnectionRefusedError:
        print(f"❌ MQTT Connection Error: Connection was refused.")
        print(f"   Please ensure the Mosquitto container is running ('docker-compose up -d').")
    except KeyboardInterrupt:
        print("\n\n🛑 Service stopped by user.")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")
    finally:
        print("   Disconnecting MQTT client and closing database connection...")
        client.disconnect()
        db_connector.close()
        print("--- Enrichment Service Shut Down ---")


if __name__ == '__main__':
    # Add the project root to the Python path to allow for package-based imports
    # This makes 'from ..utils' and 'from .' work correctly
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    main()



# This script implements a Layer-2 Data Enrichment service: it connects to an MQTT broker, listens to two topics (device logs and network/auth logs), and enriches 
# incoming messages by calling functions in the enricher module that query a DatabaseConnector.

# Uses Paho MQTT callbacks (on_connect, on_message) to subscribe and process messages asynchronously; enriched events are printed as pretty JSON.

# Reads configuration from config.ini, opens a DB connection, and performs graceful shutdown / cleanup in a finally block.

# Includes a small sys.path hack at the module entrypoint so the package-relative imports (..utils.db_connector) work when the file is executed as __main__.