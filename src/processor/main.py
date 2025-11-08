import paho.mqtt.client as mqtt
import configparser
import json
import sys
from ..utils.db_connector import DatabaseConnector
from . import enricher
from ..digital_twin.manager import HospitalDT

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
    
    # Get components from userdata
    db_connector = userdata['db_connector']
    hospital_dt_manager = userdata['hospital_dt_manager'] 
    
    enriched_event = None

    # --- LAYER 2: ENRICHMENT ---
    if msg.topic == userdata['device_topic']:
        enriched_event = enricher.process_device_message(payload, db_connector)
    elif msg.topic == userdata['network_topic']:
        enriched_event = enricher.process_network_message(payload, db_connector)
    
    # --- LAYER 3: INFORMATION ---
    if enriched_event:
        # 1. Historical Record (Store in DB)
        storage_success = db_connector.store_event(enriched_event)
        
        # 2. Living Profile (Update in-memory DTs)
        updated_states = hospital_dt_manager.update_from_event(enriched_event) # This is now a list

        # 3. Verification
        print(f"   -> L3 (Historical): Event storage success: {storage_success}")
        if updated_states:
            print("   -> L3 (Living Profile): DT states updated:")
            # --- MODIFIED BLOCK ---
            # Loop through the list of updated DTs and print each one
            for dt_name, state in updated_states:
                print(f"      --- Updated {dt_name} State ---")
                print(json.dumps(state, indent=2))
                print("      ----------------------------")
            # --- END MODIFIED BLOCK ---
        else:
            print("   -> L3 (Living Profile): No DTs updated for this event.")
            
    else:
        print("   -> Stop ⚠️ Event could not be enriched (Skipped by L2).")


def main():
    """Main function to start the Layer 2/3 processing service."""
    # Updated print message
    print("--- Starting Layer 2/3: Data Processing Service ---") 
    
    # --- Configuration ---
    config = configparser.ConfigParser()
    config.read('config.ini')
    mqtt_config = config['MQTT']

    # --- Component Initialization ---
    db_connector = DatabaseConnector()
    hospital_dt_manager = HospitalDT() # <--- ADD THIS LINE: Initialize L3 Manager

    # --- MQTT Client Initialization ---
    # Store all components in userdata to make them accessible in callbacks
    user_data = {
        "db_connector": db_connector,
        "hospital_dt_manager": hospital_dt_manager, # <--- ADD THIS LINE: Pass L3 Manager
        "device_topic": mqtt_config['DEVICE_TOPIC'],
        "network_topic": mqtt_config['NETWORK_TOPIC']
    }
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, userdata=user_data)
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        print("🔌 Attempting to connect to MQTT broker...")
        client.connect(mqtt_config['BROKER_ADDRESS'], int(mqtt_config['PORT']), 60)
        
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
        print("--- Layer 2/3 Service Shut Down ---") 


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