import paho.mqtt.client as mqtt
import configparser
import json
import sys
import os

# --- Adjust path to import from root ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(PROJECT_ROOT)
# --- End Path Adjust ---

from src.utils.db_connector import DatabaseConnector
from src.processor import enricher
from src.digital_twin.manager import HospitalDT

# --- Import Layer 4 Components ---
from src.pre_detection.detector import AnomalyDetector
from src.pre_detection.contextualizer import ConsistencyChecker
from src.pre_detection.packager import PackageCreator

def on_connect(client, userdata, flags, reason_code, properties):
    """Callback function for when the client connects to the MQTT broker."""
    if reason_code == 0:
        print("✅ Successfully connected to MQTT Broker!")
        client.subscribe(userdata['device_topic'])
        client.subscribe(userdata['network_topic'])
        print(f"   -> Subscribed to topic: {userdata['device_topic']}")
        print(f"   -> Subscribed to topic: {userdata['network_topic']}")
    else:
        print(f"❌ Failed to connect to MQTT, return code {reason_code}\n")
        sys.exit(1)

def on_message(client, userdata, msg):
    """Callback function for when a message is received from the broker."""
    payload = msg.payload.decode('utf-8')
    print(f"\n📩 Message received on topic '{msg.topic}'")
    
    db_connector = userdata['db_connector']
    hospital_dt_manager = userdata['hospital_dt_manager']
    detector = userdata['detector']
    contextualizer = userdata['contextualizer']
    packager = userdata['packager']
    
    enriched_event = None

    # --- LAYER 2: ENRICHMENT ---
    if msg.topic == userdata['device_topic']:
        enriched_event = enricher.process_device_message(payload, db_connector)
    elif msg.topic == userdata['network_topic']:
        enriched_event = enricher.process_network_message(payload, db_connector)
    
    if not enriched_event:
        print("   -> Stop ⚠️ Event could not be enriched (Skipped by L2).")
        return

    # --- LAYER 3: INFORMATION (Digital Twin Update) ---
    updated_states = hospital_dt_manager.update_from_event(enriched_event)
    
    storage_success = db_connector.store_event(enriched_event)
    print(f"   -> L3 (Historical): Event storage success: {storage_success}")

    if updated_states:
        print(f"   -> L3 (Living Profile): {len(updated_states)} DT(s) updated.")
    else:
        print("   -> L3 (Living Profile): No DTs updated for this event.")
            
    # ---
    # --- LAYER 4 PIPELINE ---
    # ---
    
    # Step 1: Anomaly Detection
    triggered_anomalies = detector.check_event(enriched_event, hospital_dt_manager)
    
    if not triggered_anomalies:
        print("   -> L4 (Detect): No anomalies detected. Event is benign. ✅")
        return
        
    print(f"   -> L4 (Detect): ⚠️ {len(triggered_anomalies)} anomalies flagged: {triggered_anomalies}")
    
    # Step 2: Consistency Check
    suspicious_anomalies = contextualizer.filter_anomalies(
        triggered_anomalies, enriched_event, hospital_dt_manager
    )
    
    if not suspicious_anomalies:
        print("   -> L4 (Context): Anomalies explained as benign. ✅")
        return

    print(f"   -> L4 (Context): 🚨 {len(suspicious_anomalies)} suspicious anomalies confirmed: {suspicious_anomalies}")

    # Step 3: Package Creation
    case_file_json = packager.build_case_file(
        suspicious_anomalies, enriched_event, hospital_dt_manager
    )
    
    print("   -> L4 (Package): 🚀 Case file built. Escalating to Layer 5 (LLM)...")
    
    # --- This is where you would send to Layer 5 ---
    print("\n" + "="*60)
    print(f"--- 🚨 SUSPICIOUS EVENT CASE FILE 🚨 ---")
    print(case_file_json)
    print("="*60 + "\n")
    

def main():
    """Main function to start the L2, L3, and L4 processing service."""
    print("--- Starting Layer 2/3/4: Data Processing Service ---") 
    
    config = configparser.ConfigParser()
    config.read('config.ini')
    mqtt_config = config['MQTT']

    db_connector = DatabaseConnector()
    hospital_dt_manager = HospitalDT()
    detector = AnomalyDetector()
    contextualizer = ConsistencyChecker()
    packager = PackageCreator()

    user_data = {
        "db_connector": db_connector,
        "hospital_dt_manager": hospital_dt_manager,
        "detector": detector,
        "contextualizer": contextualizer,
        "packager": packager,
        "device_topic": mqtt_config['DEVICE_TOPIC'],
        "network_topic": mqtt_config['NETWORK_TOPIC'] 
    }
    
    # Correcting the userdata key to match config.ini
    user_data["network_topic"] = mqtt_config['NETWORK_TOPIC']

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
    except KeyboardInterrupt:
        print("\n\n🛑 Service stopped by user.")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")
    finally:
        print("   Disconnecting MQTT client and closing database connection...")
        client.disconnect()
        db_connector.close()
        print("--- Layer 2/3/4 Service Shut Down ---") 

if __name__ == '__main__':
    main()