import json

def process_device_message(payload, db_connector):
    """
    Parses, standardizes, and enriches a raw medical device log message.
    """
    try:
        parts = payload.split(',')
        if len(parts) != 6:
            print(f"⚠️ Malformed device message received: {payload}")
            return None
        
        timestamp, device_id, patient_id, heart_rate, spo2, status = parts
        
        base_event = {
            "eventType": "MedicalDeviceLog",
            "timestamp": timestamp,
            "raw_payload": payload,
            "device": {
                "id": device_id,
                "status": status,
                "metrics": {
                    "heart_rate_bpm": int(heart_rate),
                    "spo2_percent": int(spo2)
                }
            },
            "patient": {
                "id": patient_id
            }
        }
        
        device_info = db_connector.fetch_one_as_dict(
            "SELECT device_type, location, department, ip_address FROM devices WHERE device_id = %s", (device_id,)
        )
        if device_info:
            base_event['device'].update(device_info)
            
        # Now fetches the 'status' column from the database
        patient_info = db_connector.fetch_one_as_dict(
            "SELECT full_name, current_room, assigned_doctor_id, status FROM patients WHERE patient_id = %s", (patient_id,)
        )
        if patient_info:
            base_event['patient'].update(patient_info)
            if patient_info.get('assigned_doctor_id'):
                doctor_info = db_connector.fetch_one_as_dict(
                   "SELECT full_name, role, is_on_shift FROM staff WHERE user_id = %s", (patient_info['assigned_doctor_id'],)
                )
                if doctor_info:
                    base_event['patient']['assigned_doctor_details'] = doctor_info
        
        return base_event
        
    except (ValueError, IndexError) as e:
        print(f"❌ Error parsing device message payload '{payload}': {e}")
        return None


def process_network_message(payload, db_connector):
    """
    Parses, standardizes, and enriches a raw network/auth log message.
    """
    try:
        parts = payload.split(',')
        if len(parts) != 6:
            print(f"⚠️ Malformed network message received: {payload}")
            return None
            
        timestamp, log_source, user_id, source_ip, action, target_resource = parts
        
        base_event = {
            "eventType": "NetworkAuthLog",
            "timestamp": timestamp,
            "raw_payload": payload,
            "network": {
                "source_ip": source_ip,
                "log_source": log_source
            },
            "action": {
                "type": action,
                "target_resource_id": target_resource
            },
            "user": {
                "id": user_id
            }
        }
        
        user_info = db_connector.fetch_one_as_dict(
            "SELECT full_name, role, department, access_level, is_on_shift FROM staff WHERE user_id = %s", (user_id,)
        )
        if user_info:
            base_event['user'].update(user_info)

        device_info = db_connector.fetch_one_as_dict(
            "SELECT device_id, device_type, location, department FROM devices WHERE ip_address = %s", (source_ip,)
        )
        if device_info:
            base_event['network']['source_device_details'] = device_info
            
        # If the target resource is a patient, enrich it with their info
        if target_resource.startswith('PAT-'):
            patient_info = db_connector.fetch_one_as_dict(
                "SELECT full_name, current_room, status FROM patients WHERE patient_id = %s", (target_resource,)
            )
            if patient_info:
                # Add this info to a new key
                base_event['target_patient_details'] = patient_info
            
        return base_event

    except (ValueError, IndexError) as e:
        print(f"❌ Error parsing network message payload '{payload}': {e}")
        return None