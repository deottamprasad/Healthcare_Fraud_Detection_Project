import json
import uuid

class PackageCreator:
    """
    Implements Layer 4 (Stage 3): Package Creation.
    Builds the "Contextual Anomaly Package" for Layer 5 (LLM).
    """
    def __init__(self):
        print("✅ Layer 4 (Packager) initialized.")

    def build_case_file(self, suspicious_anomalies, event, dt_manager):
        """
        Assembles all relevant information into a single JSON "case file".
        """
        case_id = f"CASE-{uuid.uuid4()}"
        print(f"   -> L4 (Package) 📦 Building case file: {case_id}")
        
        involved_entities = self._get_involved_entities(event, dt_manager)
        
        hypothesis = self._generate_hypothesis(suspicious_anomalies, event)
    
        case_file = {
            "case_id": case_id,
            "status": "Pending_L5_Review",
            "timestamp": event.get('timestamp'),
            "primary_hypothesis": hypothesis,
            "suspicious_anomalies_triggered": suspicious_anomalies,
            "triggering_event": event,
            "contextual_profiles": involved_entities
        }
        
        return json.dumps(case_file, indent=2, default=str)

    def _get_involved_entities(self, event, dt):
        """Gathers the DT states for all entities in the event."""
        entities = {}
        
        if 'user' in event:
            entities['user'] = event['user']
            
        patient_id = event.get('patient', {}).get('id')
        if not patient_id:
             target_id = event.get('action', {}).get('target_resource_id', '')
             if target_id.startswith('PAT-'): patient_id = target_id
        
        if patient_id:
            p_dt = dt.get_patient_dt(patient_id)
            if p_dt: entities['patient'] = p_dt.get_state()

        device_id = event.get('device', {}).get('id')
        if not device_id:
            device_id = event.get('network', {}).get('source_device_details', {}).get('device_id')

        if device_id:
            d_dt = dt.get_device_dt(device_id)
            if d_dt: entities['device'] = d_dt.get_state()
            
        ip = event.get('network', {}).get('source_ip')
        if ip:
            n_dt = dt.get_network_dt(ip)
            if n_dt: entities['network_ip'] = n_dt.get_state()
            
        return entities

    def _generate_hypothesis(self, anomalies, event):
        """
        Generates a simple, rule-based hypothesis for the LLM,
        prioritized by severity.
        """
        # Get the root ID for each anomaly (e.g., "R1", "B7")
        root_anomalies = {a.split(':')[0] for a in anomalies}

        # Priority 1: Data Integrity / Device Tampering (Most Severe)
        if any(rule in root_anomalies for rule in ["R8", "R9", "R10", "B7", "B8", "B9", "B10"]):
            return "Potential device tampering or critical data integrity failure."
        
        # Priority 2: Data Exfiltration
        elif "B1" in root_anomalies:
            return "Potential data exfiltration attempt (high-volume access)."
        
        # Priority 3: Privacy Violations
        elif "R4" in root_anomalies:
            return "Potential privacy violation (snooping on discharged patient)."
            
        # Priority 4: Privilege/Role Violations
        elif any(rule in root_anomalies for rule in ["R3", "R5", "R6"]):
            return "Potential role-based privilege violation or unauthorized action."

        # Priority 5: Suspicious Login Patterns
        elif any(rule in root_anomalies for rule in ["R7", "B3", "B4"]):
            return "Suspicious login pattern detected (e.g., new IP, high failures)."

        # Priority 6: Off-Shift Access (Lowest Priority Anomaly)
        elif "R1" in root_anomalies:
            return "Potential unauthorized access by off-shift employee."
        
        # Default fallback
        return "General suspicious activity detected."