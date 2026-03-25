from .models import PatientDT, DeviceDT, NetworkDT
import re
from datetime import datetime

class HospitalDT:
    """
    The Aggregated Digital Twin (Central Manager).
    Holds and manages all individual DTs as per the framework.
    """
    def __init__(self):
        self.patients = {}
        self.devices = {}
        self.networks = {}
        print("✅ Hospital Digital Twin Manager (Layer 3) initialized.")

    # --- Helper methods to safely get DTs for Layer 4 ---
    def get_patient_dt(self, patient_id):
        return self.patients.get(patient_id)

    def get_device_dt(self, device_id):
        return self.devices.get(device_id)

    def get_network_dt(self, source_ip):
        return self.networks.get(source_ip)
    # --- End new methods ---

    def update_from_event(self, event):
        """
        Main routing function. Updates the correct individual DT(s)
        based on the incoming enriched event.
        """
        event_type = event.get('eventType')
        updated_states = [] 

        try:
            if event_type == 'MedicalDeviceLog':
                patient_id = event.get('patient', {}).get('id')
                device_id = event.get('device', {}).get('id')

                if patient_id:
                    if patient_id not in self.patients:
                        self.patients[patient_id] = PatientDT(patient_id)
                    # This call now updates the patient's status from the event
                    self.patients[patient_id].update_from_medical_event(event)
                    updated_states.append(("PatientDT", self.patients[patient_id].get_state()))
                
                if device_id:
                    if device_id not in self.devices:
                        self.devices[device_id] = DeviceDT(device_id)
                    self.devices[device_id].update_from_medical_event(event, patient_id)
                    updated_states.append(("DeviceDT", self.devices[device_id].get_state()))

            elif event_type == 'NetworkAuthLog':
                source_ip = event.get('network', {}).get('source_ip')
                
                if source_ip:
                    if source_ip not in self.networks:
                        self.networks[source_ip] = NetworkDT(source_ip)
                    self.networks[source_ip].update_from_network_event(event) 
                    updated_states.append(("NetworkDT", self.networks[source_ip].get_state()))
                
                device_id = event.get('network', {}).get('source_device_details', {}).get('device_id')
                if device_id:
                    if device_id not in self.devices:
                        self.devices[device_id] = DeviceDT(device_id)
                    self.devices[device_id].update_from_network_event(event)
                    updated_states.append(("DeviceDT", self.devices[device_id].get_state()))
                
                target_resource = event.get('action', {}).get('target_resource_id', '')
                patient_id_match = re.search(r'(PAT-\d+)', target_resource)
                if patient_id_match:
                    patient_id = patient_id_match.group(1)
                    if patient_id not in self.patients:
                        self.patients[patient_id] = PatientDT(patient_id)
                    # This call now updates the patient's status from the event
                    self.patients[patient_id].update_from_network_event(event)
                    updated_states.append(("PatientDT", self.patients[patient_id].get_state()))
        
        except Exception as e:
            print(f"❌ Error during DT state update: {e}")
            
        return updated_states