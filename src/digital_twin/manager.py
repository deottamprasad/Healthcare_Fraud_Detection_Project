from .models import PatientDT, DeviceDT, NetworkDT
import re # We'll use regex to find the patient ID in log strings

class HospitalDT:
    """
    The Aggregated Digital Twin (Central Manager).
    Holds and manages all individual DTs as per the framework.
    """
    def __init__(self):
        # Dictionaries to hold all individual DTs
        self.patients = {}  # Key: patient_id, Value: PatientDT object
        self.devices = {}   # Key: device_id, Value: DeviceDT object
        self.networks = {}  # Key: source_ip, Value: NetworkDT object
        print("✅ Hospital Digital Twin Manager (Layer 3) initialized.")

    def update_from_event(self, event):
        """
        Main routing function. Updates the correct individual DT(s)
        based on the incoming enriched event.
        """
        event_type = event.get('eventType')
        updated_states = [] # An event can update multiple DTs

        try:
            if event_type == 'MedicalDeviceLog':
                # This event updates PatientDT and DeviceDT
                patient_id = event.get('patient', {}).get('id')
                device_id = event.get('device', {}).get('id')

                if patient_id:
                    if patient_id not in self.patients:
                        self.patients[patient_id] = PatientDT(patient_id)
                    self.patients[patient_id].update_from_medical_event(event)
                    updated_states.append(("PatientDT", self.patients[patient_id].get_state()))
                
                if device_id:
                    if device_id not in self.devices:
                        self.devices[device_id] = DeviceDT(device_id)
                    # We pass patient_id to link the device to the patient
                    self.devices[device_id].update_from_medical_event(event, patient_id)
                    updated_states.append(("DeviceDT", self.devices[device_id].get_state()))

            elif event_type == 'NetworkAuthLog':
                # This event can update NetworkDT, DeviceDT, and PatientDT
                
                # 1. Update NetworkDT (based on source_ip)
                source_ip = event.get('network', {}).get('source_ip')
                if source_ip:
                    if source_ip not in self.networks:
                        self.networks[source_ip] = NetworkDT(source_ip)
                    self.networks[source_ip].update_from_network_event(event)
                    updated_states.append(("NetworkDT", self.networks[source_ip].get_state()))
                
                # 2. Update DeviceDT (based on device_id from network block)
                device_id = event.get('network', {}).get('source_device_details', {}).get('device_id')
                if device_id:
                    if device_id not in self.devices:
                        self.devices[device_id] = DeviceDT(device_id)
                    self.devices[device_id].update_from_network_event(event)
                    updated_states.append(("DeviceDT", self.devices[device_id].get_state()))
                
                # 3. Update PatientDT (based on target_resource_id)
                target_resource = event.get('action', {}).get('target_resource_id', '')
                # Use regex to find a patient ID like 'PAT-00789' in the resource string
                patient_id_match = re.search(r'(PAT-\d+)', target_resource)
                if patient_id_match:
                    patient_id = patient_id_match.group(1)
                    if patient_id not in self.patients:
                        self.patients[patient_id] = PatientDT(patient_id)
                    self.patients[patient_id].update_from_network_event(event)
                    updated_states.append(("PatientDT", self.patients[patient_id].get_state()))
        
        except Exception as e:
            print(f"❌ Error during DT state update: {e}")
            
        return updated_states