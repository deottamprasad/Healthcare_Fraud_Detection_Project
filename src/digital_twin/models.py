# File: src/digital_twin/models.py

class PatientDT:
    """Represents the 'living profile' of a single patient."""
    def __init__(self, patient_id):
        self.patient_id = patient_id
        self.full_name = None
        self.current_room = None
        
        # Metrics from MedicalDeviceLog
        self.last_heart_rate = None
        self.last_spo2 = None
        self.last_device_status = None
        self.last_metric_update = None
        
        # Access info from NetworkAuthLog
        self.last_accessed_by_user_id = None
        self.last_access_action = None
        self.last_access_time = None

    def update_from_medical_event(self, event):
        """Updates the patient's state from a MedicalDeviceLog event."""
        self.last_metric_update = event.get('timestamp')
        
        if 'patient' in event:
            self.full_name = event['patient'].get('full_name', self.full_name)
            self.current_room = event['patient'].get('current_room', self.current_room)
        
        if 'device' in event and 'metrics' in event['device']:
            self.last_heart_rate = event['device']['metrics'].get('heart_rate_bpm', self.last_heart_rate)
            self.last_spo2 = event['device']['metrics'].get('spo2_percent', self.last_spo2)
            self.last_device_status = event['device'].get('status', self.last_device_status)
            
    def update_from_network_event(self, event):
        """Updates the patient's state from a NetworkAuthLog event."""
        self.last_access_time = event.get('timestamp')
        if 'user' in event:
            self.last_accessed_by_user_id = event['user'].get('id', self.last_accessed_by_user_id)
        if 'action' in event:
            self.last_access_action = event['action'].get('type', self.last_access_action)

    def get_state(self):
        """Returns the current state as a dictionary."""
        return self.__dict__

class DeviceDT:
    """Represents the 'living profile' of a single device."""
    def __init__(self, device_id):
        self.device_id = device_id
        self.device_type = None
        self.location = None
        self.department = None
        self.ip_address = None
        
        # From MedicalDeviceLog
        self.status = None
        self.current_patient_id = None # The patient this device is monitoring
        self.last_metric_update = None
        
        # From NetworkAuthLog
        self.last_user_id = None # The user logged into this device
        self.last_action_from_device = None
        self.last_network_update = None
        
    def update_from_medical_event(self, event, patient_id):
        """Updates the device's state from a MedicalDeviceLog event."""
        self.last_metric_update = event.get('timestamp')
        self.current_patient_id = patient_id
        if 'device' in event:
            self.device_type = event['device'].get('device_type', self.device_type)
            self.location = event['device'].get('location', self.location)
            self.department = event['device'].get('department', self.department)
            self.ip_address = event['device'].get('ip_address', self.ip_address)
            self.status = event['device'].get('status', self.status)

    def update_from_network_event(self, event):
        """Updates the device's state from a NetworkAuthLog event."""
        self.last_network_update = event.get('timestamp')
        if 'user' in event:
            self.last_user_id = event['user'].get('id', self.last_user_id)
        if 'action' in event:
            self.last_action_from_device = event['action'].get('type', self.last_action_from_device)
        # Update device info if it's present in the event
        if 'network' in event and 'source_device_details' in event['network']:
             details = event['network']['source_device_details']
             self.device_type = details.get('device_type', self.device_type)
             self.location = details.get('location', self.location)

    def get_state(self):
        return self.__dict__

class NetworkDT:
    """Represents the 'living profile' of a network source (IP address)."""
    def __init__(self, source_ip):
        self.source_ip = source_ip
        
        # State from NetworkAuthLog
        self.last_user_id = None
        self.last_device_id = None
        self.last_action = None
        self.last_target_resource = None
        self.last_log_source = None
        self.last_update = None
        
    def update_from_network_event(self, event):
        """Updates the network endpoint's state from a NetworkAuthLog event."""
        self.last_update = event.get('timestamp')
        
        if 'user' in event:
            self.last_user_id = event['user'].get('id', self.last_user_id)
        if 'action' in event:
            self.last_action = event['action'].get('type', self.last_action)
            self.last_target_resource = event['action'].get('target_resource_id', self.last_target_resource)
        if 'network' in event:
            self.last_log_source = event['network'].get('log_source', self.last_log_source)
            if 'source_device_details' in event['network']:
                self.last_device_id = event['network']['source_device_details'].get('device_id', self.last_device_id)

    def get_state(self):
        return self.__dict__