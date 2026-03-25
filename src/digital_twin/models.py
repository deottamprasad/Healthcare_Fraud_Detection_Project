from collections import deque
from datetime import datetime, timedelta

# --- STATISTICAL BASELINE CONSTANTS ---
STATS_WINDOW_HOURS = 1
STATS_WINDOW_MINUTES = 5
SHORT_TERM_MEMORY = 5
IP_ACCESS_RATE_MINUTES = 1

class PatientDT:
    """Represents the 'living profile' of a single patient."""
    def __init__(self, patient_id):
        self.patient_id = patient_id
        self.full_name = None
        self.current_room = None
        self.status = "Unknown"  #  Default is Unknown until an event provides it
        
        self.last_heart_rate = None
        self.last_spo2 = None
        self.last_device_status = None
        self.last_metric_update = None
        self.last_5_heart_rates = deque(maxlen=SHORT_TERM_MEMORY) 
        
        self.last_accessed_by_user_id = None
        self.last_access_action = None
        self.last_access_time = None

    def update_from_medical_event(self, event):
        """Updates the patient's state from a MedicalDeviceLog event."""
        self.last_metric_update = event.get('timestamp')
        
        # Now updates status from the enriched event
        if 'patient' in event:
            self.full_name = event['patient'].get('full_name', self.full_name)
            self.current_room = event['patient'].get('current_room', self.current_room)
            self.status = event['patient'].get('status', self.status) # <-- UPDATED
        
        if 'device' in event and 'metrics' in event['device']:
            metrics = event['device']['metrics']
            self.last_heart_rate = metrics.get('heart_rate_bpm', self.last_heart_rate)
            self.last_spo2 = metrics.get('spo2_percent', self.last_spo2)
            self.last_device_status = event['device'].get('status', self.last_device_status)
            
            if self.last_heart_rate is not None:
                self.last_5_heart_rates.append(self.last_heart_rate)
            
    def update_from_network_event(self, event):
        """Updates the patient's state from a NetworkAuthLog event."""
        self.last_access_time = event.get('timestamp')
        if 'user' in event:
            self.last_accessed_by_user_id = event['user'].get('id', self.last_accessed_by_user_id)
        if 'action' in event:
            self.last_access_action = event['action'].get('type', self.last_access_action)
            
        # If the enricher added patient details, update the DT
        if 'target_patient_details' in event:
            self.full_name = event['target_patient_details'].get('full_name', self.full_name)
            self.current_room = event['target_patient_details'].get('current_room', self.current_room)
            self.status = event['target_patient_details'].get('status', self.status)

    def get_state(self):
        """Returns the current state as a dictionary."""
        state = self.__dict__.copy()
        state['last_5_heart_rates'] = list(self.last_5_heart_rates)
        return state

class DeviceDT:
    """Represents the 'living profile' of a single device."""
    def __init__(self, device_id):
        self.device_id = device_id
        self.device_type = None
        self.location = None
        self.department = None
        self.ip_address = None
        
        self.status = "Online"
        self.current_patient_id = None
        self.last_metric_update = None
        self.last_heart_rate = None
        
        self.last_user_id = None
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

        if 'metrics' in event.get('device', {}):
            self.last_heart_rate = event['device']['metrics'].get('heart_rate_bpm', self.last_heart_rate)

    def update_from_network_event(self, event):
        """Updates the device's state from a NetworkAuthLog event."""
        self.last_network_update = event.get('timestamp')
        if 'user' in event:
            self.last_user_id = event['user'].get('id', self.last_user_id)
        if 'action' in event:
            self.last_action_from_device = event['action'].get('type', self.last_action_from_device)
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
        self.last_update = None
        
        self.failed_login_count = 0
        self.event_timestamps = deque() 
        self.known_users_set = set()
        self.user_history_5min = deque() 
        self.common_actions_set = set()

    def update_from_network_event(self, event):
        """Updates the network endpoint's state from a NetworkAuthLog event."""
        event_time_str = event.get('timestamp')
        self.last_update = event_time_str
        
        try:
            event_time = datetime.fromisoformat(event_time_str.replace('Z', '+00:00'))
        except (ValueError, TypeError):
            event_time = datetime.now() # Fallback

        action_type = event.get('action', {}).get('type')
        user_id = event.get('user', {}).get('id')

        if action_type == 'login_failure':
            self.failed_login_count += 1
        elif action_type == 'login_success':
            self.failed_login_count = 0 

        self.event_timestamps.append(event_time)
        
        if user_id and user_id != 'unknown':
            self.known_users_set.add(user_id)

        if user_id and user_id != 'unknown':
            self.user_history_5min.append((event_time, user_id))

        if action_type:
            self.common_actions_set.add(action_type)
            
        self._prune_stats(event_time)

    def _prune_stats(self, current_time):
        """Helper to remove old data from deques."""
        while self.event_timestamps and self.event_timestamps[0] < current_time - timedelta(hours=STATS_WINDOW_HOURS):
            self.event_timestamps.popleft()
            
        while self.user_history_5min and self.user_history_5min[0][0] < current_time - timedelta(minutes=STATS_WINDOW_MINUTES):
            self.user_history_5min.popleft()
    
    def get_access_count_1hr(self):
        return len(self.event_timestamps)

    def get_normal_hours_set(self):
        return {t.hour for t in self.event_timestamps}

    def get_unique_users_5min(self):
        return len(set(user for time, user in self.user_history_5min))

    def get_event_count_1min(self, current_time):
        count = 0
        for t in reversed(self.event_timestamps):
            if t > current_time - timedelta(minutes=IP_ACCESS_RATE_MINUTES):
                count += 1
            else:
                break
        return count

    def get_state(self):
        """Returns the current state as a dictionary."""
        state = self.__dict__.copy()
        state['event_timestamps'] = list(self.event_timestamps)
        state['known_users_set'] = list(self.known_users_set)
        state['user_history_5min'] = list(self.user_history_5min)
        state['common_actions_set'] = list(self.common_actions_set)
        return state