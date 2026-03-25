from datetime import datetime, timedelta

# --- STATISTICAL THRESHOLDS ---
HIGH_ACCESS_COUNT = 50       # B1: 50 events/hr
RAPID_USER_HOPPING = 3     # B4: 3 users/5min
HIGH_EVENT_RATE = 100        # B6: 100 events/min
UNREALISTIC_HR_MAX = 220   # B7
UNREALISTIC_HR_MIN = 30    # B7
UNREALISTIC_SPO2_MIN = 60  # B7
INCONSISTENT_SPO2_MAX = 80 # B8
INCONSISTENT_HR_MAX = 80   # B8
STUCK_DATA_COUNT = 4       # B9
VOLATILITY_HR_JUMP = 50    # B10
DEVICE_INTERVAL_SECS = 300 # B11: 5 minutes

class AnomalyDetector:
    """
    Implements Layer 4 (Stage 1): Anomaly Detection.
    Checks an event against all 22 defined rules.
    """
    def __init__(self):
        print("✅ Layer 4 (Detector) initialized.")
        self.rules = self._get_all_rules()

    def check_event(self, event, dt_manager):
        """
        Runs the event and DT context against all rules.
        """
        triggered_anomalies = []
        for rule_func in self.rules:
            anomaly_id = rule_func(event, dt_manager)
            if anomaly_id:
                triggered_anomalies.append(anomaly_id)
        
        return triggered_anomalies

    def _get_all_rules(self):
        """Dynamically gets all methods starting with 'rule_'."""
        return [getattr(self, func) for func in dir(self) if callable(getattr(self, func)) and func.startswith('rule_')]

    # --- A. STATIC RULES (R1 - R11) ---

    def rule_R1(self, event, dt):
        if event['eventType'] == 'NetworkAuthLog':
            if event.get('user', {}).get('is_on_shift') == False:
                return "R1:OFF_SHIFT_ACCESS"
        return None

    def rule_R2(self, event, dt):
        if event['eventType'] == 'NetworkAuthLog':
            user_dept = event.get('user', {}).get('department')
            device_dept = event.get('network', {}).get('source_device_details', {}).get('department')
            if user_dept and device_dept and user_dept != device_dept:
                return "R2:CROSS_DEPARTMENTAL_ACCESS"
        return None

    def rule_R3(self, event, dt):
        if event['eventType'] == 'NetworkAuthLog':
            action = event.get('action', {}).get('type')
            level = event.get('user', {}).get('access_level')
            if action == 'DELETE_RECORD' and level is not None and level < 5:
                return "R3:INSUFFICIENT_PRIVILEGE_ATTEMPT"
        return None

    def rule_R4(self, event, dt):
        if event['eventType'] == 'NetworkAuthLog':
            target_id = event.get('action', {}).get('target_resource_id', '')
            if target_id.startswith('PAT-'):
                patient_dt = dt.get_patient_dt(target_id)
                if patient_dt and patient_dt.status == 'Discharged':
                    return "R4:DISCHARGED_PATIENT_ACCESS"
        return None

    def rule_R5(self, event, dt):
        if event['eventType'] == 'NetworkAuthLog':
            role = event.get('user', {}).get('role')
            action = event.get('action', {}).get('type')
            if role == 'IT_Admin' and action == 'VIEW_PATIENT_VITALS':
                return "R5:ROLE_ACTION_MISMATCH"
        return None

    def rule_R6(self, event, dt):
        if event['eventType'] == 'NetworkAuthLog':
            role = event.get('user', {}).get('role')
            log_source = event.get('network', {}).get('log_source')
            if role == 'Doctor' and log_source == 'Patient_Kiosk':
                return "R6:NON_STANDARD_LOG_SOURCE"
        return None

    def rule_R7(self, event, dt):
        if event['eventType'] == 'NetworkAuthLog':
            ip = event.get('network', {}).get('source_ip')
            action = event.get('action', {}).get('type')
            if ip and action == 'login_success':
                network_dt = dt.get_network_dt(ip)
                if network_dt and network_dt.failed_login_count > 5:
                    return "R7:LOGIN_AFTER_FAILED_ATTEMPTS"
        return None

    def rule_R8(self, event, dt):
        if event['eventType'] == 'MedicalDeviceLog':
            dev_id = event.get('device', {}).get('id')
            device_dt = dt.get_device_dt(dev_id)
            if device_dt and device_dt.status in ['Offline', 'Storage']:
                return "R8:GHOST_DEVICE_ACTIVITY"
        return None

    def rule_R9(self, event, dt):
        if event['eventType'] == 'MedicalDeviceLog':
            dev_id = event.get('device', {}).get('id')
            pat_id = event.get('patient', {}).get('id')
            device_dt = dt.get_device_dt(dev_id)
            if device_dt and device_dt.current_patient_id is not None and device_dt.current_patient_id != pat_id:
                return "R9:DEVICE_PATIENT_MISMATCH"
        return None

    def rule_R10(self, event, dt):
        if event['eventType'] == 'MedicalDeviceLog':
            dev_loc = event.get('device', {}).get('location')
            pat_id = event.get('patient', {}).get('id')
            patient_dt = dt.get_patient_dt(pat_id)
            if patient_dt and dev_loc and patient_dt.current_room != dev_loc:
                return "R10:DEVICE_ROOM_MISMATCH"
        return None

    def rule_R11(self, event, dt):
        if event['eventType'] == 'MedicalDeviceLog':
            dev_id = event.get('device', {}).get('id')
            device_dt = dt.get_device_dt(dev_id)
            if device_dt and device_dt.current_patient_id is None:
                return "R11:UNASSIGNED_DEVICE_ACTIVITY"
        return None

    # --- B. BASELINE RULES (B1 - B11) ---

    def rule_B1(self, event, dt):
        if event['eventType'] == 'NetworkAuthLog':
            ip = event.get('network', {}).get('source_ip')
            network_dt = dt.get_network_dt(ip)
            if network_dt and network_dt.get_access_count_1hr() > HIGH_ACCESS_COUNT:
                return "B1:HIGH_VOLUME_ACCESS_IP"
        return None

    def rule_B2(self, event, dt):
        if event['eventType'] == 'NetworkAuthLog':
            ip = event.get('network', {}).get('source_ip')
            network_dt = dt.get_network_dt(ip)
            try:
                event_time = datetime.fromisoformat(event.get('timestamp').replace('Z', '+00:00'))
                if network_dt and (event_time.hour not in network_dt.get_normal_hours_set()):
                     if len(network_dt.event_timestamps) > 10: 
                        return "B2:ATYPICAL_TIME_OF_ACCESS_IP"
            except: pass
        return None

    def rule_B3(self, event, dt):
        if event['eventType'] == 'NetworkAuthLog':
            ip = event.get('network', {}).get('source_ip')
            user = event.get('user', {}).get('id')
            network_dt = dt.get_network_dt(ip)
            if network_dt and user and (user not in network_dt.known_users_set):
                if len(network_dt.known_users_set) > 2: 
                    return "B3:ATYPICAL_USER_FOR_IP"
        return None

    def rule_B4(self, event, dt):
        if event['eventType'] == 'NetworkAuthLog':
            ip = event.get('network', {}).get('source_ip')
            network_dt = dt.get_network_dt(ip)
            if network_dt and network_dt.get_unique_users_5min() > RAPID_USER_HOPPING:
                return "B4:RAPID_USER_HOPPING_IP"
        return None

    def rule_B5(self, event, dt):
        if event['eventType'] == 'NetworkAuthLog':
            ip = event.get('network', {}).get('source_ip')
            action = event.get('action', {}).get('type')
            network_dt = dt.get_network_dt(ip)
            if network_dt and action and (action not in network_dt.common_actions_set):
                if len(network_dt.common_actions_set) > 5: 
                    return "B5:ATYPICAL_ACTION_FOR_IP"
        return None
        
    def rule_B6(self, event, dt):
        if event['eventType'] == 'NetworkAuthLog':
            ip = event.get('network', {}).get('source_ip')
            network_dt = dt.get_network_dt(ip)
            try:
                event_time = datetime.fromisoformat(event.get('timestamp').replace('Z', '+00:00'))
                if network_dt and network_dt.get_event_count_1min(event_time) > HIGH_EVENT_RATE:
                    return "B6:HIGH_FREQUENCY_EVENTS_IP"
            except: pass
        return None

    def rule_B7(self, event, dt):
        if event['eventType'] == 'MedicalDeviceLog':
            metrics = event.get('device', {}).get('metrics', {})
            hr = metrics.get('heart_rate_bpm')
            spo2 = metrics.get('spo2_percent')
            if (hr is not None and (hr > UNREALISTIC_HR_MAX or hr < UNREALISTIC_HR_MIN)) or \
               (spo2 is not None and spo2 < UNREALISTIC_SPO2_MIN):
                return "B7:UNREALISTIC_VITALS"
        return None

    def rule_B8(self, event, dt):
        if event['eventType'] == 'MedicalDeviceLog':
            metrics = event.get('device', {}).get('metrics', {})
            hr = metrics.get('heart_rate_bpm')
            spo2 = metrics.get('spo2_percent')
            if (hr is not None and spo2 is not None) and (spo2 < INCONSISTENT_SPO2_MAX and hr < INCONSISTENT_HR_MAX):
                return "B8:INCONSISTENT_VITALS"
        return None

    def rule_B9(self, event, dt):
        if event['eventType'] == 'MedicalDeviceLog':
            pat_id = event.get('patient', {}).get('id')
            patient_dt = dt.get_patient_dt(pat_id)
            hr = event.get('device', {}).get('metrics', {}).get('heart_rate_bpm')
            if patient_dt and hr is not None:
                if len(patient_dt.last_5_heart_rates) >= STUCK_DATA_COUNT and \
                   all(h == hr for h in patient_dt.last_5_heart_rates):
                    return "B9:STUCK_DEVICE_DATA"
        return None

    def rule_B10(self, event, dt):
        if event['eventType'] == 'MedicalDeviceLog':
            dev_id = event.get('device', {}).get('id')
            device_dt = dt.get_device_dt(dev_id)
            hr = event.get('device', {}).get('metrics', {}).get('heart_rate_bpm')
            if device_dt and device_dt.last_heart_rate is not None and hr is not None:
                if abs(hr - device_dt.last_heart_rate) > VOLATILITY_HR_JUMP:
                    return "B10:HIGH_DATA_VOLATILITY"
        return None

    def rule_B11(self, event, dt):
        if event['eventType'] == 'MedicalDeviceLog':
            dev_id = event.get('device', {}).get('id')
            device_dt = dt.get_device_dt(dev_id)
            try:
                event_time = datetime.fromisoformat(event.get('timestamp').replace('Z', '+00:00'))
                if device_dt and device_dt.last_metric_update:
                    last_update_time = datetime.fromisoformat(device_dt.last_metric_update.replace('Z', '+00:00'))
                    if (event_time - last_update_time).total_seconds() > DEVICE_INTERVAL_SECS:
                        return "B11:DEVICE_DATA_RATE_ANOMALY"
            except: pass
        return None