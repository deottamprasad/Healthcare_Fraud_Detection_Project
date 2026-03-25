# File: src/pre_detection/contextualizer.py

class ConsistencyChecker:
    """
    Implements Layer 4 (Stage 2): Consistency Check.
    Checks a list of flagged anomalies for "known, innocent explanations."
    """
    
    ROLES_ADMIN = {'IT_Admin', 'Head_of_Department', 'On_Call_Surgeon'}
    ROLES_FLOATER = {'IT_Admin', 'Floater_Nurse', 'Internal_Auditor'}
    ROLES_AUDIT = {'Billing_Admin', 'Research_Lead', 'Head_of_Department'}
    ROLES_TECH = {'IT_Admin', 'Biomed_Engineer'}
    
    def __init__(self):
        print("✅ Layer 4 (Contextualizer) initialized.")
        self.check_map = self._get_check_map()

    def filter_anomalies(self, anomalies, event, dt_manager):
        """
        Filters a list of anomalies, removing any that are benign.
        """
        suspicious_anomalies = []
        for anomaly_id in anomalies:
            is_benign = self._check_consistency(anomaly_id, event, dt_manager)
            if not is_benign:
                suspicious_anomalies.append(anomaly_id)
        return suspicious_anomalies

    def _check_consistency(self, anomaly_id, event, dt):
        """Runs the specific check for a given anomaly."""
        check_function = self.check_map.get(anomaly_id.split(':')[0])
        
        if check_function:
            return check_function(anomaly_id, event, dt)
        
        return False # Default: suspicious

    def _get_check_map(self):
        """Builds the map of Anomaly IDs to check functions."""
        return {
            "R1": self._check_R1,
            "R2": self._check_R2,
            "R3": self._check_R3,
            "R4": self._check_R4,
            "R5": self._check_R5,
            "R6": self._check_R6,
            "R7": self._check_R7,
            "R8": self._check_device,
            "R9": self._check_device,
            "R10": self._check_device,
            "R11": self._check_device,
            "B1": self._check_B1,
            "B2": self._check_B2,
            "B3": self._check_B3,
            "B4": self._check_B4,
            "B10": self._check_B10,
        }

    # --- Specific Check Functions (Return True if BENIGN) ---

    def _check_R1(self, aid, e, dt): # R1: Off-Shift
        return e.get('user', {}).get('role') in self.ROLES_ADMIN

    def _check_R2(self, aid, e, dt): # R2: Cross-Dept
        return e.get('user', {}).get('role') in self.ROLES_FLOATER

    def _check_R3(self, aid, e, dt): # R3: Insufficient Priv
        is_admin = e.get('user', {}).get('role') == 'IT_Admin'
        is_test = 'TEST_' in e.get('action', {}).get('target_resource_id', '')
        return is_admin and is_test

    def _check_R4(self, aid, e, dt): # R4: Discharged Pt
        return e.get('user', {}).get('role') in {'Billing_Admin', 'Records_Clerk'}

    def _check_R5(self, aid, e, dt): # R5: Role-Action Mismatch
        if e.get('user', {}).get('role') in self.ROLES_TECH:
            target_id = e.get('action', {}).get('target_resource_id', '')
            if target_id.startswith('PAT-'):
                p_dt = dt.get_patient_dt(target_id)
                # Benign if tech is accessing a patient in maintenance
                return p_dt and p_dt.status == 'Maintenance'
        return False

    def _check_R6(self, aid, e, dt): # R6: Non-Standard Src
        return e.get('network', {}).get('log_source') == 'NURSE_STATION_SHARED'

    def _check_R7(self, aid, e, dt): # R7: Login after Fails
        return e.get('user', {}).get('role') in self.ROLES_TECH

    def _check_device(self, aid, e, dt): # R8, R9, R10, R11
        dev_id = e.get('device', {}).get('id')
        d_dt = dt.get_device_dt(dev_id)
        return d_dt and d_dt.status in ['Maintenance', 'Testing']

    def _check_B1(self, aid, e, dt): # B1: High-Volume
        return e.get('user', {}).get('role') in self.ROLES_AUDIT

    def _check_B2(self, aid, e, dt): # B2: Atypical Time
        return e.get('user', {}).get('role') in self.ROLES_ADMIN

    def _check_B3(self, aid, e, dt): # B3: Atypical User
        return e.get('user', {}).get('role') in self.ROLES_FLOATER

    def _check_B4(self, aid, e, dt): # B4: Rapid Hopping
        return e.get('network', {}).get('log_source') == 'NURSE_STATION_SHARED'

    def _check_B10(self, aid, e, dt): # B10: Volatile Vitals
        pat_id = e.get('patient', {}).get('id')
        p_dt = dt.get_patient_dt(pat_id)
        return p_dt and (p_dt.current_room == 'ER' or p_dt.status == 'Critical')