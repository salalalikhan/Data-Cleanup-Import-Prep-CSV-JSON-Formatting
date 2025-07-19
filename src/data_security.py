#!/usr/bin/env python3
"""
Enterprise Data Security and PII Protection System
Provides data masking, encryption, and compliance features for sensitive data handling
"""

import re
import hashlib
import hmac
import base64
import logging
from typing import Dict, List, Any, Optional, Set
from dataclasses import dataclass
from enum import Enum
import json
from datetime import datetime

logger = logging.getLogger(__name__)

class PIIType(Enum):
    EMAIL = "EMAIL"
    PHONE = "PHONE"
    SSN = "SSN"
    CREDIT_CARD = "CREDIT_CARD"
    NAME = "NAME"
    ADDRESS = "ADDRESS"
    DATE_OF_BIRTH = "DATE_OF_BIRTH"
    BANK_ACCOUNT = "BANK_ACCOUNT"

class MaskingStrategy(Enum):
    PARTIAL = "PARTIAL"      # Show first/last characters
    HASH = "HASH"           # Replace with hash
    TOKENIZE = "TOKENIZE"   # Replace with token
    REDACT = "REDACT"       # Replace with [REDACTED]
    PSEUDONYM = "PSEUDONYM" # Replace with fake but realistic data

@dataclass
class PIIDetection:
    field_name: str
    pii_type: PIIType
    confidence: float
    pattern_matched: str
    suggested_masking: MaskingStrategy

@dataclass
class AuditLogEntry:
    timestamp: str
    user_id: str
    action: str
    data_type: str
    record_count: int
    pii_fields_accessed: List[str]
    masking_applied: bool

class EnterpriseDataSecurity:
    """
    Enterprise-grade data security with PII detection, masking, and audit logging
    """
    
    def __init__(self, encryption_key: Optional[str] = None):
        self.encryption_key = encryption_key or self._generate_key()
        self.pii_patterns = self._initialize_pii_patterns()
        self.audit_log = []
        self.masking_rules = {}
        self.detected_pii_fields = set()
        
        # Create secure audit directory
        import os
        os.makedirs("outputs/security_audit", exist_ok=True)
    
    def _generate_key(self) -> str:
        """Generate encryption key for data protection"""
        import secrets
        return base64.b64encode(secrets.token_bytes(32)).decode('utf-8')
    
    def _initialize_pii_patterns(self) -> Dict[PIIType, List[Dict]]:
        """Initialize PII detection patterns"""
        return {
            PIIType.EMAIL: [
                {
                    'pattern': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
                    'confidence': 0.95,
                    'description': 'Standard email format'
                }
            ],
            PIIType.PHONE: [
                {
                    'pattern': r'\b(?:\+?1[-.\s]?)?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})\b',
                    'confidence': 0.90,
                    'description': 'US/Canada phone number'
                },
                {
                    'pattern': r'\b\d{3}-\d{3}-\d{4}\b',
                    'confidence': 0.95,
                    'description': 'Formatted phone number'
                }
            ],
            PIIType.SSN: [
                {
                    'pattern': r'\b\d{3}-\d{2}-\d{4}\b',
                    'confidence': 0.98,
                    'description': 'US Social Security Number'
                },
                {
                    'pattern': r'\b\d{9}\b',
                    'confidence': 0.70,
                    'description': 'Potential SSN (9 digits)'
                }
            ],
            PIIType.CREDIT_CARD: [
                {
                    'pattern': r'\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|3[0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12})\b',
                    'confidence': 0.95,
                    'description': 'Credit card number'
                }
            ],
            PIIType.NAME: [
                {
                    'pattern': r'\b[A-Z][a-z]+ [A-Z][a-z]+\b',
                    'confidence': 0.60,
                    'description': 'Potential full name'
                }
            ]
        }
    
    def detect_pii(self, text: str, field_name: str) -> List[PIIDetection]:
        """Detect PII in text with confidence scoring"""
        detections = []
        
        if not text or not isinstance(text, str):
            return detections
        
        for pii_type, patterns in self.pii_patterns.items():
            for pattern_info in patterns:
                pattern = pattern_info['pattern']
                matches = re.finditer(pattern, text, re.IGNORECASE)
                
                for match in matches:
                    # Adjust confidence based on field name context
                    confidence = pattern_info['confidence']
                    
                    # Boost confidence if field name suggests PII
                    field_lower = field_name.lower()
                    if pii_type == PIIType.EMAIL and 'email' in field_lower:
                        confidence = min(0.99, confidence + 0.15)
                    elif pii_type == PIIType.PHONE and ('phone' in field_lower or 'tel' in field_lower):
                        confidence = min(0.99, confidence + 0.15)
                    elif pii_type == PIIType.NAME and ('name' in field_lower or 'client' in field_lower):
                        confidence = min(0.99, confidence + 0.20)
                    
                    detection = PIIDetection(
                        field_name=field_name,
                        pii_type=pii_type,
                        confidence=confidence,
                        pattern_matched=match.group(),
                        suggested_masking=self._suggest_masking_strategy(pii_type, confidence)
                    )
                    detections.append(detection)
                    
                    # Track detected PII fields
                    self.detected_pii_fields.add(field_name)
        
        return detections
    
    def _suggest_masking_strategy(self, pii_type: PIIType, confidence: float) -> MaskingStrategy:
        """Suggest appropriate masking strategy based on PII type and confidence"""
        
        if confidence < 0.7:
            return MaskingStrategy.PARTIAL
        
        strategy_map = {
            PIIType.EMAIL: MaskingStrategy.PARTIAL,
            PIIType.PHONE: MaskingStrategy.PARTIAL,
            PIIType.SSN: MaskingStrategy.HASH,
            PIIType.CREDIT_CARD: MaskingStrategy.TOKENIZE,
            PIIType.NAME: MaskingStrategy.PSEUDONYM,
            PIIType.ADDRESS: MaskingStrategy.REDACT,
            PIIType.DATE_OF_BIRTH: MaskingStrategy.HASH
        }
        
        return strategy_map.get(pii_type, MaskingStrategy.REDACT)
    
    def mask_data(self, value: str, strategy: MaskingStrategy, pii_type: PIIType) -> str:
        """Apply masking strategy to sensitive data"""
        
        if not value or not isinstance(value, str):
            return value
        
        if strategy == MaskingStrategy.PARTIAL:
            return self._partial_mask(value, pii_type)
        elif strategy == MaskingStrategy.HASH:
            return self._hash_value(value)
        elif strategy == MaskingStrategy.TOKENIZE:
            return self._tokenize_value(value, pii_type)
        elif strategy == MaskingStrategy.REDACT:
            return f"[REDACTED_{pii_type.value}]"
        elif strategy == MaskingStrategy.PSEUDONYM:
            return self._generate_pseudonym(value, pii_type)
        
        return value
    
    def _partial_mask(self, value: str, pii_type: PIIType) -> str:
        """Apply partial masking showing only first/last characters"""
        
        if pii_type == PIIType.EMAIL:
            if '@' in value:
                local, domain = value.split('@', 1)
                if len(local) > 2:
                    masked_local = local[0] + '*' * (len(local) - 2) + local[-1]
                else:
                    masked_local = '*' * len(local)
                return f"{masked_local}@{domain}"
        
        elif pii_type == PIIType.PHONE:
            # Keep area code visible: (555) ***-**34
            digits = re.sub(r'\D', '', value)
            if len(digits) >= 10:
                return f"({digits[:3]}) ***-**{digits[-2:]}"
        
        elif pii_type == PIIType.NAME:
            # Show first name, mask last name: John D***
            parts = value.split()
            if len(parts) >= 2:
                masked_parts = [parts[0]]  # Keep first name
                for part in parts[1:]:
                    if len(part) > 1:
                        masked_parts.append(part[0] + '*' * (len(part) - 1))
                    else:
                        masked_parts.append('*')
                return ' '.join(masked_parts)
        
        # Default partial masking
        if len(value) > 4:
            return value[:2] + '*' * (len(value) - 4) + value[-2:]
        else:
            return '*' * len(value)
    
    def _hash_value(self, value: str) -> str:
        """Create secure hash of sensitive value"""
        
        # Use HMAC for secure hashing
        signature = hmac.new(
            self.encryption_key.encode('utf-8'),
            value.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        return f"HASH_{signature[:12]}"  # Truncated hash for readability
    
    def _tokenize_value(self, value: str, pii_type: PIIType) -> str:
        """Replace value with secure token"""
        
        # Generate deterministic token based on value
        hash_obj = hashlib.sha256((value + self.encryption_key).encode())
        token = hash_obj.hexdigest()[:16]
        
        return f"TOKEN_{pii_type.value}_{token}"
    
    def _generate_pseudonym(self, value: str, pii_type: PIIType) -> str:
        """Generate realistic but fake replacement data"""
        
        if pii_type == PIIType.NAME:
            # Simple pseudonym generator
            first_names = ['Alex', 'Jordan', 'Taylor', 'Casey', 'Morgan', 'Riley', 'Avery', 'Quinn']
            last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis']
            
            # Use hash to get consistent pseudonym for same input
            hash_val = int(hashlib.md5(value.encode()).hexdigest(), 16)
            first_idx = hash_val % len(first_names)
            last_idx = (hash_val // len(first_names)) % len(last_names)
            
            return f"{first_names[first_idx]} {last_names[last_idx]}"
        
        return f"PSEUDO_{pii_type.value}"
    
    def scan_record(self, record: Dict[str, Any]) -> Dict[str, List[PIIDetection]]:
        """Scan entire record for PII and return detections"""
        
        pii_detections = {}
        
        for field_name, value in record.items():
            if value is not None and isinstance(value, str):
                detections = self.detect_pii(value, field_name)
                if detections:
                    pii_detections[field_name] = detections
        
        return pii_detections
    
    def apply_data_protection(self, record: Dict[str, Any], 
                            protection_level: str = "medium") -> Dict[str, Any]:
        """Apply data protection based on detected PII"""
        
        protected_record = record.copy()
        pii_detections = self.scan_record(record)
        
        confidence_threshold = {
            "low": 0.5,
            "medium": 0.7,
            "high": 0.9
        }.get(protection_level, 0.7)
        
        for field_name, detections in pii_detections.items():
            for detection in detections:
                if detection.confidence >= confidence_threshold:
                    original_value = str(record[field_name])
                    masked_value = self.mask_data(
                        original_value,
                        detection.suggested_masking,
                        detection.pii_type
                    )
                    protected_record[field_name] = masked_value
                    
                    logger.info(f"Applied {detection.suggested_masking.value} masking to "
                              f"{field_name} (PII: {detection.pii_type.value}, "
                              f"confidence: {detection.confidence:.2f})")
        
        return protected_record
    
    def log_data_access(self, user_id: str, action: str, record_count: int,
                       pii_fields: List[str], masking_applied: bool = False) -> None:
        """Log data access for audit compliance"""
        
        audit_entry = AuditLogEntry(
            timestamp=datetime.now().isoformat(),
            user_id=user_id,
            action=action,
            data_type="sensitive_customer_data",
            record_count=record_count,
            pii_fields_accessed=pii_fields,
            masking_applied=masking_applied
        )
        
        self.audit_log.append(audit_entry)
        
        # Save to secure audit file
        audit_file = "outputs/security_audit/data_access_log.jsonl"
        with open(audit_file, 'a') as f:
            f.write(json.dumps({
                'timestamp': audit_entry.timestamp,
                'user_id': audit_entry.user_id,
                'action': audit_entry.action,
                'data_type': audit_entry.data_type,
                'record_count': audit_entry.record_count,
                'pii_fields_accessed': audit_entry.pii_fields_accessed,
                'masking_applied': audit_entry.masking_applied
            }) + '\n')
    
    def generate_privacy_report(self) -> Dict[str, Any]:
        """Generate comprehensive privacy compliance report"""
        
        total_records_processed = sum(entry.record_count for entry in self.audit_log)
        records_with_masking = sum(entry.record_count for entry in self.audit_log 
                                 if entry.masking_applied)
        
        # PII field frequency
        pii_field_usage = {}
        for entry in self.audit_log:
            for field in entry.pii_fields_accessed:
                pii_field_usage[field] = pii_field_usage.get(field, 0) + entry.record_count
        
        return {
            'privacy_compliance_summary': {
                'total_records_processed': total_records_processed,
                'records_with_pii_protection': records_with_masking,
                'protection_coverage': (records_with_masking / total_records_processed * 100) 
                                     if total_records_processed > 0 else 0,
                'detected_pii_fields': list(self.detected_pii_fields),
                'audit_log_entries': len(self.audit_log)
            },
            'pii_field_access_frequency': dict(sorted(pii_field_usage.items(), 
                                                    key=lambda x: x[1], reverse=True)),
            'compliance_recommendations': self._generate_privacy_recommendations(),
            'data_protection_measures': [
                "PII detection with confidence scoring",
                "Multi-strategy data masking (partial, hash, tokenize, pseudonym)",
                "Comprehensive audit logging",
                "Configurable protection levels",
                "Secure encryption key management"
            ]
        }
    
    def _generate_privacy_recommendations(self) -> List[str]:
        """Generate privacy compliance recommendations"""
        
        recommendations = []
        
        if len(self.detected_pii_fields) > 0:
            recommendations.append(
                f"Implement regular PII scanning - {len(self.detected_pii_fields)} "
                f"fields detected containing sensitive data"
            )
        
        if len(self.audit_log) == 0:
            recommendations.append("Enable audit logging for compliance tracking")
        
        recommendations.extend([
            "Implement data retention policies for sensitive information",
            "Consider additional encryption for data at rest",
            "Regular privacy impact assessments",
            "Staff training on data handling procedures",
            "Automated PII discovery for new data sources"
        ])
        
        return recommendations

if __name__ == "__main__":
    # Example usage
    security = EnterpriseDataSecurity()
    
    # Test PII detection
    test_record = {
        'client_name': 'John Smith',
        'email': 'john.smith@company.com',
        'phone': '(555) 123-4567',
        'balance': 1500
    }
    
    pii_found = security.scan_record(test_record)
    print(f"PII Detection initialized - found {len(pii_found)} PII fields")
    
    protected = security.apply_data_protection(test_record)
    print(f"Data protection applied - {len(protected)} fields processed")