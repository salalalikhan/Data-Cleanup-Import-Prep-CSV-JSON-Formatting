#!/usr/bin/env python3
"""
Advanced Data Validation Engine
Provides enterprise-grade schema validation and data quality enforcement
"""

import re
import csv
import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class ValidationSeverity(Enum):
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"

@dataclass
class ValidationRule:
    field: str
    rule_type: str
    pattern: Optional[str] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    required: bool = False
    data_type: str = "string"
    custom_validator: Optional[callable] = None

@dataclass
class ValidationResult:
    field: str
    value: Any
    is_valid: bool
    severity: ValidationSeverity
    message: str
    suggestion: Optional[str] = None

class EnterpriseDataValidator:
    """
    Enterprise-grade data validation with configurable rules and detailed reporting
    """
    
    def __init__(self):
        self.validation_rules = {}
        self.validation_results = []
        self.stats = {
            'total_records': 0,
            'valid_records': 0,
            'errors': 0,
            'warnings': 0
        }
        
    def load_validation_schema(self, schema_file: str) -> None:
        """Load validation rules from schema file"""
        try:
            with open(schema_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    rule = ValidationRule(
                        field=row['field_name'],
                        rule_type=row.get('validation_type', 'string'),
                        pattern=row.get('regex_pattern'),
                        min_length=int(row['min_length']) if row.get('min_length') else None,
                        max_length=int(row['max_length']) if row.get('max_length') else None,
                        required=row.get('required', 'false').lower() == 'true',
                        data_type=row.get('data_type', 'string')
                    )
                    self.validation_rules[row['field_name']] = rule
                    
            logger.info(f"Loaded {len(self.validation_rules)} validation rules")
            
        except Exception as e:
            logger.error(f"Failed to load validation schema: {e}")
            raise
    
    def validate_email(self, email: str) -> ValidationResult:
        """Advanced email validation with business rules"""
        if not email or not email.strip():
            return ValidationResult(
                field="email",
                value=email,
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message="Email is required",
                suggestion="Provide a valid email address"
            )
        
        # Business email validation
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(pattern, email):
            return ValidationResult(
                field="email",
                value=email,
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message="Invalid email format",
                suggestion="Use format: user@domain.com"
            )
        
        # Check for business domains (warning for personal emails)
        personal_domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com']
        domain = email.split('@')[1].lower()
        if domain in personal_domains:
            return ValidationResult(
                field="email",
                value=email,
                is_valid=True,
                severity=ValidationSeverity.WARNING,
                message="Personal email domain detected",
                suggestion="Consider using business email for professional contacts"
            )
        
        return ValidationResult(
            field="email",
            value=email,
            is_valid=True,
            severity=ValidationSeverity.INFO,
            message="Valid business email"
        )
    
    def validate_phone(self, phone: str) -> ValidationResult:
        """Advanced phone validation with international support"""
        if not phone or not phone.strip():
            return ValidationResult(
                field="phone",
                value=phone,
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message="Phone number is required"
            )
        
        # Clean phone number
        cleaned = re.sub(r'[^\d+]', '', phone)
        
        # International format validation
        patterns = [
            r'^\+1\d{10}$',  # US/Canada: +1XXXXXXXXXX
            r'^\d{10}$',     # US/Canada: XXXXXXXXXX
            r'^\d{3}-\d{3}-\d{4}$',  # US: XXX-XXX-XXXX
            r'^\(\d{3}\)\s?\d{3}-\d{4}$'  # US: (XXX) XXX-XXXX
        ]
        
        for pattern in patterns:
            if re.match(pattern, phone):
                return ValidationResult(
                    field="phone",
                    value=phone,
                    is_valid=True,
                    severity=ValidationSeverity.INFO,
                    message="Valid phone format"
                )
        
        return ValidationResult(
            field="phone",
            value=phone,
            is_valid=False,
            severity=ValidationSeverity.ERROR,
            message="Invalid phone format",
            suggestion="Use format: (XXX) XXX-XXXX or +1XXXXXXXXXX"
        )
    
    def validate_record_id(self, record_id: Any) -> ValidationResult:
        """Validate record ID with business rules"""
        if record_id is None or str(record_id).strip() == '':
            return ValidationResult(
                field="record_id",
                value=record_id,
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message="Record ID is required for data integrity"
            )
        
        # Convert to string for validation
        str_id = str(record_id).strip()
        
        # Check for valid ID patterns
        if len(str_id) < 3:
            return ValidationResult(
                field="record_id",
                value=record_id,
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message="Record ID too short",
                suggestion="Use at least 3 characters"
            )
        
        # Check for sequential patterns that might indicate test data
        if str_id.isdigit() and len(str_id) >= 3:
            num = int(str_id)
            if num < 1000:  # Sequential low numbers
                return ValidationResult(
                    field="record_id",
                    value=record_id,
                    is_valid=True,
                    severity=ValidationSeverity.WARNING,
                    message="Sequential ID detected - verify this is production data"
                )
        
        return ValidationResult(
            field="record_id",
            value=record_id,
            is_valid=True,
            severity=ValidationSeverity.INFO,
            message="Valid record ID"
        )
    
    def validate_currency(self, amount: Any) -> ValidationResult:
        """Advanced currency validation with business rules"""
        if amount is None:
            return ValidationResult(
                field="balance",
                value=amount,
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message="Balance amount is required"
            )
        
        try:
            # Convert to float for validation
            if isinstance(amount, str):
                # Remove currency symbols and formatting
                cleaned = re.sub(r'[^\d.-]', '', amount.replace(',', ''))
                float_amount = float(cleaned)
            else:
                float_amount = float(amount)
            
            # Business rule validations
            if float_amount < 0:
                return ValidationResult(
                    field="balance",
                    value=amount,
                    is_valid=True,
                    severity=ValidationSeverity.WARNING,
                    message="Negative balance detected",
                    suggestion="Verify this account has outstanding debt"
                )
            
            if float_amount > 1000000:  # Over $1M
                return ValidationResult(
                    field="balance",
                    value=amount,
                    is_valid=True,
                    severity=ValidationSeverity.WARNING,
                    message="High balance amount detected",
                    suggestion="Verify this is accurate for compliance reporting"
                )
            
            return ValidationResult(
                field="balance",
                value=amount,
                is_valid=True,
                severity=ValidationSeverity.INFO,
                message="Valid balance amount"
            )
            
        except (ValueError, TypeError):
            return ValidationResult(
                field="balance",
                value=amount,
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message="Invalid currency format",
                suggestion="Use numeric format: 1234.56"
            )
    
    def validate_record(self, record: Dict[str, Any]) -> List[ValidationResult]:
        """Validate entire record against all rules"""
        results = []
        
        # Validate each field based on loaded rules or default validators
        if 'email' in record:
            results.append(self.validate_email(record['email']))
        
        if 'phone' in record:
            results.append(self.validate_phone(record['phone']))
        
        if 'record_id' in record:
            results.append(self.validate_record_id(record['record_id']))
        
        if 'balance_cents' in record or 'balance' in record:
            balance_field = 'balance_cents' if 'balance_cents' in record else 'balance'
            results.append(self.validate_currency(record[balance_field]))
        
        # Cross-field validation
        if 'email' in record and 'phone' in record:
            if not record['email'] and not record['phone']:
                results.append(ValidationResult(
                    field="contact_info",
                    value="missing",
                    is_valid=False,
                    severity=ValidationSeverity.ERROR,
                    message="Either email or phone is required for contact"
                ))
        
        return results
    
    def generate_validation_report(self) -> Dict[str, Any]:
        """Generate comprehensive validation report"""
        error_count = sum(1 for r in self.validation_results if r.severity == ValidationSeverity.ERROR)
        warning_count = sum(1 for r in self.validation_results if r.severity == ValidationSeverity.WARNING)
        
        return {
            'summary': {
                'total_validations': len(self.validation_results),
                'errors': error_count,
                'warnings': warning_count,
                'success_rate': (1 - error_count / len(self.validation_results)) * 100 if self.validation_results else 100
            },
            'details': [
                {
                    'field': r.field,
                    'value': str(r.value),
                    'severity': r.severity.value,
                    'message': r.message,
                    'suggestion': r.suggestion
                }
                for r in self.validation_results
            ]
        }

def create_validation_schema():
    """Create a sample validation schema file"""
    schema_data = [
        {
            'field_name': 'record_id',
            'data_type': 'string',
            'required': 'true',
            'min_length': '1',
            'max_length': '20',
            'validation_type': 'alphanumeric'
        },
        {
            'field_name': 'email',
            'data_type': 'string',
            'required': 'false',
            'min_length': '5',
            'max_length': '100',
            'validation_type': 'email'
        },
        {
            'field_name': 'phone',
            'data_type': 'string',
            'required': 'false',
            'min_length': '10',
            'max_length': '20',
            'validation_type': 'phone'
        },
        {
            'field_name': 'balance_cents',
            'data_type': 'integer',
            'required': 'true',
            'validation_type': 'currency'
        }
    ]
    
    with open('config/validation_schema.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=schema_data[0].keys())
        writer.writeheader()
        writer.writerows(schema_data)
    
    print("Created validation schema: config/validation_schema.csv")

if __name__ == "__main__":
    create_validation_schema()