#!/usr/bin/env python3
"""
Enterprise Error Handling and Recovery System
Provides robust error recovery, retry logic, and failure management
"""

import time
import logging
import json
import csv
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum
import traceback
from pathlib import Path

logger = logging.getLogger(__name__)

class ErrorType(Enum):
    DATA_VALIDATION = "DATA_VALIDATION"
    PROCESSING_ERROR = "PROCESSING_ERROR"
    IO_ERROR = "IO_ERROR"
    SYSTEM_ERROR = "SYSTEM_ERROR"
    CONFIGURATION_ERROR = "CONFIGURATION_ERROR"

class ErrorSeverity(Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

@dataclass
class ErrorRecord:
    timestamp: str
    error_type: ErrorType
    severity: ErrorSeverity
    record_id: Optional[str]
    field_name: Optional[str]
    error_message: str
    raw_value: Any
    stack_trace: Optional[str]
    retry_count: int = 0
    resolved: bool = False
    resolution_strategy: Optional[str] = None

class EnterpriseErrorHandler:
    """
    Enterprise-grade error handling with retry logic, quarantine, and recovery
    """
    
    def __init__(self, max_retries: int = 3, backoff_multiplier: float = 2.0):
        self.max_retries = max_retries
        self.backoff_multiplier = backoff_multiplier
        self.error_records = []
        self.quarantine_queue = []
        self.recovery_strategies = {}
        self.error_thresholds = {
            ErrorSeverity.CRITICAL: 5,
            ErrorSeverity.HIGH: 10,
            ErrorSeverity.MEDIUM: 25,
            ErrorSeverity.LOW: 50
        }
        self.circuit_breaker_state = False
        self.consecutive_failures = 0
        
        # Set up quarantine directory
        self.quarantine_dir = Path("outputs/quarantine")
        self.quarantine_dir.mkdir(exist_ok=True)
        
    def register_recovery_strategy(self, error_type: ErrorType, strategy: Callable):
        """Register custom recovery strategy for specific error types"""
        self.recovery_strategies[error_type] = strategy
        logger.info(f"Registered recovery strategy for {error_type.value}")
    
    def log_error(self, error_type: ErrorType, severity: ErrorSeverity, 
                  error_message: str, record_id: Optional[str] = None,
                  field_name: Optional[str] = None, raw_value: Any = None) -> ErrorRecord:
        """Log error with full context and stack trace"""
        
        error_record = ErrorRecord(
            timestamp=datetime.now().isoformat(),
            error_type=error_type,
            severity=severity,
            record_id=record_id,
            field_name=field_name,
            error_message=error_message,
            raw_value=raw_value,
            stack_trace=traceback.format_exc() if severity in [ErrorSeverity.CRITICAL, ErrorSeverity.HIGH] else None
        )
        
        self.error_records.append(error_record)
        
        # Log to system logger
        log_level = {
            ErrorSeverity.CRITICAL: logging.CRITICAL,
            ErrorSeverity.HIGH: logging.ERROR,
            ErrorSeverity.MEDIUM: logging.WARNING,
            ErrorSeverity.LOW: logging.INFO
        }[severity]
        
        logger.log(log_level, 
                  f"{error_type.value} - {error_message} "
                  f"(Record: {record_id}, Field: {field_name})")
        
        # Check circuit breaker
        if severity in [ErrorSeverity.CRITICAL, ErrorSeverity.HIGH]:
            self.consecutive_failures += 1
            if self.consecutive_failures >= 5:
                self.circuit_breaker_state = True
                logger.critical("Circuit breaker activated - too many consecutive failures")
        
        return error_record
    
    def retry_with_backoff(self, operation: Callable, *args, **kwargs) -> Any:
        """Execute operation with exponential backoff retry logic"""
        
        if self.circuit_breaker_state:
            raise Exception("Circuit breaker is open - system in failure state")
        
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                result = operation(*args, **kwargs)
                # Reset consecutive failures on success
                self.consecutive_failures = 0
                return result
                
            except Exception as e:
                last_exception = e
                
                if attempt < self.max_retries:
                    # Calculate backoff delay
                    delay = (self.backoff_multiplier ** attempt) + (time.time() % 1)  # Add jitter
                    logger.warning(f"Attempt {attempt + 1} failed, retrying in {delay:.2f}s: {str(e)}")
                    time.sleep(delay)
                else:
                    logger.error(f"All {self.max_retries + 1} attempts failed for operation")
        
        # All retries exhausted
        raise last_exception
    
    def quarantine_record(self, record: Dict[str, Any], reason: str) -> None:
        """Move problematic record to quarantine for manual review"""
        
        quarantine_record = {
            'timestamp': datetime.now().isoformat(),
            'reason': reason,
            'original_record': record,
            'error_count': len([e for e in self.error_records if e.record_id == record.get('record_id')])
        }
        
        self.quarantine_queue.append(quarantine_record)
        
        # Save to quarantine file
        quarantine_file = self.quarantine_dir / f"quarantine_{datetime.now().strftime('%Y%m%d')}.jsonl"
        
        with open(quarantine_file, 'a') as f:
            f.write(json.dumps(quarantine_record) + '\n')
        
        logger.warning(f"Record {record.get('record_id')} quarantined: {reason}")
    
    def attempt_recovery(self, error_record: ErrorRecord, original_data: Any) -> Optional[Any]:
        """Attempt to recover from error using registered strategies"""
        
        if error_record.error_type in self.recovery_strategies:
            try:
                recovery_func = self.recovery_strategies[error_record.error_type]
                recovered_data = recovery_func(error_record, original_data)
                
                error_record.resolved = True
                error_record.resolution_strategy = recovery_func.__name__
                
                logger.info(f"Successfully recovered from {error_record.error_type.value} "
                          f"using {recovery_func.__name__}")
                
                return recovered_data
                
            except Exception as e:
                logger.error(f"Recovery strategy failed: {str(e)}")
                error_record.retry_count += 1
        
        return None
    
    def escalate_error(self, error_record: ErrorRecord) -> None:
        """Escalate unresolved errors for human intervention"""
        
        escalation_data = {
            'timestamp': datetime.now().isoformat(),
            'error_summary': asdict(error_record),
            'escalation_reason': f"Failed to resolve after {error_record.retry_count} attempts",
            'suggested_actions': self._get_suggested_actions(error_record)
        }
        
        # Save escalation report
        escalation_file = self.quarantine_dir / "escalated_errors.jsonl"
        with open(escalation_file, 'a') as f:
            f.write(json.dumps(escalation_data) + '\n')
        
        logger.critical(f"Error escalated for manual intervention: {error_record.error_message}")
    
    def _get_suggested_actions(self, error_record: ErrorRecord) -> List[str]:
        """Generate suggested actions based on error type"""
        
        suggestions = {
            ErrorType.DATA_VALIDATION: [
                "Review data source for systematic issues",
                "Update validation rules if business requirements changed",
                "Consider data cleaning at source system"
            ],
            ErrorType.PROCESSING_ERROR: [
                "Check for recent code changes",
                "Verify system resources (memory, disk space)",
                "Review transformation logic for edge cases"
            ],
            ErrorType.IO_ERROR: [
                "Check file permissions and disk space",
                "Verify network connectivity for remote files",
                "Consider file corruption issues"
            ],
            ErrorType.SYSTEM_ERROR: [
                "Check system health and resource usage",
                "Review recent infrastructure changes",
                "Consider scaling up resources"
            ]
        }
        
        return suggestions.get(error_record.error_type, ["Manual investigation required"])
    
    def generate_error_report(self) -> Dict[str, Any]:
        """Generate comprehensive error analysis report"""
        
        # Error distribution by type
        error_by_type = {}
        for error in self.error_records:
            error_type = error.error_type.value
            error_by_type[error_type] = error_by_type.get(error_type, 0) + 1
        
        # Error distribution by severity
        error_by_severity = {}
        for error in self.error_records:
            severity = error.severity.value
            error_by_severity[severity] = error_by_severity.get(severity, 0) + 1
        
        # Recovery success rate
        total_errors = len(self.error_records)
        resolved_errors = len([e for e in self.error_records if e.resolved])
        recovery_rate = (resolved_errors / total_errors * 100) if total_errors > 0 else 100
        
        # Most problematic fields
        field_errors = {}
        for error in self.error_records:
            if error.field_name:
                field_errors[error.field_name] = field_errors.get(error.field_name, 0) + 1
        
        return {
            'summary': {
                'total_errors': total_errors,
                'resolved_errors': resolved_errors,
                'quarantined_records': len(self.quarantine_queue),
                'recovery_success_rate': recovery_rate,
                'circuit_breaker_active': self.circuit_breaker_state
            },
            'error_distribution': {
                'by_type': error_by_type,
                'by_severity': error_by_severity
            },
            'problematic_fields': dict(sorted(field_errors.items(), 
                                            key=lambda x: x[1], reverse=True)[:10]),
            'recommendations': self._generate_recommendations()
        }
    
    def _generate_recommendations(self) -> List[str]:
        """Generate actionable recommendations based on error patterns"""
        
        recommendations = []
        
        # Check error patterns
        validation_errors = len([e for e in self.error_records 
                               if e.error_type == ErrorType.DATA_VALIDATION])
        
        if validation_errors > 10:
            recommendations.append("High number of validation errors detected - "
                                 "consider improving data quality at source")
        
        processing_errors = len([e for e in self.error_records 
                               if e.error_type == ErrorType.PROCESSING_ERROR])
        
        if processing_errors > 5:
            recommendations.append("Multiple processing errors detected - "
                                 "review transformation logic and error handling")
        
        if self.circuit_breaker_state:
            recommendations.append("System is in failure state - "
                                 "immediate intervention required")
        
        if len(self.quarantine_queue) > 20:
            recommendations.append("High number of quarantined records - "
                                 "consider batch review and pattern analysis")
        
        return recommendations

# Recovery strategy examples
def recover_date_format_error(error_record: ErrorRecord, original_data: Any) -> Any:
    """Recovery strategy for date format errors"""
    
    # Try alternative date formats
    from datetime import datetime
    
    date_patterns = [
        "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d",
        "%m-%d-%Y", "%d-%m-%Y", "%Y-%m-%d",
        "%b %d %Y", "%d %b %Y", "%B %d %Y"
    ]
    
    date_str = str(original_data).strip()
    
    for pattern in date_patterns:
        try:
            parsed_date = datetime.strptime(date_str, pattern)
            return parsed_date.strftime("%Y-%m-%d")
        except ValueError:
            continue
    
    # If all patterns fail, use current date as fallback
    logger.warning(f"Could not parse date '{date_str}', using current date")
    return datetime.now().strftime("%Y-%m-%d")

def recover_currency_format_error(error_record: ErrorRecord, original_data: Any) -> Any:
    """Recovery strategy for currency format errors"""
    
    import re
    
    # Try to extract numeric value from currency string
    currency_str = str(original_data).strip()
    
    # Remove currency symbols and extract numbers
    cleaned = re.sub(r'[^\d.-]', '', currency_str.replace(',', ''))
    
    try:
        amount = float(cleaned)
        # Convert to cents
        return int(amount * 100)
    except ValueError:
        # Default to zero if cannot recover
        logger.warning(f"Could not recover currency value '{currency_str}', defaulting to 0")
        return 0

if __name__ == "__main__":
    # Example usage
    error_handler = EnterpriseErrorHandler()
    
    # Register recovery strategies
    error_handler.register_recovery_strategy(ErrorType.DATA_VALIDATION, recover_date_format_error)
    error_handler.register_recovery_strategy(ErrorType.PROCESSING_ERROR, recover_currency_format_error)
    
    print("Enterprise Error Handler initialized with recovery strategies")