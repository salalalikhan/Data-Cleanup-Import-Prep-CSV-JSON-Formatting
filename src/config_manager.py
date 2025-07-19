#!/usr/bin/env python3
"""
Enterprise Configuration Management System
Provides centralized configuration with environment support and validation
"""

import json
import os
import logging
from typing import Dict, Any, Optional, List
from pathlib import Path
from dataclasses import dataclass
import copy

logger = logging.getLogger(__name__)

@dataclass
class ConfigValidationResult:
    is_valid: bool
    errors: List[str]
    warnings: List[str]

class EnterpriseConfigManager:
    """
    Enterprise-grade configuration management with environment support,
    validation, and hot-reloading capabilities
    """
    
    def __init__(self, config_dir: str = "config", environment: str = "production"):
        self.config_dir = Path(config_dir)
        self.environment = environment
        self.config_cache = {}
        self.config_schemas = {}
        self.watchers = {}
        
        # Ensure config directory exists
        self.config_dir.mkdir(exist_ok=True)
        
        # Load base configuration
        self._load_base_config()
        
        # Apply environment-specific overrides
        self._apply_environment_config()
        
        logger.info(f"Configuration manager initialized for environment: {environment}")
    
    def _load_base_config(self) -> None:
        """Load base configuration from processing_config.json"""
        base_config_file = self.config_dir / "processing_config.json"
        
        if base_config_file.exists():
            try:
                with open(base_config_file, 'r') as f:
                    self.config_cache['base'] = json.load(f)
                logger.info("Base configuration loaded successfully")
            except Exception as e:
                logger.error(f"Failed to load base configuration: {e}")
                self.config_cache['base'] = self._get_default_config()
        else:
            logger.warning("Base configuration not found, using defaults")
            self.config_cache['base'] = self._get_default_config()
            self._save_default_config()
    
    def _apply_environment_config(self) -> None:
        """Apply environment-specific configuration overrides"""
        env_config_file = self.config_dir / f"{self.environment}_config.json"
        
        if env_config_file.exists():
            try:
                with open(env_config_file, 'r') as f:
                    env_config = json.load(f)
                
                # Deep merge environment config with base config
                self.config_cache['active'] = self._deep_merge(
                    copy.deepcopy(self.config_cache['base']), 
                    env_config
                )
                logger.info(f"Environment configuration applied: {self.environment}")
                
            except Exception as e:
                logger.error(f"Failed to load environment config: {e}")
                self.config_cache['active'] = copy.deepcopy(self.config_cache['base'])
        else:
            self.config_cache['active'] = copy.deepcopy(self.config_cache['base'])
            logger.info(f"No environment-specific config found for {self.environment}")
    
    def _deep_merge(self, base: Dict, override: Dict) -> Dict:
        """Deep merge two dictionaries, with override taking precedence"""
        result = copy.deepcopy(base)
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        
        return result
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Return default configuration if no config file exists"""
        return {
            "data_processing": {
                "batch_size": 1000,
                "max_errors_per_batch": 50,
                "enable_parallel_processing": False,
                "max_workers": 1
            },
            "validation": {
                "strict_mode": False,
                "email_validation": {"enabled": True},
                "phone_validation": {"enabled": True}
            },
            "error_handling": {
                "max_retries": 3,
                "enable_circuit_breaker": True
            },
            "output_formats": {
                "csv": {"enabled": True},
                "json": {"enabled": True}
            }
        }
    
    def _save_default_config(self) -> None:
        """Save default configuration to file"""
        try:
            config_file = self.config_dir / "processing_config.json"
            with open(config_file, 'w') as f:
                json.dump(self.config_cache['base'], f, indent=2)
            logger.info("Default configuration saved")
        except Exception as e:
            logger.error(f"Failed to save default configuration: {e}")
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation (e.g., 'data_processing.batch_size')
        """
        try:
            keys = key_path.split('.')
            value = self.config_cache.get('active', {})
            
            for key in keys:
                if isinstance(value, dict) and key in value:
                    value = value[key]
                else:
                    return default
            
            return value
            
        except Exception as e:
            logger.error(f"Error getting config value for '{key_path}': {e}")
            return default
    
    def set(self, key_path: str, value: Any, save_to_file: bool = False) -> bool:
        """
        Set configuration value using dot notation
        """
        try:
            keys = key_path.split('.')
            config = self.config_cache.get('active', {})
            
            # Navigate to parent of target key
            current = config
            for key in keys[:-1]:
                if key not in current:
                    current[key] = {}
                current = current[key]
            
            # Set the value
            current[keys[-1]] = value
            
            if save_to_file:
                self._save_config_to_file()
            
            logger.info(f"Configuration updated: {key_path} = {value}")
            return True
            
        except Exception as e:
            logger.error(f"Error setting config value for '{key_path}': {e}")
            return False
    
    def _save_config_to_file(self) -> None:
        """Save current configuration to file"""
        try:
            config_file = self.config_dir / f"{self.environment}_config.json"
            with open(config_file, 'w') as f:
                json.dump(self.config_cache['active'], f, indent=2)
            logger.info(f"Configuration saved to {config_file}")
        except Exception as e:
            logger.error(f"Failed to save configuration: {e}")
    
    def validate_config(self) -> ConfigValidationResult:
        """Validate current configuration against business rules"""
        errors = []
        warnings = []
        
        # Validate data processing settings
        batch_size = self.get('data_processing.batch_size', 1000)
        if batch_size <= 0:
            errors.append("Batch size must be positive")
        elif batch_size > 10000:
            warnings.append("Large batch size may cause memory issues")
        
        # Validate error handling settings
        max_retries = self.get('error_handling.max_retries', 3)
        if max_retries < 0:
            errors.append("Max retries cannot be negative")
        elif max_retries > 10:
            warnings.append("High retry count may cause performance issues")
        
        # Validate output formats
        csv_enabled = self.get('output_formats.csv.enabled', True)
        json_enabled = self.get('output_formats.json.enabled', True)
        
        if not csv_enabled and not json_enabled:
            errors.append("At least one output format must be enabled")
        
        # Validate security settings
        if self.get('data_security.enable_pii_detection', False):
            protection_level = self.get('data_security.protection_level', 'medium')
            if protection_level not in ['low', 'medium', 'high']:
                errors.append("Invalid protection level - must be 'low', 'medium', or 'high'")
        
        # Performance validation
        memory_limit = self.get('performance.memory_limit_mb', 2048)
        if memory_limit < 512:
            warnings.append("Low memory limit may cause processing failures")
        
        max_workers = self.get('data_processing.max_workers', 1)
        if max_workers > os.cpu_count():
            warnings.append(f"Max workers ({max_workers}) exceeds CPU count ({os.cpu_count()})")
        
        return ConfigValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )
    
    def get_environment_info(self) -> Dict[str, Any]:
        """Get information about current environment and configuration"""
        return {
            'environment': self.environment,
            'config_directory': str(self.config_dir),
            'config_files_loaded': list(self.config_cache.keys()),
            'validation_status': self.validate_config(),
            'key_settings': {
                'batch_size': self.get('data_processing.batch_size'),
                'parallel_processing': self.get('data_processing.enable_parallel_processing'),
                'max_workers': self.get('data_processing.max_workers'),
                'pii_detection': self.get('data_security.enable_pii_detection'),
                'error_threshold': self.get('data_processing.error_threshold_percentage'),
                'output_formats': {
                    'csv': self.get('output_formats.csv.enabled'),
                    'json': self.get('output_formats.json.enabled'),
                    'excel': self.get('output_formats.excel.enabled')
                }
            }
        }
    
    def create_environment_config(self, environment: str, 
                                 overrides: Dict[str, Any]) -> bool:
        """Create configuration for specific environment"""
        try:
            env_config_file = self.config_dir / f"{environment}_config.json"
            
            with open(env_config_file, 'w') as f:
                json.dump(overrides, f, indent=2)
            
            logger.info(f"Environment configuration created: {environment}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create environment config for {environment}: {e}")
            return False
    
    def list_available_environments(self) -> List[str]:
        """List all available environment configurations"""
        environments = ['base']
        
        for config_file in self.config_dir.glob("*_config.json"):
            env_name = config_file.stem.replace('_config', '')
            if env_name != 'processing':  # Skip base config
                environments.append(env_name)
        
        return environments
    
    def reload_config(self) -> bool:
        """Reload configuration from files"""
        try:
            self._load_base_config()
            self._apply_environment_config()
            logger.info("Configuration reloaded successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to reload configuration: {e}")
            return False
    
    def export_config(self, output_file: str, include_environment: bool = True) -> bool:
        """Export current configuration to file"""
        try:
            config_to_export = self.config_cache['active'] if include_environment else self.config_cache['base']
            
            with open(output_file, 'w') as f:
                json.dump(config_to_export, f, indent=2)
            
            logger.info(f"Configuration exported to {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to export configuration: {e}")
            return False

# Global configuration instance
_config_manager = None

def get_config_manager(environment: str = None) -> EnterpriseConfigManager:
    """Get global configuration manager instance"""
    global _config_manager
    
    if _config_manager is None or (environment and _config_manager.environment != environment):
        env = environment or os.getenv('DATA_PROCESSING_ENV', 'production')
        _config_manager = EnterpriseConfigManager(environment=env)
    
    return _config_manager

def get_config(key_path: str, default: Any = None) -> Any:
    """Convenience function to get configuration value"""
    return get_config_manager().get(key_path, default)

def set_config(key_path: str, value: Any, save_to_file: bool = False) -> bool:
    """Convenience function to set configuration value"""
    return get_config_manager().set(key_path, value, save_to_file)

if __name__ == "__main__":
    # Example usage and testing
    config = EnterpriseConfigManager()
    
    # Test configuration access
    batch_size = config.get('data_processing.batch_size')
    print(f"Batch size: {batch_size}")
    
    # Test validation
    validation = config.validate_config()
    print(f"Configuration valid: {validation.is_valid}")
    
    if validation.warnings:
        print(f"Warnings: {validation.warnings}")
    
    # Test environment info
    env_info = config.get_environment_info()
    print(f"Environment: {env_info['environment']}")
    
    print("Configuration management system initialized")