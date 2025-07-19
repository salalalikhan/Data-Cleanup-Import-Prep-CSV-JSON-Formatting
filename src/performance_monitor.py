#!/usr/bin/env python3
"""
Enterprise Performance Monitoring and Batch Processing System
Provides scalable processing with comprehensive performance metrics
"""

import time
import psutil
import threading
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import logging
import json
import csv
from pathlib import Path
import gc
import tracemalloc

logger = logging.getLogger(__name__)

@dataclass
class PerformanceMetrics:
    timestamp: str
    processing_stage: str
    records_processed: int
    processing_time_seconds: float
    records_per_second: float
    memory_usage_mb: float
    cpu_usage_percent: float
    errors_encountered: int
    success_rate: float

@dataclass
class BatchStatus:
    batch_id: str
    start_time: str
    end_time: Optional[str]
    records_count: int
    success_count: int
    error_count: int
    status: str  # 'processing', 'completed', 'failed'
    processing_time: Optional[float]

class EnterprisePerformanceMonitor:
    """
    Enterprise-grade performance monitoring with real-time metrics,
    resource usage tracking, and scalable batch processing
    """
    
    def __init__(self, enable_memory_tracking: bool = True):
        self.metrics_history = []
        self.batch_history = []
        self.current_batch = None
        self.enable_memory_tracking = enable_memory_tracking
        self.monitoring_active = False
        self.monitoring_thread = None
        
        # Performance thresholds
        self.thresholds = {
            'memory_warning_mb': 1024,  # 1GB
            'memory_critical_mb': 2048,  # 2GB
            'cpu_warning_percent': 80,
            'min_records_per_second': 100,
            'max_processing_time_minutes': 30
        }
        
        # Resource monitoring
        self.resource_stats = {
            'peak_memory_mb': 0,
            'peak_cpu_percent': 0,
            'total_records_processed': 0,
            'total_processing_time': 0
        }
        
        if enable_memory_tracking:
            tracemalloc.start()
        
        # Create performance logs directory
        Path("outputs/performance_logs").mkdir(exist_ok=True)
        
        logger.info("Performance monitor initialized")
    
    def start_monitoring(self, interval_seconds: int = 5) -> None:
        """Start real-time performance monitoring"""
        if self.monitoring_active:
            return
        
        self.monitoring_active = True
        self.monitoring_thread = threading.Thread(
            target=self._monitor_resources,
            args=(interval_seconds,),
            daemon=True
        )
        self.monitoring_thread.start()
        logger.info(f"Performance monitoring started (interval: {interval_seconds}s)")
    
    def stop_monitoring(self) -> None:
        """Stop performance monitoring"""
        self.monitoring_active = False
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=5)
        logger.info("Performance monitoring stopped")
    
    def _monitor_resources(self, interval: int) -> None:
        """Background thread for monitoring system resources"""
        while self.monitoring_active:
            try:
                # Get system metrics
                process = psutil.Process()
                memory_mb = process.memory_info().rss / 1024 / 1024
                cpu_percent = process.cpu_percent()
                
                # Update peak values
                self.resource_stats['peak_memory_mb'] = max(
                    self.resource_stats['peak_memory_mb'], memory_mb
                )
                self.resource_stats['peak_cpu_percent'] = max(
                    self.resource_stats['peak_cpu_percent'], cpu_percent
                )
                
                # Check thresholds and log warnings
                if memory_mb > self.thresholds['memory_critical_mb']:
                    logger.critical(f"Memory usage critical: {memory_mb:.1f}MB")
                elif memory_mb > self.thresholds['memory_warning_mb']:
                    logger.warning(f"Memory usage high: {memory_mb:.1f}MB")
                
                if cpu_percent > self.thresholds['cpu_warning_percent']:
                    logger.warning(f"CPU usage high: {cpu_percent:.1f}%")
                
                time.sleep(interval)
                
            except Exception as e:
                logger.error(f"Error in resource monitoring: {e}")
                time.sleep(interval)
    
    def start_batch(self, batch_id: str, records_count: int) -> None:
        """Start tracking a new batch"""
        self.current_batch = BatchStatus(
            batch_id=batch_id,
            start_time=datetime.now().isoformat(),
            end_time=None,
            records_count=records_count,
            success_count=0,
            error_count=0,
            status='processing',
            processing_time=None
        )
        
        logger.info(f"Started batch {batch_id} with {records_count} records")
    
    def complete_batch(self, success_count: int, error_count: int) -> None:
        """Complete current batch tracking"""
        if not self.current_batch:
            logger.warning("No active batch to complete")
            return
        
        end_time = datetime.now()
        start_time = datetime.fromisoformat(self.current_batch.start_time)
        processing_time = (end_time - start_time).total_seconds()
        
        self.current_batch.end_time = end_time.isoformat()
        self.current_batch.success_count = success_count
        self.current_batch.error_count = error_count
        self.current_batch.processing_time = processing_time
        self.current_batch.status = 'completed' if error_count == 0 else 'partial_failure'
        
        # Update global stats
        self.resource_stats['total_records_processed'] += success_count
        self.resource_stats['total_processing_time'] += processing_time
        
        # Save batch to history
        self.batch_history.append(self.current_batch)
        
        # Log performance metrics
        records_per_second = success_count / processing_time if processing_time > 0 else 0
        success_rate = (success_count / self.current_batch.records_count) * 100
        
        logger.info(f"Completed batch {self.current_batch.batch_id}: "
                   f"{success_count}/{self.current_batch.records_count} records "
                   f"({success_rate:.1f}% success) in {processing_time:.2f}s "
                   f"({records_per_second:.1f} records/sec)")
        
        self.current_batch = None
    
    def record_performance_metric(self, stage: str, records_processed: int,
                                processing_time: float, errors_count: int = 0) -> None:
        """Record performance metrics for a processing stage"""
        
        # Get current resource usage
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024
        cpu_percent = process.cpu_percent()
        
        # Calculate performance metrics
        records_per_second = records_processed / processing_time if processing_time > 0 else 0
        success_rate = ((records_processed - errors_count) / records_processed * 100) if records_processed > 0 else 100
        
        metric = PerformanceMetrics(
            timestamp=datetime.now().isoformat(),
            processing_stage=stage,
            records_processed=records_processed,
            processing_time_seconds=processing_time,
            records_per_second=records_per_second,
            memory_usage_mb=memory_mb,
            cpu_usage_percent=cpu_percent,
            errors_encountered=errors_count,
            success_rate=success_rate
        )
        
        self.metrics_history.append(metric)
        
        # Performance warnings
        if records_per_second < self.thresholds['min_records_per_second']:
            logger.warning(f"Low processing performance: {records_per_second:.1f} records/sec")
        
        if processing_time > self.thresholds['max_processing_time_minutes'] * 60:
            logger.warning(f"Long processing time: {processing_time:.1f} seconds")
    
    def process_in_batches(self, data: List[Any], batch_size: int,
                          processor_func: Callable, max_workers: int = 4,
                          error_threshold: float = 0.05) -> Dict[str, Any]:
        """
        Process large datasets in batches with parallel processing and error handling
        """
        
        total_records = len(data)
        total_success = 0
        total_errors = 0
        batch_results = []
        
        # Calculate number of batches
        num_batches = (total_records + batch_size - 1) // batch_size
        
        logger.info(f"Processing {total_records} records in {num_batches} batches "
                   f"(batch size: {batch_size}, workers: {max_workers})")
        
        start_time = time.time()
        
        # Process batches
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            
            for batch_idx in range(num_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, total_records)
                batch_data = data[start_idx:end_idx]
                
                batch_id = f"batch_{batch_idx + 1:03d}"
                
                # Submit batch for processing
                future = executor.submit(
                    self._process_single_batch,
                    batch_id, batch_data, processor_func
                )
                futures.append((batch_id, future))
            
            # Collect results
            for batch_id, future in futures:
                try:
                    batch_result = future.result(timeout=300)  # 5 minute timeout
                    batch_results.append(batch_result)
                    
                    total_success += batch_result['success_count']
                    total_errors += batch_result['error_count']
                    
                    # Check error threshold
                    current_error_rate = total_errors / (total_success + total_errors)
                    if current_error_rate > error_threshold:
                        logger.error(f"Error rate exceeded threshold: {current_error_rate:.2%}")
                        # Cancel remaining batches
                        for remaining_batch_id, remaining_future in futures:
                            if not remaining_future.done():
                                remaining_future.cancel()
                        break
                    
                except Exception as e:
                    logger.error(f"Batch {batch_id} failed: {e}")
                    total_errors += len(data[batch_idx * batch_size:(batch_idx + 1) * batch_size])
        
        total_time = time.time() - start_time
        
        # Record overall performance
        self.record_performance_metric(
            stage="batch_processing",
            records_processed=total_success + total_errors,
            processing_time=total_time,
            errors_count=total_errors
        )
        
        return {
            'total_records': total_records,
            'success_count': total_success,
            'error_count': total_errors,
            'processing_time': total_time,
            'success_rate': (total_success / total_records * 100) if total_records > 0 else 0,
            'records_per_second': total_success / total_time if total_time > 0 else 0,
            'batch_results': batch_results
        }
    
    def _process_single_batch(self, batch_id: str, batch_data: List[Any],
                             processor_func: Callable) -> Dict[str, Any]:
        """Process a single batch and return results"""
        
        self.start_batch(batch_id, len(batch_data))
        batch_start_time = time.time()
        
        success_count = 0
        error_count = 0
        results = []
        
        try:
            for record in batch_data:
                try:
                    result = processor_func(record)
                    results.append(result)
                    success_count += 1
                except Exception as e:
                    logger.error(f"Error processing record in {batch_id}: {e}")
                    error_count += 1
            
            batch_time = time.time() - batch_start_time
            self.complete_batch(success_count, error_count)
            
            return {
                'batch_id': batch_id,
                'success_count': success_count,
                'error_count': error_count,
                'processing_time': batch_time,
                'results': results
            }
            
        except Exception as e:
            logger.error(f"Critical error in batch {batch_id}: {e}")
            if self.current_batch:
                self.current_batch.status = 'failed'
            raise
    
    def optimize_performance(self) -> Dict[str, Any]:
        """Analyze performance and provide optimization recommendations"""
        
        if not self.metrics_history:
            return {"message": "No performance data available"}
        
        # Calculate averages
        avg_records_per_second = sum(m.records_per_second for m in self.metrics_history) / len(self.metrics_history)
        avg_memory_mb = sum(m.memory_usage_mb for m in self.metrics_history) / len(self.metrics_history)
        avg_cpu_percent = sum(m.cpu_usage_percent for m in self.metrics_history) / len(self.metrics_history)
        
        recommendations = []
        
        # Performance recommendations
        if avg_records_per_second < self.thresholds['min_records_per_second']:
            recommendations.append("Consider increasing batch size or enabling parallel processing")
        
        if avg_memory_mb > self.thresholds['memory_warning_mb']:
            recommendations.append("High memory usage detected - consider processing smaller batches")
        
        if avg_cpu_percent < 30:
            recommendations.append("Low CPU utilization - consider increasing worker threads")
        elif avg_cpu_percent > 90:
            recommendations.append("High CPU usage - consider reducing worker threads")
        
        # Calculate optimal batch size
        if len(self.batch_history) > 1:
            best_batch = max(self.batch_history, 
                           key=lambda b: (b.success_count / b.processing_time) if b.processing_time else 0)
            optimal_batch_size = best_batch.records_count
            recommendations.append(f"Optimal batch size appears to be around {optimal_batch_size} records")
        
        return {
            'current_performance': {
                'avg_records_per_second': avg_records_per_second,
                'avg_memory_usage_mb': avg_memory_mb,
                'avg_cpu_usage_percent': avg_cpu_percent
            },
            'peak_usage': {
                'peak_memory_mb': self.resource_stats['peak_memory_mb'],
                'peak_cpu_percent': self.resource_stats['peak_cpu_percent']
            },
            'recommendations': recommendations,
            'total_stats': self.resource_stats
        }
    
    def generate_performance_report(self) -> Dict[str, Any]:
        """Generate comprehensive performance analysis report"""
        
        # Calculate statistics
        if self.metrics_history:
            processing_times = [m.processing_time_seconds for m in self.metrics_history]
            records_per_second_values = [m.records_per_second for m in self.metrics_history]
            
            stats = {
                'total_batches_processed': len(self.batch_history),
                'total_records_processed': self.resource_stats['total_records_processed'],
                'total_processing_time': self.resource_stats['total_processing_time'],
                'average_processing_time': sum(processing_times) / len(processing_times),
                'average_records_per_second': sum(records_per_second_values) / len(records_per_second_values),
                'peak_performance': max(records_per_second_values) if records_per_second_values else 0,
                'peak_memory_usage_mb': self.resource_stats['peak_memory_mb'],
                'peak_cpu_usage_percent': self.resource_stats['peak_cpu_percent']
            }
        else:
            stats = {'message': 'No performance data available'}
        
        # Batch performance summary
        batch_summary = []
        for batch in self.batch_history[-10:]:  # Last 10 batches
            batch_summary.append({
                'batch_id': batch.batch_id,
                'records_count': batch.records_count,
                'success_rate': (batch.success_count / batch.records_count * 100) if batch.records_count > 0 else 0,
                'processing_time': batch.processing_time,
                'records_per_second': (batch.success_count / batch.processing_time) if batch.processing_time else 0
            })
        
        return {
            'performance_summary': stats,
            'recent_batches': batch_summary,
            'optimization_analysis': self.optimize_performance(),
            'monitoring_status': {
                'monitoring_active': self.monitoring_active,
                'memory_tracking_enabled': self.enable_memory_tracking,
                'metrics_collected': len(self.metrics_history)
            }
        }
    
    def save_performance_logs(self) -> None:
        """Save performance metrics to files"""
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save metrics history
        metrics_file = f"outputs/performance_logs/metrics_{timestamp}.jsonl"
        with open(metrics_file, 'w') as f:
            for metric in self.metrics_history:
                f.write(json.dumps(asdict(metric)) + '\n')
        
        # Save batch history
        batch_file = f"outputs/performance_logs/batches_{timestamp}.jsonl"
        with open(batch_file, 'w') as f:
            for batch in self.batch_history:
                f.write(json.dumps(asdict(batch)) + '\n')
        
        # Save performance report
        report_file = f"outputs/performance_logs/performance_report_{timestamp}.json"
        with open(report_file, 'w') as f:
            json.dump(self.generate_performance_report(), f, indent=2)
        
        logger.info(f"Performance logs saved to outputs/performance_logs/")

if __name__ == "__main__":
    # Example usage
    monitor = EnterprisePerformanceMonitor()
    monitor.start_monitoring()
    
    # Simulate processing
    def sample_processor(record):
        time.sleep(0.001)  # Simulate processing time
        return f"processed_{record}"
    
    # Test batch processing
    test_data = list(range(100))
    results = monitor.process_in_batches(
        data=test_data,
        batch_size=25,
        processor_func=sample_processor,
        max_workers=2
    )
    
    print(f"Processed {results['success_count']} records in {results['processing_time']:.2f}s")
    print(f"Performance: {results['records_per_second']:.1f} records/sec")
    
    monitor.stop_monitoring()
    print("Performance monitoring system initialized")