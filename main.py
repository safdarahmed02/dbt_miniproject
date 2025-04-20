#!/usr/bin/env python3
"""
Main entry point for the Real-Time vs. Batch Analytics Pipeline.
This script provides commands to start the producer, streaming job, and comparison analysis.
"""
import os
import sys
import time
import argparse
import logging
import subprocess
import signal
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define script paths
PRODUCER_SCRIPT = os.path.join("producer", "tweet_producer.py")
STREAMING_SCRIPT = os.path.join("streaming", "spark_streaming.py")
BATCH_SCRIPT = os.path.join("batch", "batch_processor.py")
BATCH_SCHEDULER_SCRIPT = os.path.join("batch", "scheduler.py")
COMPARISON_SCRIPT = os.path.join("comparison", "comparison_analyzer.py")

def run_process(script_path, process_name):
    """Run a Python script as a subprocess."""
    logger.info(f"Starting {process_name}...")
    
    try:
        process = subprocess.Popen(
            ["python3", script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        logger.info(f"{process_name} started with PID {process.pid}")
        return process
    except Exception as e:
        logger.error(f"Error starting {process_name}: {e}")
        return None

def start_producer():
    """Start the tweet producer."""
    return run_process(PRODUCER_SCRIPT, "Tweet Producer")

def start_streaming_job():
    """Start the Spark Streaming job."""
    return run_process(STREAMING_SCRIPT, "Spark Streaming Job")

def start_batch_scheduler():
    """Start the batch scheduler."""
    return run_process(BATCH_SCHEDULER_SCRIPT, "Batch Scheduler")

def run_comparison_analysis():
    """Run the comparison analysis."""
    logger.info("Running comparison analysis...")
    process = run_process(COMPARISON_SCRIPT, "Comparison Analysis")
    if process:
        stdout, stderr = process.communicate()
        if process.returncode == 0:
            logger.info("Comparison analysis completed successfully")
        else:
            logger.error(f"Comparison analysis failed: {stderr.decode()}")

def stop_processes(processes):
    """Stop all running processes."""
    for name, process in processes.items():
        if process and process.poll() is None:
            logger.info(f"Stopping {name}...")
            process.terminate()
            try:
                process.wait(timeout=5)
                logger.info(f"{name} stopped gracefully")
            except subprocess.TimeoutExpired:
                logger.warning(f"{name} did not terminate gracefully, killing...")
                process.kill()

def start_all_services():
    """Start all services."""
    processes = {}
    
    try:
        # Start producer
        producer_process = start_producer()
        if producer_process:
            processes["Producer"] = producer_process
        
        # Wait a bit for producer to initialize
        time.sleep(5)
        
        # Start streaming job
        streaming_process = start_streaming_job()
        if streaming_process:
            processes["Streaming"] = streaming_process
        
        # Wait a bit for streaming job to initialize
        time.sleep(10)
        
        # Start batch scheduler
        batch_scheduler_process = start_batch_scheduler()
        if batch_scheduler_process:
            processes["Batch Scheduler"] = batch_scheduler_process
        
        # Keep running until interrupted
        logger.info("All services started. Press Ctrl+C to stop.")
        while True:
            time.sleep(1)
    
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, stopping all services...")
    finally:
        stop_processes(processes)
        logger.info("All services stopped")

def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Real-Time vs. Batch Analytics Pipeline")
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Start all services
    subparsers.add_parser("start", help="Start all services (producer, streaming, batch scheduler)")
    
    # Start individual services
    subparsers.add_parser("producer", help="Start the tweet producer")
    subparsers.add_parser("streaming", help="Start the Spark Streaming job")
    subparsers.add_parser("batch", help="Run the batch job once")
    subparsers.add_parser("scheduler", help="Start the batch scheduler")
    
    # Run comparison
    subparsers.add_parser("compare", help="Run comparison analysis")
    
    return parser.parse_args()

def main():
    """Main entry point."""
    args = parse_args()
    
    if args.command == "start":
        start_all_services()
    elif args.command == "producer":
        process = start_producer()
        if process:
            try:
                process.wait()
            except KeyboardInterrupt:
                process.terminate()
    elif args.command == "streaming":
        process = start_streaming_job()
        if process:
            try:
                process.wait()
            except KeyboardInterrupt:
                process.terminate()
    elif args.command == "batch":
        run_process(BATCH_SCRIPT, "Batch Job")
    elif args.command == "scheduler":
        process = start_batch_scheduler()
        if process:
            try:
                process.wait()
            except KeyboardInterrupt:
                process.terminate()
    elif args.command == "compare":
        run_comparison_analysis()
    else:
        # No command or invalid command
        logger.error("Please specify a valid command")
        sys.exit(1)

if __name__ == "__main__":
    main() 