#!/usr/bin/env python3
"""
Demo Script - Runs the entire Real-Time vs Batch Analytics Pipeline
and displays the results automatically.

This script:
1. Checks/starts Docker containers
2. Runs the producer for a set duration
3. Runs the streaming job to process data
4. Runs the batch job for comparison
5. Runs the comparison analysis
6. Displays the results
"""
import os
import sys
import time
import subprocess
import logging
import signal
import webbrowser
from datetime import datetime, timedelta
import json
import pandas as pd
import matplotlib.pyplot as plt

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Script paths
PRODUCER_SCRIPT = os.path.join("producer", "tweet_producer.py")
STREAMING_SCRIPT = os.path.join("streaming", "spark_streaming.py")
BATCH_SCRIPT = os.path.join("batch", "batch_processor.py")
COMPARISON_SCRIPT = os.path.join("comparison", "comparison_analyzer.py")

# Demo configuration
DEMO_DURATION = 300  # 5 minutes (in seconds)
DATA_GENERATION_TIME = 180  # 3 minutes (in seconds)
PROCESSING_WAIT_TIME = 60  # 1 minute for processing to catch up

# Output directory for results
OUTPUT_DIR = os.path.join("comparison", "output")

def check_docker_running():
    """Check if Docker containers are running, start them if not."""
    logger.info("Checking Docker container status...")
    
    try:
        # Check if containers are running
        process = subprocess.run(
            ["docker-compose", "ps", "--services", "--filter", "status=running"],
            stdout=subprocess.PIPE,
            text=True,
            check=True
        )
        
        running_services = process.stdout.strip().split('\n')
        expected_services = ["zookeeper", "kafka", "mysql", "spark-master", "spark-worker"]
        
        # Check if any expected services are not running
        missing_services = [svc for svc in expected_services if svc not in running_services and svc]
        
        if missing_services:
            logger.info(f"Starting missing Docker services: {', '.join(missing_services)}")
            subprocess.run(["docker-compose", "up", "-d"], check=True)
            
            # Wait for services to start
            logger.info("Waiting for services to start (30s)...")
            time.sleep(30)
        else:
            logger.info("All Docker containers are running")
            
        return True
    
    except subprocess.CalledProcessError as e:
        logger.error(f"Error checking/starting Docker containers: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False

def run_process(script_path, process_name, duration=None):
    """Run a Python script as a subprocess with optional timeout."""
    logger.info(f"Starting {process_name}...")
    
    try:
        process = subprocess.Popen(
            ["python3", script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        logger.info(f"{process_name} started with PID {process.pid}")
        
        if duration:
            # Run for the specified duration then terminate
            try:
                logger.info(f"Running {process_name} for {duration} seconds...")
                time.sleep(duration)
                process.terminate()
                process.wait(timeout=5)
                logger.info(f"{process_name} terminated after {duration}s")
            except subprocess.TimeoutExpired:
                logger.warning(f"{process_name} did not terminate gracefully, killing...")
                process.kill()
        else:
            # For non-duration processes, wait for completion
            stdout, stderr = process.communicate()
            if process.returncode == 0:
                logger.info(f"{process_name} completed successfully")
            else:
                logger.error(f"{process_name} failed: {stderr.decode()}")
        
        return process.returncode == 0
        
    except Exception as e:
        logger.error(f"Error running {process_name}: {e}")
        return False

def find_latest_file(directory, pattern):
    """Find the most recent file in a directory matching a pattern."""
    files = [os.path.join(directory, f) for f in os.listdir(directory) if pattern in f]
    if not files:
        return None
    
    return max(files, key=os.path.getmtime)

def display_results():
    """Display the results of the comparison analysis."""
    logger.info("Collecting and displaying results...")
    
    # Look for the latest summary and chart files
    summary_file = find_latest_file(OUTPUT_DIR, "summary_")
    pct_diff_chart = find_latest_file(OUTPUT_DIR, "percent_diff_dist_")
    top_hashtags_chart = find_latest_file(OUTPUT_DIR, "top_hashtags_comparison_")
    latency_chart = find_latest_file(OUTPUT_DIR, "latency_dist_")
    
    if not summary_file:
        logger.error("No results found! Make sure the comparison analysis ran successfully.")
        return False
    
    # Display summary results
    try:
        with open(summary_file, 'r') as f:
            summary = json.load(f)
        
        logger.info("\n" + "="*60)
        logger.info("REAL-TIME VS BATCH ANALYTICS - RESULTS SUMMARY")
        logger.info("="*60)
        
        logger.info(f"Total Windows Analyzed: {summary['windows_count']}")
        
        logger.info("\nACCURACY METRICS:")
        logger.info(f"Mean Percent Difference: {summary['mean_pct_diff']:.2f}%")
        logger.info(f"Median Percent Difference: {summary['median_pct_diff']:.2f}%")
        logger.info(f"Maximum Percent Difference: {summary['max_pct_diff']:.2f}%")
        logger.info(f"Windows within 1% Error Threshold: {summary['windows_within_threshold']:.2f}%")
        
        logger.info("\nLATENCY METRICS:")
        logger.info(f"Mean Latency: {summary['mean_latency']:.2f}s")
        logger.info(f"Median Latency: {summary['median_latency']:.2f}s")
        logger.info(f"Maximum Latency: {summary['max_latency']:.2f}s")
        logger.info(f"Windows within 5s Latency Target: {summary['latency_within_target']:.2f}%")
        
        logger.info("\nPERFORMANCE ASSESSMENT:")
        
        # Assess if target metrics were met
        accuracy_passed = summary['windows_within_threshold'] >= 95  # 95% of windows within 1% error
        latency_passed = summary['latency_within_target'] >= 95  # 95% of windows under 5s latency
        
        logger.info(f"Accuracy Target Met: {'✅ YES' if accuracy_passed else '❌ NO'}")
        logger.info(f"Latency Target Met: {'✅ YES' if latency_passed else '❌ NO'}")
        
        logger.info("="*60)
        
        # Try to open the chart images
        if pct_diff_chart and os.path.exists(pct_diff_chart):
            logger.info(f"Percent Difference Chart: {pct_diff_chart}")
            try:
                webbrowser.open(f"file://{os.path.abspath(pct_diff_chart)}")
            except:
                pass
        
        if top_hashtags_chart and os.path.exists(top_hashtags_chart):
            logger.info(f"Top Hashtags Comparison Chart: {top_hashtags_chart}")
            try:
                webbrowser.open(f"file://{os.path.abspath(top_hashtags_chart)}")
            except:
                pass
        
        if latency_chart and os.path.exists(latency_chart):
            logger.info(f"Latency Distribution Chart: {latency_chart}")
            try:
                webbrowser.open(f"file://{os.path.abspath(latency_chart)}")
            except:
                pass
        
        return True
        
    except Exception as e:
        logger.error(f"Error displaying results: {e}")
        return False

def run_demo():
    """Run the complete demonstration end-to-end."""
    logger.info(f"Starting Real-Time vs Batch Analytics Pipeline Demo")
    start_time = time.time()
    
    # Step 1: Check Docker containers
    if not check_docker_running():
        logger.error("Failed to start Docker containers. Demo aborted.")
        return False
    
    # Step 2: Run the producer to generate data
    logger.info(f"Generating tweet data for {DATA_GENERATION_TIME} seconds...")
    producer_success = run_process(PRODUCER_SCRIPT, "Tweet Producer", DATA_GENERATION_TIME)
    
    if not producer_success:
        logger.warning("Producer encountered issues, but continuing with the demo...")
    
    # Step 3: Run the streaming job to process data
    logger.info(f"Processing data with Spark Streaming for {PROCESSING_WAIT_TIME} seconds...")
    streaming_success = run_process(STREAMING_SCRIPT, "Spark Streaming", PROCESSING_WAIT_TIME)
    
    if not streaming_success:
        logger.warning("Streaming job encountered issues, but continuing with the demo...")
    
    # Step 4: Run the batch job for comparison
    logger.info("Running batch processing job...")
    batch_success = run_process(BATCH_SCRIPT, "Batch Processing", None)
    
    if not batch_success:
        logger.error("Batch processing failed. Demo cannot continue without batch results.")
        return False
    
    # Step 5: Run the comparison analysis
    logger.info("Running comparison analysis...")
    comparison_success = run_process(COMPARISON_SCRIPT, "Comparison Analysis", None)
    
    if not comparison_success:
        logger.error("Comparison analysis failed. Cannot display results.")
        return False
    
    # Step 6: Display the results
    display_results()
    
    # Calculate and show total demo run time
    total_time = time.time() - start_time
    logger.info(f"Demo completed in {total_time:.1f} seconds ({total_time/60:.1f} minutes)")
    
    return True

if __name__ == "__main__":
    try:
        success = run_demo()
        if success:
            logger.info("Demo completed successfully!")
        else:
            logger.error("Demo encountered errors.")
            sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Demo stopped by user.")
    except Exception as e:
        logger.error(f"Unexpected error in demo: {e}")
        sys.exit(1) 