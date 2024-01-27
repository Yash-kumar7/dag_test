# performance_testing.py

import time
fromdags.main_dag import fetch_app_usage_logs, extract_booking_logs, acquire_occupancy_logs

def test_fetch_app_usage_logs_performance():
    start_time = time.time()
    fetch_app_usage_logs()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"fetch_app_usage_logs took {execution_time} seconds")

def test_extract_booking_logs_performance():
    start_time = time.time()
    extract_booking_logs()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"extract_booking_logs took {execution_time} seconds")

def test_acquire_occupancy_logs_performance():
    start_time = time.time()
    acquire_occupancy_logs()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"acquire_occupancy_logs took {execution_time} seconds")
