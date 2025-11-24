
def calculate_slots(rows_per_sec, avg_row_size_bytes, complexity_level, latency_target_sec):
    # Throughput benchmarks per slot in MB/sec based on complexity
    complexity_throughput = {
        'simple': 10,      # MB/sec per slot
        'moderate': 5,
        'complex': 3
    }

    if complexity_level not in complexity_throughput:
        raise ValueError("Invalid complexity level. Choose from: simple, moderate, complex")

    # Calculate data rate in MB/sec
    data_rate_mb_sec = (rows_per_sec * avg_row_size_bytes) / (1024 * 1024)

    # Get throughput per slot
    throughput_per_slot = complexity_throughput[complexity_level]

    # Calculate base slots needed
    base_slots = data_rate_mb_sec / throughput_per_slot

    # Apply buffer (30%)
    slots_with_buffer = base_slots * 1.3

    # Adjust for latency target (optional heuristic: if latency target < 10 sec, add 20%)
    if latency_target_sec < 10:
        slots_with_buffer *= 1.2

    # Round up
    recommended_slots = int(slots_with_buffer + 0.999)

    return {
        'data_rate_mb_sec': round(data_rate_mb_sec, 2),
        'base_slots': round(base_slots, 2),
        'recommended_slots': recommended_slots
    }

# Example usage
rows_per_sec = 50000          # streaming rows per second
avg_row_size_bytes = 500      # average row size in bytes
complexity_level = 'complex'  # query complexity
latency_target_sec = 5        # desired latency in seconds

result = calculate_slots(rows_per_sec, avg_row_size_bytes, complexity_level, latency_target_sec)

print("BigQuery Slot Sizing Estimate:")
print(f"Data Rate: {result['data_rate_mb_sec']} MB/sec")
print(f"Base Slots Needed: {result['base_slots']}")
print(f"Recommended Slots (with buffer): {result['recommended_slots']}")

