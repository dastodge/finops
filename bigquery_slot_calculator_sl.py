
import streamlit as st

def calculate_slots(rows_per_sec, avg_row_size_bytes, complexity_level, latency_target_sec):
    complexity_throughput = {
        'simple': 10,      # MB/sec per slot
        'moderate': 5,
        'complex': 3
    }

    if complexity_level not in complexity_throughput:
        raise ValueError("Invalid complexity level. Choose from: simple, moderate, complex")

    data_rate_mb_sec = (rows_per_sec * avg_row_size_bytes) / (1024 * 1024)
    throughput_per_slot = complexity_throughput[complexity_level]
    base_slots = data_rate_mb_sec / throughput_per_slot
    slots_with_buffer = base_slots * 1.3

    if latency_target_sec < 10:
        slots_with_buffer *= 1.2

    recommended_slots = int(slots_with_buffer + 0.999)

    return data_rate_mb_sec, base_slots, recommended_slots

# Streamlit UI
st.title("BigQuery Slot Sizing Calculator")
st.write("Estimate the number of slots needed for streaming workloads with Continuous Queries.")

rows_per_sec = st.slider("Rows per second", min_value=1000, max_value=100000, value=50000, step=1000)
avg_row_size_bytes = st.slider("Average row size (bytes)", min_value=100, max_value=2000, value=500, step=50)
complexity_level = st.selectbox("Query complexity", ["simple", "moderate", "complex"])
latency_target_sec = st.slider("Latency target (seconds)", min_value=1, max_value=60, value=5)

if st.button("Calculate Slots"):
    data_rate, base_slots, recommended_slots = calculate_slots(rows_per_sec, avg_row_size_bytes, complexity_level, latency_target_sec)
    st.subheader("Results")
    st.write(f"**Data Rate:** {data_rate:.2f} MB/sec")
    st.write(f"**Base Slots Needed:** {base_slots:.2f}")
