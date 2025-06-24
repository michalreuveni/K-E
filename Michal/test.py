import matplotlib.pyplot as plt
import pandas as pd

# Create a timeline of months in 2025
months = pd.date_range(start="2025-01-01", end="2025-12-31", freq="MS")
kpi_values = [50 + i * 2 for i in range(len(months))]  # Simulated KPI trend

# Create events with approximate dates
events = {
    "production-grade confluence page": "2025-02-15",
    "AI knowledge agent launch": "2025-06-01",
    "Podcast Launch": "2025-09-10",
    "Complete learning plans": "2025-11-10"
}

# Plotting
plt.figure(figsize=(12, 6))
plt.plot(months, kpi_values, label="TIS KPI", marker='o')
plt.title("TIS KPI Progress with Enablement Events (2025)")
plt.xlabel("Month")
plt.ylabel("TIS KPI")
plt.grid(True)

# Add vertical lines for events
for event, date in events.items():
    event_date = pd.to_datetime(date)
    plt.axvline(event_date, color='red', linestyle='--', linewidth=1)
    plt.text(event_date, max(kpi_values) + 2, event, rotation=90,
             verticalalignment='bottom', horizontalalignment='center', color='red')

plt.tight_layout()
plt.xticks(rotation=45)
plt.legend()
plt.show()
