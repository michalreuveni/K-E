import pandas as pd
from collections import defaultdict

# File path
file_path = "/Users/michal.reuveni/Downloads/pages_2.xlsx"  # Replace with your actual file path

# Load the Excel file
df = pd.read_excel(file_path)

# Convert CONTEXT_ARRAY from string to actual lists (if stored as strings)
df["CONTEXT_ARRAY"] = df["CONTEXT_ARRAY"].apply(eval)  # Ensure arrays are lists


def find_problematic_pages(df, pages_col="CONTEXT_ARRAY", metric_col="NO_ELEMENT_EVENTS_PER", journey_col="JOURNEY_PER",
                           threshold=90):
    """
    Identify pages likely causing data collection issues, weighted by JOURNEY_PER,
    and filter to only those contributing >1% of the total weighted impact.

    Parameters:
        df (pd.DataFrame): DataFrame with session data.
        pages_col (str): Column containing lists of pages per session.
        metric_col (str): Column representing NO_ELEMENT_EVENTS_PER.
        journey_col (str): Column representing JOURNEY_PER (used for weighting).
        threshold (float): Minimum percentage to consider a journey problematic.

    Returns:
        list of tuples: [(page, estimated_contribution, percentage_contribution)] sorted in descending order.
    """
    # Filter only problematic sessions
    problematic_sessions = df[df[metric_col] > threshold]

    # Create a dictionary to store weighted occurrence of pages
    page_weights = defaultdict(float)

    for _, row in problematic_sessions.iterrows():
        for page in row[pages_col]:
            page_weights[page] += row[journey_col]  # Weighting by JOURNEY_PER

    # Compute total contribution for percentage filtering
    total_contribution = sum(page_weights.values())

    # Sort pages by total weighted impact (descending order)
    sorted_pages = sorted(
        [(page, weight, (weight / total_contribution) * 100) for page, weight in page_weights.items()],
        key=lambda x: x[1],  # Sort by estimated contribution (absolute value)
        reverse=True
    )

    # Filter pages contributing >1% of the total
    filtered_pages = [(page, contrib, pct) for page, contrib, pct in sorted_pages if pct > 1]

    return filtered_pages


# Find problematic pages with estimated contribution
problematic_pages = find_problematic_pages(df)

# Print results
print("Likely problematic pages (sorted by estimated contribution, only >1% impact):")
for page, contribution, pct in problematic_pages:
    print(f"{page}: Estimated Contribution = {contribution:.2f}, Percentage Contribution = {pct:.2f}%")
