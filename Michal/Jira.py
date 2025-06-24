from jira import JIRA
import snowflake.connector

# Jira API Credentials
JIRA_URL = "https://biocatch.atlassian.net"  # Replace with your Jira URL
EMAIL = "michal.reuveni@biocatch.com"  # Replace with your Jira email
API_TOKEN = "ATATT3xFfGF0wqbyBMW6xBJennXpT-6WyEYyaoIpx_ySDkAPlLvgZNwuao-yKmUCC_uahz0xB5KyOrvhu4oNVdoqO9FBrxNPDCxnYd0k8kErrFQm6HBbWp09AcZBcTSc3K7aSahEAvLlgZVKC3WAimqxj9xJwCbC6RSuZ6EDgakzPMhj0WOOEG4=01592CF1"  # Replace with your Jira API token

# Connect to Jira
jira = JIRA(JIRA_URL, basic_auth=(EMAIL, API_TOKEN))

# Ask for a ticket ID
ISSUE_KEY = input("Enter Jira Ticket ID (e.g., PROJ-1234): ")

# Fetch the ticket details
issue = jira.issue(ISSUE_KEY)

# Extract the customer name (assuming it's stored in a custom field)
CUSTOM_FIELD_ID = "Customer ID"  # Replace with your actual custom field ID
customer_name = issue.fields.__dict__.get(CUSTOM_FIELD_ID, "unknown_customer")

# 🔹 Snowflake Credentials
SNOWFLAKE_ACCOUNT = "biocatch.prod_azure_westeurope"
USERNAME = "MICHAL.REUVENI@BIOCATCH.COM"
PASSWORD = "Avocado14!"
WAREHOUSE = "ANALYTICS_WH_MEDIUM"
DATABASE = "BIOCATCH_PROD"
TEMPLATE_DASHBOARD = "dq-dashboard-template-dY8FfuW4k"  # Template dashboard name

#🔹 Create new schema and new dashboard
new_schema = f"{customer_name.lower()}_schema"
new_dashboard_name = f"dq-dashboard-{customer_name.lower()}"

# 🔹 Connect to Snowflake
conn = snowflake.connector.connect(
    user=USERNAME,
    password=PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE
)
cursor = conn.cursor()

# 🔹 Sample Query (Replace with your actual query)
query = """
SELECT * FROM BIOCATCH_PROD.BUZZ.API_CALLS LIMIT 10;
"""

# 🔹 Execute the query
cursor.execute(query)

# 🔹 Fetch the results
results = cursor.fetchall()

# 🔹 Display the results
for row in results:
    print(row)

# Close the cursor and connection
cursor.close()
conn.close()