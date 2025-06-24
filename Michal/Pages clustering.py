import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.cluster import KMeans
import ast
from collections import Counter
from snowflake_access.SQLUtils import query_on_multiple_cids  # Ensure this is available in your environment

# Step 1: Run Snowflake Query Instead of Reading Excel
query = """
 select CONTEXT_ARRAY,(COUNT_if(e_element_events_count=0) / SUM(COUNT(*)) OVER ()) * 100 as missing_data_per,
(COUNT(*) / SUM(COUNT(*)) OVER ()) * 100 as journey_per,
count(*) as total_calls from 
            (with t1 as (select sid,e_element_events_count
            FROM sessions 
            WHERE to_date(starttime) >=current_date()-2
            AND session_type not in ('api_only','meta_data_wup_only')
            AND sid in (select associated_sid from api_calls
                         where to_date(starttime) >=current_date()-2
                        
                         )
            GROUP BY all
            
            ),
            t2 as (SELECT
                    sid,
                    ARRAY_AGG(application) AS CONTEXT_ARRAY
                FROM (
                    SELECT 
                        distinct sid,
                        application,CONTEXT_START_TIME,
                        ROW_NUMBER() OVER (PARTITION BY sid, application ORDER BY CONTEXT_START_TIME) AS row_num
                        FROM contexts
                        where to_date(starttime) >=current_date()-2
                        order by CONTEXT_START_TIME
                ) where row_num =1
                GROUP BY 
                    sid
                   )
             select t1.*,t2.* from t1 natural join t2) group by all order by Journey_per desc;
    
"""

df = query_on_multiple_cids(query, ['dagoth'])

# Step 2: Convert CONTEXT_ARRAY string lists to actual Python lists
df["x"] = df["context_array"].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)

# Step 3: Flatten Unique Pages
all_pages = set(page for pages_list in df["x"].dropna() for page in pages_list)

# Step 4: Load Embedding Model
model = SentenceTransformer("paraphrase-MiniLM-L6-v2")

# Step 5: Generate Embeddings
page_vectors = model.encode(list(all_pages))

# Step 6: Clustering
num_clusters = 10
kmeans = KMeans(n_clusters=num_clusters, random_state=42, n_init=10)
labels = kmeans.fit_predict(page_vectors)

# Step 7: Map Pages to Clusters
page_clusters = {page: f"Cluster {label}" for page, label in zip(all_pages, labels)}

# Step 8: Generate Cluster Names
def name_cluster(pages):
    words = [word for page in pages for word in page.split("_")]
    common_words = Counter(words).most_common(2)
    return " ".join([w[0].capitalize() for w in common_words]) if common_words else "Unnamed Cluster"

cluster_names = {}
for cluster_id in set(labels):
    cluster_pages = [page for page, label in page_clusters.items() if label == f"Cluster {cluster_id}"]
    cluster_names[f"Cluster {cluster_id}"] = name_cluster(cluster_pages)

# Step 9: Assign Cluster Labels to Each Session
def assign_session_label(page_list):
    if not page_list:
        return "Unknown", "Unknown Cluster"
    cluster_counts = {page_clusters.get(page, "Unknown"): 0 for page in page_list}
    for page in page_list:
        if page in page_clusters:
            cluster_counts[page_clusters[page]] += 1
    most_common_cluster = max(cluster_counts, key=cluster_counts.get)
    return most_common_cluster, cluster_names.get(most_common_cluster, "Unknown Cluster")

df["session_cluster"], df["session_cluster_name"] = zip(*df["x"].apply(assign_session_label))

# Step 10: Add Cluster Pages to DataFrame
cluster_pages = {f"Cluster {cluster_id}": [page for page, label in page_clusters.items() if label == f"Cluster {cluster_id}"] for cluster_id in set(labels)}
df["cluster_pages"] = df["session_cluster"].apply(lambda x: ', '.join(cluster_pages.get(x, [])))

numeric_cols = [
    "total_calls",
    "missing_data_per",
    "journey_per"
]

for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')  # Or df[col].astype(float)

# Step 11: Save to Excel
output_file = "/Users/michal.reuveni/Downloads/Pages_clusters_from_snowflake.xlsx"
df.to_excel(output_file, index=False)
print(f"Clustering complete! Results saved to {output_file}")
