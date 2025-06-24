import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.cluster import KMeans
from collections import Counter

# Step 1: Load Excel File
file_path = "/Users/michal.reuveni/Downloads/comments.xlsx"  # Replace with your file path
df = pd.read_excel(file_path)

# Step 2: Drop NaNs and clean text
df["CLEAN_PAGE"] = df["CONTEXT_ARRAY"].fillna("").astype(str).str.strip()
df = df[df["CLEAN_PAGE"] != ""]  # Remove rows with empty strings

# Step 3: Get Unique Pages
all_pages = df["CLEAN_PAGE"].unique()

# Step 4: Load Pre-trained Embedding Model
model = SentenceTransformer("paraphrase-MiniLM-L6-v2")

# Step 5: Convert Pages into Embeddings
page_vectors = model.encode(list(all_pages))

# Step 6: Cluster Pages
num_clusters = 10  # Adjust based on dataset size
kmeans = KMeans(n_clusters=num_clusters, random_state=42, n_init=10)
labels = kmeans.fit_predict(page_vectors)

# Step 7: Map Pages to Clusters
page_clusters = {page: f"Cluster {label}" for page, label in zip(all_pages, labels)}

# Step 8: Generate Cluster Names Based on Common Words
def name_cluster(pages):
    words = [word for page in pages for word in page.split("_")]  # Adjust separator if needed
    common_words = Counter(words).most_common(2)
    return " ".join([w[0].capitalize() for w in common_words]) if common_words else "Unnamed Cluster"

# Step 9: Assign Meaningful Names to Clusters
cluster_names = {}
for cluster_id in set(labels):
    cluster_pages = [page for page, label in page_clusters.items() if label == f"Cluster {cluster_id}"]
    cluster_names[f"Cluster {cluster_id}"] = name_cluster(cluster_pages)

# Step 10: Assign Cluster Labels to Each Row
def assign_cluster_info(page):
    cluster = page_clusters.get(page, "Unknown")
    name = cluster_names.get(cluster, "Unknown Cluster")
    return cluster, name

df["session_cluster"], df["session_cluster_name"] = zip(*df["CLEAN_PAGE"].apply(assign_cluster_info))

# Step 11: Add Cluster Pages to Excel
cluster_pages = {f"Cluster {cluster_id}": [page for page, label in page_clusters.items() if label == f"Cluster {cluster_id}"] for cluster_id in set(labels)}
df["cluster_pages"] = df["session_cluster"].apply(lambda x: ', '.join(cluster_pages.get(x, [])))

# Step 12: Save Results
output_file = "/Users/michal.reuveni/Downloads/comment_cluster.xlsx"
df.to_excel(output_file, index=False)
print(f"Clustering complete! Results saved to {output_file}")
