# scripts/customer_segmentation.py
# K-Means clustering on RFM data from mart_customer_ltv

import snowflake.connector
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from dotenv import load_dotenv
import os

load_dotenv()

# ── 1. Connect and pull RFM data ─────────────────────────────────────────────
print("Connecting to Snowflake...")
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse="ECOMMERCE_WH",
    database="ECOMMERCE_DB",
    schema="MARTS",
    role="ACCOUNTADMIN"
)

print("Pulling RFM data...")
rfm_df = pd.read_sql("""
    SELECT
        CUSTOMER_ID,
        RECENCY_DAYS,
        TOTAL_ORDERS,
        TOTAL_SPENT
    FROM MARTS.MART_CUSTOMER_LTV
    WHERE RECENCY_DAYS IS NOT NULL
      AND TOTAL_ORDERS IS NOT NULL
      AND TOTAL_SPENT IS NOT NULL
""", conn)

print(f"  Loaded {len(rfm_df):,} customers")
conn.close()

# ── 2. Scale features ─────────────────────────────────────────────────────────
features = ['RECENCY_DAYS', 'TOTAL_ORDERS', 'TOTAL_SPENT']
scaler = StandardScaler()
rfm_scaled = scaler.fit_transform(rfm_df[features])

# ── 3. Elbow method to find optimal clusters ──────────────────────────────────
print("Running elbow method...")
inertias = []
for k in range(1, 11):
    kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
    kmeans.fit(rfm_scaled)
    inertias.append(kmeans.inertia_)

os.makedirs('visuals', exist_ok=True)
plt.figure(figsize=(8, 5))
plt.plot(range(1, 11), inertias, 'bo-')
plt.title('Elbow Method — Optimal Number of Clusters')
plt.xlabel('Number of Clusters')
plt.ylabel('Inertia')
plt.tight_layout()
plt.savefig('visuals/elbow_curve.png', dpi=150)
plt.close()
print("  Saved visuals/elbow_curve.png")

# ── 4. Run K-Means with 4 clusters ───────────────────────────────────────────
print("Running K-Means with 4 clusters...")
kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
rfm_df['cluster'] = kmeans.fit_predict(rfm_scaled)

# ── 5. Print cluster summary to help label them ───────────────────────────────
print("\n── Cluster Summary ─────────────────────────────────")
cluster_summary = rfm_df.groupby('cluster')[features].mean().round(2)
print(cluster_summary)

# ── 6. Label clusters based on characteristics ───────────────────────────────
# High spend + low recency = Champions
# Low recency + low spend = Loyal
# High recency + medium spend = At Risk
# High recency + low spend = Lost
def label_cluster(row):
    if row['TOTAL_SPENT'] > cluster_summary['TOTAL_SPENT'].median() and \
       row['RECENCY_DAYS'] < cluster_summary['RECENCY_DAYS'].median():
        return 'Champions'
    elif row['RECENCY_DAYS'] < cluster_summary['RECENCY_DAYS'].median():
        return 'Loyal Customers'
    elif row['TOTAL_SPENT'] > cluster_summary['TOTAL_SPENT'].median():
        return 'At Risk'
    else:
        return 'Lost Customers'

rfm_df['segment'] = rfm_df.apply(label_cluster, axis=1)

print("\n── Segment Distribution ────────────────────────────")
seg_counts = rfm_df['segment'].value_counts()
for seg, count in seg_counts.items():
    print(f"  {seg:<20} {count:>7,}  ({100*count/len(rfm_df):.1f}%)")

# ── 7. Plot segments ──────────────────────────────────────────────────────────
print("\nGenerating segment plot...")
plt.figure(figsize=(10, 6))
sns.scatterplot(
    data=rfm_df,
    x='RECENCY_DAYS',
    y='TOTAL_SPENT',
    hue='segment',
    palette='Set2',
    alpha=0.5,
    s=20
)
plt.title('Customer Segments — RFM Clustering')
plt.xlabel('Recency (Days Since Last Order)')
plt.ylabel('Total Spent ($)')
plt.legend(title='Segment')
plt.tight_layout()
plt.savefig('visuals/customer_segments.png', dpi=150)
plt.close()
print("  Saved visuals/customer_segments.png")

# ── 8. Save results ───────────────────────────────────────────────────────────
rfm_df.to_csv('visuals/customer_segments.csv', index=False)
print("  Saved visuals/customer_segments.csv")

print("\n✅ Customer segmentation complete!")