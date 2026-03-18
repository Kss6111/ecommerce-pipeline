# scripts/sentiment_analysis.py
# VADER NLP sentiment analysis on 100K+ customer reviews

import snowflake.connector
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from dotenv import load_dotenv
import os

load_dotenv()

# ── 1. Connect and pull reviews from Snowflake ──────────────────────────────
print("Connecting to Snowflake...")
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse="ECOMMERCE_WH",
    database="ECOMMERCE_DB",
    schema="STAGING",
    role="ACCOUNTADMIN"
)

print("Pulling reviews...")
reviews_df = pd.read_sql("""
    SELECT
        REVIEW_ID,
        ORDER_ID,
        REVIEW_SCORE,
        REVIEW_TITLE,
        REVIEW_MESSAGE
    FROM STAGING.STG_ORDER_REVIEWS
""", conn)

print(f"  Loaded {len(reviews_df):,} reviews")

# ── 2. Run VADER sentiment analysis ─────────────────────────────────────────
print("Running sentiment analysis...")
analyzer = SentimentIntensityAnalyzer()

def get_sentiment_label(text):
    if pd.isna(text) or str(text).strip() == '':
        return 'No Review'
    score = analyzer.polarity_scores(str(text))['compound']
    if score >= 0.05:
        return 'Positive'
    elif score <= -0.05:
        return 'Negative'
    else:
        return 'Neutral'

def get_sentiment_score(text):
    if pd.isna(text) or str(text).strip() == '':
        return 0.0
    return analyzer.polarity_scores(str(text))['compound']

reviews_df['sentiment']       = reviews_df['REVIEW_MESSAGE'].apply(get_sentiment_label)
reviews_df['sentiment_score'] = reviews_df['REVIEW_MESSAGE'].apply(get_sentiment_score)

# ── 3. Print findings ────────────────────────────────────────────────────────
print("\n── Sentiment Distribution ──────────────────────────")
sentiment_counts = reviews_df['sentiment'].value_counts()
total = len(reviews_df)
for label, count in sentiment_counts.items():
    print(f"  {label:<12} {count:>7,}  ({100*count/total:.1f}%)")

print("\n── Avg Review Score by Sentiment ───────────────────")
avg_scores = reviews_df.groupby('sentiment')['REVIEW_SCORE'].mean().round(2)
for label, score in avg_scores.items():
    print(f"  {label:<12} {score}")

print("\n── Reviews with No Text ────────────────────────────")
no_review = (reviews_df['sentiment'] == 'No Review').sum()
print(f"  {no_review:,} reviews had no comment text ({100*no_review/total:.1f}%)")

# ── 4. Save results ──────────────────────────────────────────────────────────
os.makedirs('visuals', exist_ok=True)
reviews_df.to_csv('visuals/sentiment_analysis.csv', index=False)
print(f"\n✅ Saved to visuals/sentiment_analysis.csv")

conn.close()
print("✅ Sentiment analysis complete!")