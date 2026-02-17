"""
RESTAURANT MARKET INTELLIGENCE MODEL
Author: Zachary Rosenthal
Purpose: Integrating disparate restaurant datasets to identify high-growth 
         disruptors and evaluate unit-level economic efficiency.
"""

import pandas as pd
import sqlite3
import os

# --- 1. DATA INGESTION & DATABASE SETUP ---
# Connect to SQLite (Creates 'restaurant_stats.db' if it doesn't exist)
conn = sqlite3.connect('restaurant_stats.db')

def load_data():
    """Loads raw CSV data into a centralized SQL database."""
    top250_path = 'Top250.csv' 
    ind100_path = 'Independence100.csv'
      census_file_path = 'us_pop_by_state_2020.csv'    
         
    
    df_top250 = pd.read_csv(top250_path)
    df_ind100 = pd.read_csv(ind100_path)
    df_pop = pd.read_csv(census_file_path)
    
    df_top250.to_sql('top250', conn, index=False, if_exists='replace')

    # Strip periods from the State column (e.g., 'N.Y.' becomes 'NY')
    df_ind['State'] = df_ind['State'].str.replace('.', '', regex=False).str.strip()

    # The 'Translation' dictionary the dataset quirks
    state_fix_map = {
         'Fla': 'FL',
         'Ill': 'IL',
         'Nev': 'NV',
         'Ind': 'IN',
         'Pa': 'PA',
         'Calif': 'CA',
         'Ga': 'GA',
         'Mich': 'MI',
         'Mass': 'MA',
         'Ore': 'OR',
         'Tenn': 'TN',
         'Colo': 'CO',
         'Va': 'VA',
         'Texas': 'TX'
         }

         # Apply the map to the 'State' column
         # .fillna keeps original values if they aren't in the map (like 'NY' or 'NJ')
         df_ind['State'] = df_ind['State'].map(state_fix_map).fillna(df_ind['State'])

         # This adds a second table called 'ind100' to your existing database
         df_ind.to_sql('ind100', conn, index=False, if_exists='replace')
         df_ind100.to_sql('ind100', conn, index=False, if_exists='replace')

         df_pop.to_sql('state_population', conn, index=False, if_exists='replace')
         print("Database Initialized: 'top250','ind100', 'state_population' tables created.")

# --- 2. THE 'ADAPTIVE GROWTH' MODEL ---
def run_market_analysis():
    """
    Analyzes segment-specific performance by re-calibrating 'Success' thresholds.
    This identifies brands that outperform their category-specific economic baselines.
    """
    # This query cleans the YOY growth strings and applies the Adaptive Model logic
    query = """
    SELECT 
        Restaurant,
        Segment_Category,
        (Sales * 1000 / Units) AS AUV_Thousands,
        CAST(REPLACE(YOY_Sales, '%', '') AS FLOAT) AS YOY_Growth,
        CASE 
            /* Adaptive Threshold: Different segments require different AUV benchmarks */
            WHEN Segment_Category = 'Steak' AND (Sales * 1000 / Units) > 5000 AND CAST(REPLACE(YOY_Sales, '%', '') AS FLOAT) > 10 THEN 'High-Growth Disruptor'
            WHEN Segment_Category = 'Chicken' AND (Sales * 1000 / Units) > 1000 AND CAST(REPLACE(YOY_Sales, '%', '') AS FLOAT) > 10 THEN 'High-Growth Disruptor'
            WHEN CAST(REPLACE(YOY_Sales, '%', '') AS FLOAT) < 0 THEN 'At-Risk Laggard'
            ELSE 'Stable Performer'
        END AS Market_Status
    FROM top250
    WHERE Segment_Category IN ('Chicken', 'Steak', 'Burger', 'Pizza')
    ORDER BY Segment_Category, YOY_Growth DESC;
    """
    return pd.read_sql(query, conn)

# --- 3. EXECUTIVE SUMMARY AGGREGATION ---
def get_executive_summary():
    """Counts Disruptors vs Laggards across key market segments."""
    query = """
    SELECT 
        Segment_Category,
        Market_Status,
        COUNT(*) as Brand_Count
    FROM (
        SELECT 
            Segment_Category,
            CASE 
                WHEN CAST(REPLACE(YOY_Sales, '%', '') AS FLOAT) > 10.0 THEN 'High-Growth Disruptor'
                WHEN CAST(REPLACE(YOY_Sales, '%', '') AS FLOAT) < 0.0 THEN 'At-Risk Laggard'
                ELSE 'Stable Performer'
            END AS Market_Status
        FROM top250
    )
    WHERE Segment_Category IN ('Chicken', 'Steak', 'Burger', 'Pizza')
    GROUP BY Segment_Category, Market_Status
    ORDER BY Segment_Category, Brand_Count DESC;
    """
    return pd.read_sql(query, conn)

# --- 4. EXECUTION & DISPLAY ---
if __name__ == "__main__":
    
    print("\n--- PHASE 1: INDIVIDUAL BRAND CLASSIFICATION ---")
    analysis_df = run_market_analysis()
    print(analysis_df.head(10))
    
    print("\n--- PHASE 2: EXECUTIVE MARKET SUMMARY ---")
    summary_df = get_executive_summary()
    print(summary_df)

    conn.close()
