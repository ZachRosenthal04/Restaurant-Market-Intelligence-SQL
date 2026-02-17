"""
RESTAURANT MARKET INTELLIGENCE MODEL
Author: Zachary Rosenthal
Purpose: Integrating disparate restaurant datasets to identify high-growth 
         disruptors and evaluate unit-level economic efficiency.
"""

import pandas as pd
import sqlite3

# --- 1. DATA INGESTION & DATABASE SETUP ---
# Creating a global connection object for the script
conn = sqlite3.connect('restaurant_stats.db')

def load_data():
    """Loads raw CSV data and standardizes state keys for relational joins."""
    # Define file paths
    top250_path = 'Top250.csv' 
    ind100_path = 'Independence100.csv'
    census_file_path = 'us_pop_by_state_2020.csv'    
    
    # Read CSVs into DataFrames
    df_top250 = pd.read_csv(top250_path)
    df_ind = pd.read_csv(ind100_path)
    df_pop = pd.read_csv(census_file_path)
    
    # --- CLEANING & NORMALIZATION ---
    # Strip periods and whitespace from the State column (e.g., 'N.Y.' -> 'NY')
    df_ind['State'] = df_ind['State'].str.replace('.', '', regex=False).str.strip()

    # Dictionary to bridge unconventional abbreviations to Census state_codes
    state_fix_map = {
        'Fla': 'FL', 'Ill': 'IL', 'Nev': 'NV', 'Ind': 'IN', 'Pa': 'PA',
        'Calif': 'CA', 'Ga': 'GA', 'Mich': 'MI', 'Mass': 'MA', 'Ore': 'OR',
        'Tenn': 'TN', 'Colo': 'CO', 'Va': 'VA', 'Texas': 'TX'
    }

    # Apply the mapping
    df_ind['State'] = df_ind['State'].map(state_fix_map).fillna(df_ind['State'])

    # Upload cleaned data to SQL tables
    df_top250.to_sql('top250', conn, index=False, if_exists='replace')
    df_ind.to_sql('ind100', conn, index=False, if_exists='replace')
    df_pop.to_sql('state_population', conn, index=False, if_exists='replace')
    
    print("Database Initialized: 'top250','ind100', and 'state_population' tables created.")

# --- 2. THE 'ADAPTIVE GROWTH' MODEL ---
def run_market_analysis():
    """Identifies brands outperforming category-specific economic baselines."""
    query = """
    SELECT 
        Restaurant,
        Segment_Category,
        (Sales * 1000 / Units) AS AUV_Thousands,
        CAST(REPLACE(YOY_Sales, '%', '') AS FLOAT) AS YOY_Growth,
        CASE 
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

# --- 3. MARKET EFFICIENCY ANALYSIS ---
def run_efficiency_analysis():
    """Normalizes independent restaurant sales against US Census population data."""
    query = """
    SELECT 
        p.state AS State_Name,
        p."2020_census" AS Population,
        SUM(r.Sales) AS Total_Sales,
        ROUND((SUM(r.Sales) / CAST(p."2020_census" AS FLOAT)), 2) AS Dollars_Per_Person
    FROM ind100 r
    JOIN state_population p ON r.State = p.state_code
    GROUP BY p.state
    ORDER BY Dollars_Per_Person DESC;
    """
    return pd.read_sql(query, conn)

# --- 4. EXECUTIVE SUMMARY AGGREGATION ---
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

# --- 5. MASTER EXECUTION & DISPLAY ---
if __name__ == "__main__":
    # Initialize the data
    load_data()
    
    print("\n--- PHASE 1: INDIVIDUAL BRAND CLASSIFICATION ---")
    analysis_df = run_market_analysis()
    print(analysis_df.head(10))
    
    print("\n--- PHASE 2: MARKET EFFICIENCY (Per Capita Spend) ---")
    efficiency_df = run_efficiency_analysis()
    print(efficiency_df.head(10)) 

    print("\n--- PHASE 3: EXECUTIVE MARKET SUMMARY ---")
    summary_df = get_executive_summary()
    print(summary_df)

    conn.close()
    print("\n✅ Analysis complete. Connection closed.")
