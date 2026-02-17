# Restaurant-Market-Intelligence-SQL

Restaurant Industry Market Intelligence Report
# Phase 1
Strategic Analysis of the US Top 250 & Independence 100 
Executive Summary
This project analyzes market saturation and unit-level efficiency across the U.S. restaurant landscape. By integrating disparate datasets (Large Chains vs. Independent Legends), I developed an Adaptive Growth Model to identify market disruptors and at-risk incumbents.

## Key Business Insights - Phase 1
Segment Efficiency Gap: While the "Steak" category leads in Average Unit Volume (AUV), the "Chicken" segment shows the highest velocity for new market disruptors.

The "Disruptor" Identification: By re-calibrating performance benchmarks for the Chicken segment ($1M+ AUV), I identified Jollibee and Chicken Salad Chick as high-growth disruptors, significantly outperforming the industry average YOY growth of 1.6%.

Risk Mitigation: Developed a SQL-based tagging system to flag "At-Risk Laggards"—brands showing negative YOY growth despite high unit counts—providing a lead-generation tool for competitive displacement.

Technical Skills Demonstrated
Advanced SQL: Subqueries and complex CASE statements for business logic.

Data Normalization: Merging datasets with 1,000x scale differences and cleaning "dirty" string data for numeric analysis.

# Phase 2: Geographic Market Intelligence & Normalization
In this phase, I integrated the 2020 US Census population data to move beyond raw sales totals and evaluate Market Efficiency.

Data Engineering & Relational Joins
The Challenge: Sales data used unconventional state truncations (e.g., "Calif", "Ore"), while Census data used standard 2-letter ISO codes (e.g., "CA", "OR").

The Solution: Engineered a Python-based mapping dictionary to standardize keys across disparate datasets, enabling a 100% match rate for relational SQL joins.

Metric Development: Dollars Per Person
I developed a "Market Efficiency Score" to level the playing field between high-population states (California) and smaller markets (D.C.).

## Key Business Insights - Phase 2
TBD

## Data Dictionary:
Restaurant Market Intelligence: Data Dictionary
Restaurant: String - The legal or trade name of the restaurant brand.
Segment_Category: String - The specific industry niche (e.g., Burger, Steak, Chicken).
Sales: Float - Total annual system-wide sales, typically expressed in Millions of USD.
Units: Integer - The total number of physical locations/storefronts in operation.
AUV_Thousands: Float - Average Unit Volume: Calculated as (Sales * 1000 / Units). Measures store-level efficiency.
YOY_Sales: String - Year-Over-Year Sales: The percentage change in total sales compared to the previous year.
Market_Status: String - A custom-calculated label identifying the brand's growth trajectory (e.g., Disruptor, Laggard).
