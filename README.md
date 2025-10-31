# Land2Import – Crop import predition

Land2Import is a data-driven ETL and analytics system designed to study how agricultural land conversion across Indian states correlates with national food import volumes. The project integrates multiple government datasets, cleans and standardizes the data, merges them into a unified analytical model, and generates insights through visualization dashboards.

---

## ✅ Features
- Integrates multi-source datasets (Agriculture, Land Use, Import/Export)
- Cleans and standardizes missing, null, and inconsistent values
- Performs unit conversions and year-wise interpolation
- Creates a master dataset with State, Year, Land Area, Converted Land, Import Volume, and Import Value
- Provides analytics via Metabase / Power BI dashboards
- Modular and scalable ETL pipeline built in Python

---

## 🏗️ System Architecture

---

## 📦 Data Sources

Land2Import combines multiple official datasets:

- **Land Use Statistics (LUS)** – Ministry of Agriculture & Farmers’ Welfare  
- **Agricultural Census of India** – Directorate of Economics & Statistics  
- **Food Import Data** – Department of Commerce (DGFT)  
- **FAOSTAT International Trade Data** – Food and Agriculture Organization  
- **State-wise Crop Production Reports** – Govt. Open Data Portal  
- **Customs Import/Export Datasets** – Data.gov.in

These datasets are cleaned, normalized, and merged based on **State** and **Year**.

---

## 🔄 ETL Pipeline Overview

1. **Extract**  
   - Load CSV/Excel datasets from multiple government sources  
   - Standardize state names and year formats  
   - Remove duplicates and invalid rows  

2. **Transform**  
   - Handle missing/null values using forward/backward fill  
   - Convert area units (hectare ↔ sq.km)  
   - Compute *converted land area*  
   - Aggregate food import volumes & values  

3. **Load**  
   - Store processed tables in a unified master dataset  
   - Export cleaned dataset for dashboards (CSV/Parquet)

---

## 📊 Dashboards & Insights

The unified dataset is used to generate analytics such as:

- Year-wise agricultural land conversion trends  
- State-wise hotspots of agricultural land loss  
- Correlation between land conversion and food imports  
- Import dependency scorecard  
- Time-series charts with forecasting  

Dashboards can be built in **Metabase**, **Power BI**, or **Superset**.

---

## 🧠 Key Outcomes

- Identifies states with rapid farmland reduction  
- Highlights food categories driving import dependency  
- Enables policymakers to assess long-term risks  
- Helps researchers study land–food–economy relationships  

---

## 🚀 Tech Stack

- **Python** – Pandas, NumPy, Matplotlib  
- **ETL** – Custom Python pipeline  
- **Storage** – CSV / Parquet  
- **Visualization** – Metabase / Power BI  
- **Version Control** – Git + GitHub  

---

---

## 📜 License
This project is released under the **MIT License**.

---

## 🤝 Contributions
Contributions, dataset suggestions, or improvements are welcome!  
Feel free to submit a pull request or open an issue.



