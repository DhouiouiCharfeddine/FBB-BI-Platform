# 📊 FBB BI Platform – Tunisie Telecom

## 📃 Project Overview

This is an **end-of-study Business Intelligence platform** for **Tunisie Telecom’s Fixed Broadband (FBB) network**.\
It automates the ingestion, processing, and analysis of daily network router data, transforming raw JSON metrics into actionable insights visualized in **Power BI dashboards**.

### Objectives

- Automate daily ingestion and ETL of FBB network data
- Transform raw JSON files into structured, analysis-ready formats
- Provide interactive Power BI dashboards with daily auto-refresh
- Enable network performance monitoring and operational decision-making

---

## 🛠 Tools & Technologies

- **Python** – Automated ETL, data processing, and service orchestration
- **MongoDB** – Storage for raw and processed data
- **Power BI** – Interactive dashboards and visualization
- **JSON** – Raw input data format for metrics
- **CSV/Excel** – Intermediate data format for ETL

---

## 📂 Project Structure

```
├── __pycache__/                          
├── Directory_Json/
│   └── Qos_CPE/
│       ├── Config_Router_YYYYMMDD.json
│       └── Stat_IP_Traffic_YYYYMMDD.json
├── Text_Files/
│   ├── Ingested_Files.txt
│   ├── Processed_Documents.txt
│   └── Unknown_Files.txt
├── config_data.csv
├── config_data_new.csv
├── stat_data.csv
├── stat_data_new.csv
├── merged_df.csv
├── ETL_Routers.py                         # Main ETL script
├── Ingest_RoutersData.py                  # Import JSON into MongoDB
├── MyService.py                           # Windows service orchestrating the pipeline
└── README.md
```

---

## 🔍 How It Works

1. **Automatic Service Execution**

   - `MyService.py` runs every 24h automatically.
   - Checks `Directory_Json/Qos_CPE/` for new JSON files.
   - Skips already ingested files (`Ingested_Files.txt`) and logs unknown files (`Unknown_Files.txt`).

2. **Data Ingestion**

   - `Ingest_RoutersData.py` imports new JSON files into **MongoDB**.

3. **ETL & Transformation**

   - `ETL_Routers.py` extracts new documents, merges, transforms, and prepares data.
   - Avoids duplicates using `Processed_Documents.txt`.

4. **Power BI Integration**

   - Processed data is exported to **Power BI dashboards**.
   - Dashboards are set to **auto-refresh daily**, showing the latest metrics and insights.

---

## 💎 Features & Business Value

- **End-to-End Automation:** Daily pipeline runs automatically via Windows service
- **Data Integrity:** Duplicate files/documents avoided using tracking text files
- **Actionable Insights:** Network KPIs, traffic, and operational performance visualized
- **Scalable:** Supports new metrics or additional JSON sources

---

## 📏 How to Use

1. Clone the repository:

```bash
git clone https://github.com/yourusername/FBB-BI-Platform.git
```

2. Place incoming JSON files in `Directory_Json/Qos_CPE/`.
3. Run `MyService.py` to start the automated daily pipeline (or run ETL scripts manually for testing).
4. Open Power BI dashboards to view interactive insights.

---

## 🌟 Conclusion

This project demonstrates:

- Automated **ETL pipelines** for unstructured network data
- Integration of **MongoDB and Power BI** for real-time analysis
- Handling of **daily JSON files, deduplication, merging, and transformation**
- End-to-end **BI workflow** for operational decision-making

---

## 📣 Contact

- **LinkedIn:** [Charfeddine Dhouioui](https://www.linkedin.com/in/charfeddine-dhouioui-987ab7318)
- **Email:** [dhouiouicharfeddine@gmail.com](mailto\:dhouiouicharfeddine@gmail.com)

