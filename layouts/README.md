## 📝 Description
Added the technical presentation for the **Bronze Layer (Day 1)** to the `layouts/` directory. This is the first part of a 3-part documentation series covering the end-to-end Medallion Architecture (Bronze, Silver, Gold) of this project.

## 🏗️ What's inside
* **Ingestion Strategy:** Visual explanation of the ELT approach and raw data landing.
* **Security Measures:** Documentation on how the pipeline prevents Zip-Slips and GZ bombs during extraction.
* **Storage Patterns:** Details on the atomic write operations (`.tmp` to `.rename`) and the Hive-style partitioning implemented for future partition pruning.

## 🚀 Next Steps
* Add Silver Layer presentation (DuckDB vectorization & Pydantic contracts).
* Add Gold Layer presentation (Dimensional modeling).