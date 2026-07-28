# Synapse-Databricks-Fabric-Comparison---Project
Comparative end-to-end data engineering solution implementing the same Retail Banking & Card Analytics use case using Azure Synapse Analytics, Azure Databricks, and Microsoft Fabric. The project evaluates data ingestion, Bronze-Silver-Gold transformations, orchestration, processing performance, development experience, CI/CD, cost, and Power BI integration.

This project implements the same  Banking, Fraud Transactions and Credit Card Analytics business requirements across three Microsoft data platforms: Azure Synapse Analytics, Azure Databricks, and Microsoft Fabric.

The objective is to evaluate each platform based on real implementation experience rather than theoretical feature comparison.

# Platforms Compared
Azure Synapse Analytics
Azure Databricks
Microsoft Fabric

# Evaluation Criteria
Data ingestion performance
Data transformation performance
Full pipeline execution time
Compute/cluster startup time
Individual notebook/script execution time
Orchestration capabilities
Ease of solution development
Git and CI/CD integration
Cost and compute model
Power BI integration
Developer experience and stability
Scalability

# Business Objectives
Objective 1 — Customer Segmentation (Value & Risk)
Segment the current customer base into High-Value, Medium-Value, and At-Risk tiers using credit score and income

Objective 2 — Transaction & Spending Trends
Track how customers are spending across time and category

Objective 3 — Credit Utilization & High-Risk Accounts
Monitor credit utilization to flag over-leveraged, high-risk customers.

Objective 4 — Fraud Monitoring 
Monitor fraudulent activity to surface high-risk transactions, categories, and locations.


Architectural Diagrams 

# Azure Synapse Analytics

<img width="761" height="452" alt="synapse-architectural-diagram" src="https://github.com/user-attachments/assets/e423844b-1de3-4c8f-b93e-ae880d9b31fc" />

# Azure Data Bricks

<img width="752" height="342" alt="databricks-architectural-diagram" src="https://github.com/user-attachments/assets/584d4329-7ba1-43a5-9e13-ba34da084183" />

# Microsoft Fabric

<img width="772" height="376" alt="fabric-architectural diagram" src="https://github.com/user-attachments/assets/6bc9c447-256a-469d-b32c-b8c924726dc6" />

# Key Project Findings

Based on the implemented workload, Azure Databricks provided the best overall processing performance and development stability. Microsoft Fabric provided the most integrated analytics experience and the simplest initial setup, while Azure Synapse provided reliable enterprise orchestration and strong SQL-based analytics capabilities.



