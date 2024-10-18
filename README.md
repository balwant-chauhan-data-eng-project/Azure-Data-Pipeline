 # End-to-End Data Pipeline with SQL Server, Azure Data Factory, Databricks, Data Lake, and Synapse 🔄🌐

In this project, we will create a data pipeline to move data from an on-premises SQL Server to an Azure Data Lake, transform and organize it through various stages (bronze, silver, gold), and finally connect it to Azure Synapse for data visualization in Power BI.

---

## Step 1: Download AdventureWorks Database 🗄️

- Download the AdventureWorks Database from [Microsoft SQL Server sample databases](https://github.com/Microsoft/sql-server-samples).
- Install and configure SQL Server on your on-premises system.

---

## Step 2: Create SQL Server Credentials 🔐

- Create a user in SQL Server with appropriate permissions:
  - Username: `AdventureUser`
  - Password: `SecurePassword123`

---

## Step 3: Set Up Azure Data Factory and SSIS Integration Runtime 🔄🏗️

1. Create a Linked Service in Azure Data Factory (ADF) to connect to your on-premises SQL Server using the Self-hosted or Azure-SSIS Integration Runtime.
2. Install Integration Runtime on your on-premises system and test the connection.

---

## Step 4: Build a Data Pipeline to Transfer Data to Bronze Data Lake 📥

- Create a pipeline in Azure Data Factory to transfer data from the on-premises SQL Server to the Bronze Data Lake.

---

## Step 5: Mount the Data Lake in Databricks 📁🔗

- Mount the Bronze Data Lake in Azure Databricks for processing.

---

## Step 6: Transform Data from Bronze to Silver 🥈🔄

- Create a Databricks notebook to convert DateTime to Date format and save the transformed data to the Silver Container in the Data Lake.

---

## Step 7: Transform Data from Silver to Gold 🥇✨

- Rename columns in the Silver data for consistency and save the transformed data to the Gold Container in the Data Lake.

---

## Step 8: Create a View in Azure Synapse and Connect to Gold DB 🛠️

- Connect Azure Synapse to the Gold Data Layer and create a view to simplify querying the aggregated data.

---

## Step 9: Automate Data Pipeline Execution with Azure Data Factory 📅🔄

- Automate the execution of the data pipeline within Azure Data Factory.

---

## Step 10: Visualize the Data in Power BI 📊✨

- Connect Power BI to the Azure Synapse view for data visualization.



