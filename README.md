# End-to-End Data Pipeline with SQL Server, Azure Data Factory, Databricks, Data Lake, and Synapse 🔄🌐

In this project, we will create a data pipeline to move data from an on-premises SQL Server to an Azure Data Lake, transform and organize it through various stages (bronze, silver, gold), and finally connect it to Azure Synapse for data visualization in Power BI.

---

## Step 1: Download AdventureWorks Database 🗄️

- **Download the AdventureWorks Database** from Microsoft SQL Server sample databases [here](https://github.com/Microsoft/sql-server-samples).
- Install and configure **SQL Server** on your on-premises system to store the AdventureWorks data.

---

## Step 2: Create SQL Server Credentials 🔐

- **Create a User in SQL Server** with appropriate permissions to manage the database.
  - Username: `AdventureUser`
  - Password: `SecurePassword123`

```sql
CREATE LOGIN AdventureUser WITH PASSWORD = 'SecurePassword123';
CREATE USER AdventureUser FOR LOGIN AdventureUser;
ALTER SERVER ROLE sysadmin ADD MEMBER AdventureUser;

---

## Step 3: Set Up Azure Data Factory and SSIS Integration Runtime 🔄🏗️

1. **Create a Linked Service** in **Azure Data Factory (ADF)** to connect to your on-premises SQL Server using the **Self-hosted Integration Runtime** or **Azure-SSIS Integration Runtime**.
   - Use the **SSIS Integration Runtime** to bridge ADF with your on-premises SQL Server.

2. **Install Integration Runtime** on your on-premises system:
   - Go to Azure Data Factory > Manage > Integration Runtimes > +New > Self-Hosted > Download and configure it.

3. **Test the Connection** in Azure Data Factory to ensure it can connect to your on-premises SQL Server.

---

## Step 4: Build a Data Pipeline to Transfer Data to Bronze Data Lake 📥

- **Create a Pipeline** in Azure Data Factory:
  1. **Source**: On-premises SQL Server (AdventureWorks DB).
  2. **Destination**: **Bronze Data Lake** (Raw Data Storage).
  
- **Transfer Data**: Use **Copy Activity** to move data from SQL Server to **Bronze Layer** in **Azure Data Lake Storage (ADLS)**.

---

## Step 5: Mount the Data Lake in Databricks 📁🔗

- **Mount the Bronze Data Lake** in Azure Databricks:

```python
configs = {
    "fs.azure.account.key.<your-storage-account-name>.dfs.core.windows.net": "<access-key>"
}
dbutils.fs.mount(
    source = "abfss://bronze@<your-storage-account-name>.dfs.core.windows.net/",
    mount_point = "/mnt/bronze",
    extra_configs = configs
)

---

## Step 6: Transform Data from Bronze to Silver 🥈🔄

- **Create a Databricks Notebook** to transform the Bronze data:
  - **Convert DateTime to Date** format for better readability.

```python
from pyspark.sql.functions import to_date

df_bronze = spark.read.format("delta").load("/mnt/bronze/adventureworks")
df_silver = df_bronze.withColumn("OrderDate", to_date("OrderDate", "yyyy-MM-dd"))
df_silver.write.format("delta").mode("overwrite").save("/mnt/silver/adventureworks")

Save the Transformed Data to the Silver Container in the Data Lake.

---


## Step 7: Transform Data from Silver to Gold 🥇✨

- **Create a Databricks Notebook** to transform the Silver data:
  - **Rename Columns**: Change column names to follow a consistent naming convention (e.g., `FirstName` to `First_Name`).

```python
# Load the Silver data
df_silver = spark.read.format("delta").load("/mnt/silver/adventureworks")

# Rename columns for consistency
df_gold = df_silver.withColumnRenamed("FirstName", "First_Name") \
                   .withColumnRenamed("LastName", "Last_Name")

# Save the Transformed Data to the Gold Container in the Data Lake
df_gold.write.format("delta").mode("overwrite").save("/mnt/gold/adventureworks")


---

## Step 8: Create a View in Azure Synapse and Connect to Gold DB 🛠️

- **Connect Azure Synapse** to the **Gold Data Layer** stored in the Azure Data Lake.
- **Create a View** that provides an aggregated or curated form of the gold data to simplify querying.

   Here's an example SQL query to create a view in Synapse:

   ```sql
   CREATE OR ALTER VIEW GoldView AS
   SELECT * FROM OPENROWSET(
       BULK 'https://<your-storage-account-name>.dfs.core.windows.net/gold/adventureworks',
       FORMAT = 'DELTA'
   ) AS result;

---

## Step 9: Automate Data Pipeline Execution with Azure Data Factory 📅🔄

---

## Step 10: Visualize the Data in Power BI 📊✨


