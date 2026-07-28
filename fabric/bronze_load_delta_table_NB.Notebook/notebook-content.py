# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "40805afb-19b5-4155-93e6-39884b85bda4",
# META       "default_lakehouse_name": "project4_bank_credit_lakehouse",
# META       "default_lakehouse_workspace_id": "b78da15c-ce13-4765-9fa3-4b3bafabcbd6",
# META       "known_lakehouses": [
# META         {
# META           "id": "40805afb-19b5-4155-93e6-39884b85bda4"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Create Bronze Delta table for CSV files

from pyspark.sql import functions as F

users_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("Files/users_data.csv")

users_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_users")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# cards data
cards_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("Files/RawData/cards_data.csv")

cards_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_cards")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transactions
transactions_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("Files/transactions_data.csv")

transactions_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_transactions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType


# Read JSON
mcc_raw_df = spark.read \
    .option("multiLine", "true") \
    .json("Files/mcc_codes.json")


# Convert struct to map
mcc_map_df = (
    mcc_raw_df
    .select(
        F.from_json(
            F.to_json(F.struct("*")),
            MapType(StringType(), StringType())
        ).alias("mcc_map")
    )
)


# Explode map into rows
mcc_bronze_df = (
    mcc_map_df
    .select(
        F.explode("mcc_map")
        .alias("mcc", "category")
    )
)


# Cast columns
mcc_bronze_df = (
    mcc_bronze_df
    .withColumn("mcc", F.col("mcc").cast("int"))
    .withColumn("category", F.col("category").cast("string"))
)


display(mcc_bronze_df)

mcc_bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_mcc_codes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, MapType, StringType

fraud_path = "Files/train_fraud_labels.json"   # relative to the attached lakehouse

fraud_schema = StructType([
    StructField("target", MapType(StringType(), StringType()), True)
])

df_raw = (
    spark.read
    .option("wholetext", "true")      # important: file is one giant JSON object
    .text(fraud_path)
)

df_fraud_final = (
    df_raw
    .select(F.from_json(F.col("value"), fraud_schema).alias("data"))
    .select(F.explode(F.col("data.target")).alias("transaction_id", "fraud_label"))
    .withColumn("transaction_id", F.col("transaction_id").cast("long"))
)

(
    df_fraud_final
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("train_fraud_labels")   # no 3-part catalog name in Fabric
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
