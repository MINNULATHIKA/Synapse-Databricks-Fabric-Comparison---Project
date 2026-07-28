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

from pyspark.sql import functions as F
from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CLEAN USERS CSV DATA
users_df = spark.table("bronze_users")

users_clean_df = (
    users_df
    .withColumn(
        "per_capita_income",
        F.regexp_replace("per_capita_income", "[$,]", "").cast("double")
    )
    .withColumn(
        "yearly_income",
        F.regexp_replace("yearly_income", "[$,]", "").cast("double")
    )
    .withColumn(
        "total_debt",
        F.regexp_replace("total_debt", "[$,]", "").cast("double")
    )
    .withColumn("id", F.col("id").cast("long"))
    .withColumn("current_age", F.col("current_age").cast("int"))
    .withColumn("retirement_age", F.col("retirement_age").cast("int"))
    .withColumn("credit_score", F.col("credit_score").cast("int"))
    .withColumn("num_credit_cards", F.col("num_credit_cards").cast("int"))
    .dropDuplicates(["id"])
)

users_clean_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_users")


    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# ==========================================
# CLEAN CARD CSV DATA
# ==========================================

# Read Bronze Delta table
cards_df = spark.table("bronze_cards")

cards_clean_df = (
    cards_df

    # -----------------------------
    # CAST NUMERIC COLUMNS
    # -----------------------------
    .withColumn(
        "id",
        F.col("id").cast("long")
    )
    .withColumn(
        "client_id",
        F.col("client_id").cast("long")
    )
    .withColumn(
        "credit_limit",
        F.regexp_replace(
            F.col("credit_limit"),
            "[$,]",
            ""
        ).cast("double")
    )
    .withColumn(
        "num_cards_issued",
        F.col("num_cards_issued").cast("int")
    )

    # -----------------------------
    # CONVERT DATE COLUMNS
    # Example: 12/2022 -> 2022-12-01
    # -----------------------------
    .withColumn(
        "acct_open_date",
        F.to_date(
            F.col("acct_open_date"),
            "MM/yyyy"
        )
    )
    .withColumn(
        "expires",
        F.to_date(
            F.col("expires"),
            "MM/yyyy"
        )
    )

    # -----------------------------
    # CONVERT YES / NO TO BOOLEAN
    # -----------------------------
    .withColumn(
        "has_chip",
        F.when(
            F.upper(F.trim(F.col("has_chip"))) == "YES",
            True
        )
        .when(
            F.upper(F.trim(F.col("has_chip"))) == "NO",
            False
        )
        .otherwise(None)
    )

    .withColumn(
        "card_on_dark_web",
        F.when(
            F.upper(F.trim(F.col("card_on_dark_web"))) == "YES",
            True
        )
        .when(
            F.upper(F.trim(F.col("card_on_dark_web"))) == "NO",
            False
        )
        .otherwise(None)
    )

    # -----------------------------
    # REMOVE DUPLICATE CARDS
    # -----------------------------
    .dropDuplicates(["id"])
)

# ==========================================
# SAVE TO SILVER DELTA TABLE
# ==========================================

cards_clean_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_cards")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean Transactions Data

transactions_df = spark.table("bronze_transactions")

transactions_clean_df = (
    transactions_df
    .withColumn("id", F.col("id").cast("long"))
    .withColumn("client_id", F.col("client_id").cast("long"))
    .withColumn("card_id", F.col("card_id").cast("long"))
    .withColumn("merchant_id", F.col("merchant_id").cast("long"))
    .withColumn("mcc", F.col("mcc").cast("int"))
    .withColumn(
        "amount",
        F.regexp_replace("amount", "[$,]", "").cast("double")
    )
    .withColumn(
        "transaction_timestamp",
        F.to_timestamp("date", "MM-dd-yyyy HH:mm")
    )
    .withColumn(
        "transaction_date",
        F.to_date("transaction_timestamp")
    )
    .withColumn(
        "transaction_year",
        F.year("transaction_timestamp")
    )
    .withColumn(
        "transaction_month",
        F.month("transaction_timestamp")
    )
    .withColumn(
        "transaction_month_name",
        F.date_format("transaction_timestamp", "MMMM")
    )
    .withColumn(
        "transaction_type",
        F.when(F.col("amount") < 0, "Credit")
         .when(F.col("amount") > 0, "Debit")
         .otherwise("Zero")
    )
    .withColumn(
        "transaction_amount",
        F.abs(F.col("amount"))
    )
    .drop("date")
    .dropDuplicates(["id"])
)

# save in parquent format in silver
transactions_clean_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_transactions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# ==========================================
# READ BRONZE MCC TABLE
# ==========================================

mcc_bronze_df = spark.table("bronze_mcc_codes")

# ==========================================
# CLEAN MCC CODES
# ==========================================

mcc_clean_df = (
    mcc_bronze_df
    .select(
        F.col("mcc").cast("int").alias("mcc"),
        F.trim(F.col("category").cast("string")).alias("category")
    )
    .filter(
        F.col("mcc").isNotNull() &
        F.col("category").isNotNull()
    )
    .dropDuplicates(["mcc"])
)

# Check Silver data
mcc_clean_df.show(10, truncate=False)

# ==========================================
# WRITE TO SILVER DELTA TABLE
# ==========================================

mcc_clean_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_mcc_codes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Read Bronze Delta table
fraud_bronze_df = spark.table("train_fraud_labels")

# Check Bronze data
display(fraud_bronze_df)

fraud_labels_clean_df = (
    fraud_bronze_df
    .select(
        F.col("transaction_id").cast("long").alias("transaction_id"),
        F.col("fraud_label").cast("string").alias("fraud_label")
    )
    .withColumn(
        "is_fraud",
        F.when(F.col("fraud_label") == "Yes", 1)
         .otherwise(0)
    )
    .dropDuplicates(["transaction_id"])
)



fraud_labels_clean_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_fraud_labels")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
