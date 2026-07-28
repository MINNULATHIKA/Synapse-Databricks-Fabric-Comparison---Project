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

# Customer_segmentation 

users_df = spark.table("silver_users")

# Adding debt-to-income ratio
users_df = (
    users_df
    .withColumn(
        "debt_to_income_ratio",
        F.when(
            F.col("yearly_income") > 0,
            F.col("total_debt") / F.col("yearly_income")
        ).otherwise(None)
    )
)

customer_segmentation_df = (
    users_df
    .withColumn(
        "customer_segment",
        F.when(
            (F.col("credit_score") >= 750) &
            (F.col("yearly_income") >= 75000) &
            (F.col("debt_to_income_ratio") < 0.40),
            "High-Value"
        )
        .when(
            (F.col("credit_score") < 600) |
            (F.col("debt_to_income_ratio") > 0.75),
            "At-Risk"
        )
        .otherwise("Medium-Value")
    )
)


customer_segmentation_df.write \
    .format("delta")\
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("gold_users")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transaction & Spending Trends
from pyspark.sql import functions as F

transactions_df = spark.table("silver_transactions")

mcc_df = spark.table("silver_mcc_codes")

transactions_enriched_df = (
    transactions_df.alias("t")
    .join(
        mcc_df.alias("m"),
        F.col("t.mcc") == F.col("m.mcc"),
        "left"
    )
    .select(
        F.col("t.id").alias("transaction_id"),
        F.col("t.client_id").alias("customer_id"),
        F.col("t.card_id"),
        F.col("t.transaction_year"),
        F.col("t.transaction_month"),
        F.col("t.transaction_month_name"),
        F.col("t.transaction_amount"),
        F.when(
            F.col("t.amount") < 0,
            F.abs(F.col("t.amount"))
        ).otherwise(F.lit(0)).alias("debited_amount"),

        F.when(
            F.col("t.amount") > 0,
            F.col("t.amount")
        ).otherwise(F.lit(0)).alias("credited_amount"),
        F.col("t.merchant_city"),
        F.col("t.merchant_state"),
        F.col("t.mcc"),
        F.col("m.category")
    )
)


# Write in parquet

transactions_enriched_df.write \
    .format("delta")\
    .mode("overwrite") \
    .option("mergeSchema","true")\
    .saveAsTable("gold_transactions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Credit Utilization
cards_df = spark.table("silver_cards")

customer_credit_df = (
    cards_df
    .groupBy("client_id")
    .agg(
        F.sum("credit_limit").alias("total_credit_limit"),
        F.countDistinct("id").alias("total_cards")
    )
)

# Joining With Users
credit_utilization_df = (
    users_df.alias("u")
    .join(
        customer_credit_df.alias("c"),
        F.col("u.id") == F.col("c.client_id"),
        "left"
    )
    .select(
        F.col("u.id").alias("customer_id"),
        F.col("u.current_age"),
        F.col("u.gender"),
        F.col("u.yearly_income"),
        F.col("u.total_debt"),
        F.col("u.credit_score"),
        F.col("c.total_credit_limit"),
        F.col("c.total_cards")
    )
)

# Calculate Credit utilization
credit_utilization_df = (
    credit_utilization_df
    .withColumn(
        "credit_utilization",
        F.when(
            F.col("total_credit_limit") > 0,
            F.col("total_debt") / F.col("total_credit_limit")
        ).otherwise(None)
    )
)

# Create utilization band
credit_utilization_df = (
    credit_utilization_df
    .withColumn(
        "utilization_band",
        F.when(
            F.col("credit_utilization") < 0.30,
            "Low (<30%)"
        )
        .when(
            F.col("credit_utilization") <= 0.70,
            "Medium (30%-70%)"
        )
        .otherwise(
            "High (>70%)"
        )
    )
)

credit_utilization_df.write \
    .format("delta")\
    .mode("overwrite") \
    .option("mergeSchema","true")\
    .saveAsTable("gold_credit_utilization")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fraud Monitoring
transactions_df = spark.table("silver_transactions")
fraud_df = spark.table("silver_fraud_labels")

mcc_df = spark.table("silver_mcc_codes")

#Colleting Fraud activity details analysing based on Merchant Details 
fraud_transactions_df = (
    transactions_df.alias("t")
    .join(
        fraud_df.alias("f"),
        F.col("t.id") == F.col("f.transaction_id"),
        "left"
    )
)
fraud_monitoring_df = (
    fraud_transactions_df.alias("t")
    .join(
        mcc_df.alias("m"),
        F.col("t.mcc") == F.col("m.mcc"),
        "left"
    )
)


fraud_monitoring_df = (
    fraud_monitoring_df
    .select(
        F.col("t.id").alias("transaction_id"),
        F.col("t.client_id"),
        F.col("t.card_id"),
        F.col("t.transaction_date"),
        F.col("t.transaction_amount"),

        # Debit and Credit Amounts
        F.when(
            F.col("t.amount") < 0,
            F.abs(F.col("t.amount"))
        ).otherwise(F.lit(0)).alias("debited_amount"),

        F.when(
            F.col("t.amount") > 0,
            F.col("t.amount")
        ).otherwise(F.lit(0)).alias("credited_amount"),

        F.col("t.merchant_city"),
        F.col("t.merchant_state"),
        F.col("t.mcc"),
        F.col("m.category"),
        F.col("t.fraud_label"),
        F.col("t.is_fraud")
    )
)

fraud_monitoring_df.write \
    .format("delta")\
    .mode("overwrite") \
    .option("mergeSchema","true")\
    .saveAsTable("gold_fraud_monitoring")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
