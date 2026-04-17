# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# MAGIC %md
# MAGIC # 05 - Model ETA Trust
# MAGIC
# MAGIC This model does **not** predict actual bus arrival/departure.
# MAGIC
# MAGIC It predicts whether the current ETA is likely to keep moving before the trip leaves the feed.
# MAGIC That is the product idea: turn real-time ETA snapshots into a student-facing confidence signal.

# COMMAND ----------

from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql.functions import (
    abs as spark_abs,
    col,
    count as spark_count,
    dayofweek,
    desc,
    expr,
    hour,
    last,
    row_number,
    unix_timestamp,
    when,
)
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "cumtd_eta_drift")
dbutils.widgets.text("shift_threshold_minutes", "3")
dbutils.widgets.text("min_training_rows", "50")

catalog = dbutils.widgets.get("catalog").strip()
schema_name = dbutils.widgets.get("schema").strip()
shift_threshold_minutes = float(dbutils.widgets.get("shift_threshold_minutes"))
min_training_rows = int(dbutils.widgets.get("min_training_rows"))

database_name = f"{catalog}.{schema_name}" if catalog else schema_name
raw_table = f"{database_name}.raw_departure_snapshots"

print(f"Reading snapshots from {raw_table}")

# COMMAND ----------

raw_df = spark.table(raw_table)

required_columns = [
    "ingestion_timestamp",
    "stop_id",
    "trip_id",
    "route_short_name",
    "scheduledDeparture",
    "estimatedDeparture",
    "minutesTillDeparture",
    "isRealTime",
]
missing_columns = [name for name in required_columns if name not in raw_df.columns]
if missing_columns:
    raise ValueError(f"Raw table is missing required columns: {missing_columns}")

# COMMAND ----------

group_w = Window.partitionBy("stop_id", "trip_id", "route_short_name")
ordered_w = group_w.orderBy("ingestion_timestamp")
full_ordered_w = ordered_w.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

model_df = (
    raw_df.where(col("stop_id").isNotNull())
    .where(col("trip_id").isNotNull())
    .where(col("route_short_name").isNotNull())
    .where(col("scheduledDeparture").isNotNull())
    .where(col("estimatedDeparture").isNotNull())
    .withColumn("snapshot_count", spark_count("*").over(group_w))
    .withColumn("snapshot_number", row_number().over(ordered_w))
    .withColumn("final_observed_estimated_departure", last("estimatedDeparture", ignorenulls=True).over(full_ordered_w))
    .withColumn(
        "current_offset_minutes",
        (unix_timestamp("estimatedDeparture") - unix_timestamp("scheduledDeparture")) / 60.0,
    )
    .withColumn(
        "time_to_departure_minutes",
        (unix_timestamp("estimatedDeparture") - unix_timestamp("ingestion_timestamp")) / 60.0,
    )
    .withColumn(
        "remaining_eta_shift_minutes",
        spark_abs(
            (unix_timestamp("final_observed_estimated_departure") - unix_timestamp("estimatedDeparture")) / 60.0
        ),
    )
    .withColumn(
        "will_shift_3plus_minutes",
        when(col("remaining_eta_shift_minutes") >= shift_threshold_minutes, 1.0).otherwise(0.0),
    )
    .withColumn("hour_of_day", hour("ingestion_timestamp").cast("double"))
    .withColumn("day_of_week", dayofweek("ingestion_timestamp").cast("double"))
    .withColumn("minutesTillDeparture", col("minutesTillDeparture").cast("double"))
    .withColumn("isRealTimeFlag", when(col("isRealTime") == True, 1.0).otherwise(0.0))
    .where(col("snapshot_count") >= 3)
    .where(col("snapshot_number") < col("snapshot_count"))
    .where(col("time_to_departure_minutes").isNotNull())
    .where(col("current_offset_minutes").isNotNull())
    .where(col("minutesTillDeparture").isNotNull())
)

training_rows = model_df.count()
positive_rows = model_df.where(col("will_shift_3plus_minutes") == 1.0).count()

print(f"Training rows: {training_rows:,}")
print(f"Positive labels: {positive_rows:,}")

if training_rows < min_training_rows:
    raise ValueError(
        f"Only {training_rows:,} training rows found. Collect more repeated snapshots or lower min_training_rows."
    )

if positive_rows == 0 or positive_rows == training_rows:
    raise ValueError(
        "The label has only one class. Collect more data or adjust shift_threshold_minutes."
    )

# COMMAND ----------

categorical_features = ["route_short_name", "stop_id"]
numeric_features = [
    "time_to_departure_minutes",
    "current_offset_minutes",
    "minutesTillDeparture",
    "isRealTimeFlag",
    "hour_of_day",
    "day_of_week",
    "snapshot_number",
]

indexers = [
    StringIndexer(inputCol=feature, outputCol=f"{feature}_idx", handleInvalid="keep")
    for feature in categorical_features
]
encoder = OneHotEncoder(
    inputCols=[f"{feature}_idx" for feature in categorical_features],
    outputCols=[f"{feature}_ohe" for feature in categorical_features],
)
assembler = VectorAssembler(
    inputCols=numeric_features + [f"{feature}_ohe" for feature in categorical_features],
    outputCol="features",
)
classifier = GBTClassifier(
    labelCol="will_shift_3plus_minutes",
    featuresCol="features",
    maxIter=40,
    maxDepth=4,
    seed=42,
)

pipeline = Pipeline(stages=indexers + [encoder, assembler, classifier])
train_df, test_df = model_df.randomSplit([0.75, 0.25], seed=42)

model = pipeline.fit(train_df)
predictions = model.transform(test_df).cache()

auc = BinaryClassificationEvaluator(
    labelCol="will_shift_3plus_minutes",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC",
).evaluate(predictions)
accuracy = MulticlassClassificationEvaluator(
    labelCol="will_shift_3plus_minutes",
    predictionCol="prediction",
    metricName="accuracy",
).evaluate(predictions)

print(f"AUC: {auc:.3f}")
print(f"Accuracy: {accuracy:.3f}")

# COMMAND ----------

displayHTML(
    f"""
    <div style="display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin:12px 0 18px 0;">
      <div style="border:1px solid #e5e7eb;border-radius:8px;padding:16px;background:#fff;">
        <div style="font-size:13px;color:#6b7280;">Prediction Target</div>
        <div style="font-size:23px;font-weight:800;color:#111827;margin-top:8px;">ETA shifts ≥ {shift_threshold_minutes:.0f} min</div>
        <div style="font-size:12px;color:#6b7280;margin-top:8px;">before the final observed ETA</div>
      </div>
      <div style="border:1px solid #e5e7eb;border-radius:8px;padding:16px;background:#fff;">
        <div style="font-size:13px;color:#6b7280;">Training Rows</div>
        <div style="font-size:30px;font-weight:800;color:#111827;margin-top:8px;">{training_rows:,}</div>
        <div style="font-size:12px;color:#6b7280;margin-top:8px;">snapshot-level examples</div>
      </div>
      <div style="border:1px solid #e5e7eb;border-radius:8px;padding:16px;background:#fff;">
        <div style="font-size:13px;color:#6b7280;">AUC</div>
        <div style="font-size:30px;font-weight:800;color:#111827;margin-top:8px;">{auc:.3f}</div>
        <div style="font-size:12px;color:#6b7280;margin-top:8px;">ranking ETA risk</div>
      </div>
      <div style="border:1px solid #e5e7eb;border-radius:8px;padding:16px;background:#fff;">
        <div style="font-size:13px;color:#6b7280;">Accuracy</div>
        <div style="font-size:30px;font-weight:800;color:#111827;margin-top:8px;">{accuracy:.3f}</div>
        <div style="font-size:12px;color:#6b7280;margin-top:8px;">baseline sanity check</div>
      </div>
    </div>
    """
)

# COMMAND ----------

confusion_pdf = (
    predictions.groupBy("will_shift_3plus_minutes", "prediction")
    .agg(spark_count("*").alias("rows"))
    .toPandas()
)

confusion = np.zeros((2, 2), dtype=int)
for _, row in confusion_pdf.iterrows():
    confusion[int(row["will_shift_3plus_minutes"]), int(row["prediction"])] = int(row["rows"])

fig, ax = plt.subplots(figsize=(6.5, 5.5), dpi=140)
im = ax.imshow(confusion, cmap="Blues")
ax.set_xticks([0, 1])
ax.set_xticklabels(["Pred stable", "Pred shaky"])
ax.set_yticks([0, 1])
ax.set_yticklabels(["Actually stable", "Actually shaky"])
ax.set_title("ETA Trust Model Confusion Matrix", fontsize=16, fontweight="bold", loc="left", pad=14)
for i in range(2):
    for j in range(2):
        ax.text(j, i, f"{confusion[i, j]:,}", ha="center", va="center", fontsize=14, fontweight="bold")
for spine in ax.spines.values():
    spine.set_visible(False)
fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
plt.tight_layout()
plt.show()

# COMMAND ----------

gbt_model = model.stages[-1]
attrs = predictions.schema["features"].metadata.get("ml_attr", {}).get("attrs", {})
feature_names = []
for attr_group in attrs.values():
    feature_names.extend([item["name"] for item in attr_group])

importances = gbt_model.featureImportances.toArray()
importance_pdf = (
    pd.DataFrame({"feature": feature_names[: len(importances)], "importance": importances})
    .sort_values("importance", ascending=False)
    .head(12)
    .sort_values("importance", ascending=True)
)

fig, ax = plt.subplots(figsize=(11, 5.5), dpi=140)
ax.barh(importance_pdf["feature"], importance_pdf["importance"], color="#2563eb")
ax.set_title("What Drives ETA Trust Risk?", fontsize=18, fontweight="bold", loc="left", pad=14)
ax.text(
    0,
    1.04,
    "Feature importance from the Gradient Boosted Trees model.",
    transform=ax.transAxes,
    fontsize=11,
    color="#6b7280",
)
ax.tick_params(labelsize=9, colors="#374151", length=0)
for spine in ax.spines.values():
    spine.set_visible(False)
ax.grid(axis="x", alpha=0.30, color="#e5e7eb")
plt.tight_layout()
plt.show()

# COMMAND ----------

scored_df = (
    predictions.withColumn("probability_array", vector_to_array("probability"))
    .withColumn("shaky_eta_probability", col("probability_array")[1])
    .withColumn("eta_trust_score", expr("cast(round(100 - shaky_eta_probability * 100, 0) as int)"))
)

route_model_summary = (
    scored_df.groupBy("route_short_name")
    .agg(
        spark_count("*").alias("scored_snapshots"),
        expr("round(avg(shaky_eta_probability), 3)").alias("avg_shaky_eta_probability"),
        expr("round(avg(eta_trust_score), 1)").alias("avg_eta_trust_score"),
        expr("round(avg(remaining_eta_shift_minutes), 2)").alias("avg_future_eta_shift_minutes"),
    )
    .orderBy(desc("avg_shaky_eta_probability"))
)

display(route_model_summary)

# COMMAND ----------

route_model_pdf = route_model_summary.limit(15).toPandas()
if route_model_pdf.empty:
    print("No route-level model scores available.")
else:
    route_model_pdf = route_model_pdf.sort_values("avg_shaky_eta_probability", ascending=True)
    fig, ax = plt.subplots(figsize=(12, max(4, len(route_model_pdf) * 0.48)), dpi=140)
    colors = [
        "#16a34a" if score >= 75 else "#f59e0b" if score >= 50 else "#ef4444"
        for score in route_model_pdf["avg_eta_trust_score"]
    ]
    ax.barh(route_model_pdf["route_short_name"], route_model_pdf["avg_eta_trust_score"], color=colors)
    for patch, prob in zip(ax.patches, route_model_pdf["avg_shaky_eta_probability"]):
        width = patch.get_width()
        ax.text(width + 1, patch.get_y() + patch.get_height() / 2, f"{width:.0f}/100  ·  shaky prob {prob:.2f}", va="center", fontsize=9)
    ax.set_xlim(0, 110)
    ax.set_title("Model-Based ETA Trust Score by Route", fontsize=18, fontweight="bold", loc="left", pad=14)
    ax.text(
        0,
        1.04,
        "Higher score means the model expects less future ETA movement from the current snapshot.",
        transform=ax.transAxes,
        fontsize=11,
        color="#6b7280",
    )
    ax.tick_params(labelsize=9, colors="#374151", length=0)
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.grid(axis="x", alpha=0.30, color="#e5e7eb")
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product Interpretation
# MAGIC
# MAGIC This model is not trying to replace MTD.
# MAGIC
# MAGIC It adds a confidence layer:
# MAGIC
# MAGIC - **High trust:** the current ETA is unlikely to keep moving by 3+ minutes.
# MAGIC - **Medium trust:** keep checking.
# MAGIC - **Low trust:** add buffer or consider another route.
# MAGIC
# MAGIC That is the student-facing app/dashboard direction.
