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
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import accuracy_score, confusion_matrix, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OrdinalEncoder, StandardScaler
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
all_features = numeric_features + categorical_features
TARGET = "will_shift_3plus_minutes"

# Convert to pandas for sklearn
train_pdf = model_df.select(all_features + [TARGET]).toPandas()
train_pdf[TARGET] = train_pdf[TARGET].astype(int)

X = train_pdf[all_features]
y = train_pdf[TARGET]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42)

preprocessor = ColumnTransformer([
    ("num", StandardScaler(), numeric_features),
    ("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), categorical_features),
])

clf = GradientBoostingClassifier(
    n_estimators=200, max_depth=4, learning_rate=0.1, subsample=0.8, random_state=42,
)

X_train_t = preprocessor.fit_transform(X_train)
X_test_t = preprocessor.transform(X_test)

clf.fit(X_train_t, y_train)
y_pred = clf.predict(X_test_t)
y_prob = clf.predict_proba(X_test_t)[:, 1]

auc = roc_auc_score(y_test, y_prob)
accuracy = accuracy_score(y_test, y_pred)

print(f"AUC: {auc:.3f}")
print(f"Accuracy: {accuracy:.3f}")

# COMMAND ----------

displayHTML(
    f"""
    <div style="display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin:12px 0 18px 0;">
      <div style="border:1px solid #e5e7eb;border-radius:8px;padding:16px;background:#fff;">
        <div style="font-size:13px;color:#6b7280;">Prediction Target</div>
        <div style="font-size:23px;font-weight:800;color:#111827;margin-top:8px;">ETA shifts \u2265 {shift_threshold_minutes:.0f} min</div>
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

cm = confusion_matrix(y_test, y_pred)

fig, ax = plt.subplots(figsize=(6.5, 5.5), dpi=140)
im = ax.imshow(cm, cmap="Blues")
ax.set_xticks([0, 1])
ax.set_xticklabels(["Pred stable", "Pred shaky"])
ax.set_yticks([0, 1])
ax.set_yticklabels(["Actually stable", "Actually shaky"])
ax.set_title("ETA Trust Model Confusion Matrix", fontsize=16, fontweight="bold", loc="left", pad=14)
for i in range(2):
    for j in range(2):
        ax.text(j, i, f"{cm[i, j]:,}", ha="center", va="center", fontsize=14, fontweight="bold")
for spine in ax.spines.values():
    spine.set_visible(False)
fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
plt.tight_layout()
plt.show()

# COMMAND ----------

feature_names = numeric_features + categorical_features
importances = clf.feature_importances_
importance_pdf = (
    pd.DataFrame({"feature": feature_names, "importance": importances})
    .sort_values("importance", ascending=False)
    .head(12)
    .sort_values("importance", ascending=True)
)

fig, ax = plt.subplots(figsize=(11, 5.5), dpi=140)
ax.barh(importance_pdf["feature"], importance_pdf["importance"], color="#2563eb")
ax.set_title("What Drives ETA Trust Risk?", fontsize=18, fontweight="bold", loc="left", pad=14)
ax.text(
    0, 1.04,
    "Feature importance from the Gradient Boosted Trees model.",
    transform=ax.transAxes, fontsize=11, color="#6b7280",
)
ax.tick_params(labelsize=9, colors="#374151", length=0)
for spine in ax.spines.values():
    spine.set_visible(False)
ax.grid(axis="x", alpha=0.30, color="#e5e7eb")
plt.tight_layout()
plt.show()

# COMMAND ----------

# Score the test set with trust scores
scored_pdf = X_test.copy()
scored_pdf["shaky_eta_probability"] = y_prob
scored_pdf["eta_trust_score"] = (100 - y_prob * 100).round(0).astype(int)
scored_pdf["prediction"] = y_pred
scored_pdf["actual"] = y_test.values

route_model_pdf = (
    scored_pdf.groupby("route_short_name")
    .agg(
        scored_snapshots=("shaky_eta_probability", "count"),
        avg_shaky_eta_probability=("shaky_eta_probability", "mean"),
        avg_eta_trust_score=("eta_trust_score", "mean"),
    )
    .round(3)
    .sort_values("avg_shaky_eta_probability", ascending=False)
    .reset_index()
)

display(spark.createDataFrame(route_model_pdf))

# COMMAND ----------

if route_model_pdf.empty:
    print("No route-level model scores available.")
else:
    plot_pdf = route_model_pdf.head(15).sort_values("avg_shaky_eta_probability", ascending=True)
    fig, ax = plt.subplots(figsize=(12, max(4, len(plot_pdf) * 0.48)), dpi=140)
    colors = [
        "#16a34a" if score >= 75 else "#f59e0b" if score >= 50 else "#ef4444"
        for score in plot_pdf["avg_eta_trust_score"]
    ]
    ax.barh(plot_pdf["route_short_name"], plot_pdf["avg_eta_trust_score"], color=colors)
    for patch, prob in zip(ax.patches, plot_pdf["avg_shaky_eta_probability"]):
        width = patch.get_width()
        ax.text(width + 1, patch.get_y() + patch.get_height() / 2,
                f"{width:.0f}/100  \u00b7  shaky prob {prob:.2f}", va="center", fontsize=9)
    ax.set_xlim(0, 110)
    ax.set_title("Model-Based ETA Trust Score by Route", fontsize=18, fontweight="bold", loc="left", pad=14)
    ax.text(
        0, 1.04,
        "Higher score means the model expects less future ETA movement from the current snapshot.",
        transform=ax.transAxes, fontsize=11, color="#6b7280",
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
