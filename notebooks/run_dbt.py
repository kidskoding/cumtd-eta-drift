# Databricks notebook source
import os
import subprocess

subprocess.run(["pip", "install", "dbt-databricks>=1.10,<2", "-q"], check=True)

token = dbutils.secrets.get("cumtd-eta-drift", "databricks-token")
os.environ["DATABRICKS_TOKEN"] = token

cmd = dbutils.widgets.get("dbt_command")
project_dir = "/Workspace/Users/anirudhkonidala@gmail.com/.databricks_bundles_v2/cumtd_eta_drift/prod/files/dbt"

result = subprocess.run(
    f"cd {project_dir} && {cmd}",
    shell=True,
    text=True,
)
if result.returncode != 0:
    raise Exception(f"dbt command failed: {cmd}")
