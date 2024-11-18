from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, input_file_name
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder.appName("StandardizeData").getOrCreate()

# Define file path and read all CSV files
file_path = "SPD_SAMHSA/*.csv"
df = spark.read.option("header", "true").csv(file_path)

# Extract the year from the filename and add it as a new column
df = df.withColumn("filename", input_file_name())
df = df.withColumn("year", regexp_extract("filename", r".*?(\d{4}).*", 1))

# Standardize columns containing 'SERIOUS PSYCHOLOGICAL DISTRESS' to 'SPD'
for column in df.columns:
    if "SERIOUS PSYCHOLOGICAL DISTRESS" in column:
        df = df.withColumnRenamed(column, "SPD")
    if "GENDER" in column:
        df = df.withColumnRenamed(column, "GENDER")

# Drop the filename column since no longer needed
df = df.drop("filename")

# Convert to Pandas DataFrame for filtering and sorting
df_pandas = df.toPandas()

# Filter rows where 'SPD' column starts with '1' (assuming SPD contains string values)
# 1 because this means 'Yes' to SPD
df_pandas = df_pandas[df_pandas["SPD"].str.startswith("1")]

# Ensure the 'year' column is numeric and sort by year
df_pandas["year"] = pd.to_numeric(df_pandas["year"])
df_pandas = df_pandas.sort_values(by="year")

# Select only the desired columns
columns_to_keep = [
    'SPD', 'GENDER', 'Total %', 'Total % SE',
    'Total % CI (lower)', 'Total % CI (upper)', 'Weighted Count',
    'Count', 'Count SE', 'year'
]

combined_SPD_data = df_pandas[columns_to_keep]

# Export the filtered data to a CSV file
combined_SPD_data.to_csv("years2004to2022_SPD_data_filtered.csv", index=False, header=True)

# print(df_pandas)
# Summary Statistics
# print(combined_SPD_data.describe)



