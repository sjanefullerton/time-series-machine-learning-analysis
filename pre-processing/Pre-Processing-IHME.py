from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Initialize SparkSession
spark = SparkSession.builder.appName("AgeGenderIHME").getOrCreate()

# Load the CSV file
df = spark.read.csv("IHME/SPECIFIC_age_gender_states_GBD_1990to2021.csv", header=True, inferSchema=True)

for column in df.columns:
    if "location_name" in column:
        df = df.withColumnRenamed(column, "state")

# Dictionary of full state names to their abbreviations
state_abbreviations = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR", "California": "CA",
    "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE", "Florida": "FL", "Georgia": "GA",
    "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
    "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS", "Missouri": "MO",
    "Montana": "MT", "Nebraska": "NE", "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ",
    "New Mexico": "NM", "New York": "NY", "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH",
    "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
    "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT", "Vermont": "VT",
    "Virginia": "VA", "Washington": "WA", "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY"
}

# UDF to replace state names with abbreviations
def replace_state_with_abbreviation(state_name):
    return state_abbreviations.get(state_name, state_name)

# Register UDF
replace_state_udf = udf(replace_state_with_abbreviation, StringType())

# Apply the UDF to replace state names with abbreviations
df_with_abbr = df.withColumn("state", replace_state_udf(df["state"]))

# Save the result to a new CSV file
# Save the result to a single CSV file
df_with_abbr.coalesce(1).write.option("header", "true").csv("IHME_PreProcessed.csv") #short_states_age_gender_IHME
