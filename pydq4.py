import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Assuming you already have a DataFrame named 'df'
# If not, you can create one with your data

# Replace 'your_excel_file.xlsx' with the path to your actual Excel file
excel_file_path = "C:\\Users\\akshay pinjarkar\\Desktop\\pydq2.xlsx"

# Use pandas to read the Excel file
df = pd.read_excel(excel_file_path, sheet_name="Sheet1")
subject_min_marks = df.groupby(['rollno', 'fname', 'subject'])['mark'].min().reset_index()


# Define a function to calculate the 'xyz' value based on the specified conditions
result = df.groupby(['rollno', 'subject'])['mark'].min().reset_index()


# Define the custom functions to calculate scores based on the 'Result' value
def math_score(Result):
    if 1 >= Result >= 0.98:
        return 0
    elif 0.98 > Result >= 0.95:
        return 10
    elif Result < 0.95:
        return 15
    else:
        return 0

def sci_score(Result):
    if Result == 1:
        return 0
    elif Result < 1:
        return 5
    else:
        return 0

def his_score(Result):
    if Result == 1:
        return 0
    elif Result < 1:
        return 30
    else:
        return 0

df['score2'] = df.apply(lambda row: math_score(row['mark']) if row['subject'] == 'math'
else (sci_score(row['mark']) if row['sub'] == 'sci' else his_score(row['mark'])), axis=1)

# Group by 'rollno' and 'fname', then sum the 'score' for each group
df['score2'] = df.groupby(['rollno', 'fname'])['score2'].transform('sum')



















