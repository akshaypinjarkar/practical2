import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# Initialize Spark session
spark = SparkSession.builder.appName("ExcelToDataFrame").getOrCreate()

# Replace 'your_excel_file.xlsx' with the path to your actual Excel file
excel_file_path = "C:\\Users\\akshay pinjarkar\\Desktop\\pydq1.xlsx"

# Use pandas to read the Excel file
df = pd.read_excel(excel_file_path, sheet_name="Sheet1")  # Adjust 'Sheet1' to your actual sheet name if needed
validity_df = df[df['Dimension'] == 'validity']
Result1 = validity_df.groupby(['Field Name', 'Table Name'])['Result'].min()

# print(Result1)
def math_score(Result):
    if Result <= 1  and Result >= 0.98:
        return 0
    elif Result < 0.98 and Result >= 0.95:
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

# Apply the custom functions to each row to calculate the scores
df['score1'] = df.apply(lambda row: math_score(row['Result']) if row['Dimension'] == 'completness'
else (sci_score(row['Result']) if row['Dimension'] == 'conformity' else his_score(row['Result'])), axis=1)

# Group by 'rollno' and 'fname', then sum the 'score' for each group
df['score1'] = df.groupby(['Table Name', 'Field Name'])['score1'].transform('sum')


print(df)
