import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# Initialize Spark session
spark = SparkSession.builder.appName("ExcelToDataFrame").getOrCreate()

# Replace 'your_excel_file.xlsx' with the path to your actual Excel file
excel_file_path = "C:\\Users\\akshay pinjarkar\\Desktop\\pydq.xlsx"

# Use pandas to read the Excel file
df = pd.read_excel(excel_file_path, sheet_name="xyz")  # Adjust 'Sheet1' to your actual sheet name if needed

print(df)
print(df.dtypes)


def math_score(mark):
    if mark <= 100  and mark >= 98:
        return 0
    elif mark < 98 and mark >= 95:
        return 10
    elif mark < 95:
        return 15
    else:
        return 0

def sci_score(mark):
    if mark == 100:
        return 0
    elif mark < 100:
        return 5
    else:
        return 0

def his_score(mark):
    if mark == 100:
        return 0
    elif mark < 100:
        return 30
    else:
        return 0

# Apply the custom functions to each row to calculate the scores
df['score'] = df.apply(lambda row: math_score(row['mark']) if row['subject'] == 'math' else (sci_score(row['mark'])
if row['subject'] == 'sci' else his_score(row['mark'])), axis=1)

# Group by 'rollno' and 'fname', then sum the 'score' for each group
df['score'] = df.groupby(['rollno', 'fname'])['score'].transform('sum')

print(df)

# # Calculate the "score" column using the defined function
# pivot_df["score"] = pivot_df.apply(calculate_score, axis=1)
#
# # Display the resulting DataFrame
# print(pivot_df)
