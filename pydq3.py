# import pandas as pd
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
#
# # Assuming you already have a DataFrame named 'df'
# # If not, you can create one with your data
#
# # Replace 'your_excel_file.xlsx' with the path to your actual Excel file
# excel_file_path = "C:\\Users\\akshay pinjarkar\\Desktop\\pydq1.xlsx"
#
# # Use pandas to read the Excel file
# df = pd.read_excel(excel_file_path, sheet_name="Sheet1")  # Adjust 'Sheet1' to your actual sheet name if needed
#
#
#
# # Filter only the rows with Dimension as 'validity'
# validity_df = df[df['Dimension'] == 'validity']
#
# # Find the validity with the minimum Result for each group (Table Name, Field Name)
# validity_df = validity_df.loc[validity_df.groupby(['Table Name', 'Field Name'])['Result'].idxmin()]
#
#
# # Define the custom functions to calculate scores based on the 'Result' value
# def math_score(Result):
#     if 1 >= Result >= 0.98:
#         return 0
#     elif 0.98 > Result >= 0.95:
#         return 10
#     elif Result < 0.95:
#         return 15
#     else:
#         return 0
#
# def sci_score(Result):
#     if Result == 1:
#         return 0
#     elif Result < 1:
#         return 5
#     else:
#         return 0
#
# def his_score(Result):
#     if Result == 1:
#         return 0
#     elif Result < 1:
#         return 30
#     else:
#         return 0
#
# df['score2'] = df.apply(lambda row: math_score(row['Result']) if row['Dimension'] == 'completness'
# else (sci_score(row['Result']) if row['Dimension'] == 'conformity' else his_score(row['Result'])), axis=1)
#
# # Group by 'rollno' and 'fname', then sum the 'score' for each group
# df['score2'] = df.groupby(['Table Name', 'Field Name'])['score2'].transform('sum')
#
# validity_df['score1'] = validity_df.apply(
#     lambda row: math_score(row['Result']) if row['Dimension'] == 'completness'
#     else (sci_score(row['Result']) if row['Dimension'] == 'conformity' else his_score(row['Result'])), axis=1
# )
#
# # Group by 'Table Name' and 'Field Name', then sum the 'score1' for each group
# validity_df['score1'] = validity_df.groupby(['Table Name', 'Field Name'])['score1'].transform('sum')
#
#
# # Merge the calculated scores back to the original DataFrame
# df = pd.merge(df, validity_df[['Field Name', 'Table Name', 'score1']], on=['Field Name', 'Table Name'], how='left')
# # print(df)
# # Fill missing scores with 0
# df['score1'] = df['score1'].fillna(df['score2'])
#
# df3 = df.head(35)
# print(df3)
# # print(df3.dtypes)



To apply a condition on the "score2" column and only consider the corresponding "score1" value when "score2" is greater than 50 using Python and pandas, you can use the following code:

python
Copy code
import pandas as pd

# Assuming you have your data in a pandas DataFrame, you can create it like this:
data = {
    "score1": [15, 15, 0, 0, 0, 30, 30, 30, 30],
    "score2": [15, 15, 0, 0, 0, 150, 150, 90, 90]
}

df = pd.DataFrame(data)

# Apply the condition and create a new column "filtered_score1" with the selected values
df["filtered_score1"] = df.apply(lambda row: row["score1"] if row["score2"] > 50 else None, axis=1)

print(df)