# import pandas as pd
#
# # Sample data as a list of dictionaries
# data = {
#     'tname': ["xyz", "xyz", "xyz", "xyz1","xyz1","xyz1","xyz1"],
#     'fnme': ["abc", "abc", "abc","abc1","abc1","abc1","abc1"],
#     'dimesnsion': ["completness", "comformity", "validity","completness", "comformity", "validity","validity"],
#     'score': [100, 100, 98, 98, 99, 98,95]
# }
#
#
# # Create a DataFrame from the data
# df = pd.DataFrame(data)
# print(df)
#
# # Function to calculate the rank based on the dimension and score
# def calculate_rank(row):
#     a1=0
#     b1=0
#     c1=0
#     if row["dimesnsion"] == "completness":
#         if row["score"] == 100 :
#             a1 += 0
#             return a1
#         elif row["score"] < 98 :
#             a1 += 10
#             return a1
#         elif row["score"] < 95 :
#             a1 += 15
#             return a1
#     elif row["dimesnsion"] == "comformity":
#         if row["score"] == 100:
#             b1 += 0
#             return b1
#         elif row["score"] < 100:
#             b1 += 5
#             return b1
#     elif row["dimesnsion"] == "validity":
#         min_validity_score = df.loc[df["dimesnsion"] == "validity", "score"].min()
#         if min_validity_score == 100:
#             c1 += 0
#             return c1
#         elif min_validity_score < 100:
#             c1 += 30
#             return c1
#
#     return a1+b1+c1
#
# # Apply the calculate_rank function to the DataFrame to calculate the rank column
# df["rank"] = df.apply(calculate_rank, axis=1)
#
# # Calculate the total rank as the sum of completeness, conformity, and validity ranks
# df["rank"] = df.groupby(["tname", "fnme"])["rank"].transform("sum")
#
# # Print the DataFrame with the desired output
# print(df)

#
# import pandas as pd
#
# # Sample data as a dictionary
# data = {
#     'tname': ['xyz', 'xyz', 'xyz', 'xyz1', 'xyz1', 'xyz1', 'xyz1'],
#     'fnme': ['abc', 'abc', 'abc', 'abc1', 'abc1', 'abc1', 'abc1'],
#     'dimesnsion': ['completness', 'comformity', 'validity', 'completness', 'comformity', 'validity', 'validity'],
#     'score': [100, 100, 98, 98, 99, 98, 95]
# }
#
# # Step 1: Create a DataFrame from the input data
# df = pd.DataFrame(data)
#
# # Step 2: Define functions to calculate the rank based on the conditions provided
# def get_completness_rank(score):
#     if score == 100:
#         return 0
#     elif score < 98:
#         return 5
#     elif score < 95:
#         return 10
#     else:
#         return 0
#
# def get_comformity_rank(score):
#     if score == 100:
#         return 0
#     else:
#         return 5
#
# def get_validity_rank(score):
#     if score == 100:
#         return 0
#     else:
#         return 30
#
# # Step 3: Calculate the rank based on the conditions and add it as a new column
# df['rank'] = df.apply(lambda row: get_completness_rank(row['score']) +
#                                    get_comformity_rank(row['score']) +
#                                    get_validity_rank(row['score']), axis=1)
#
# # Step 4: Group by 'tname' and 'fnme' and get the minimum validity score for each group
# df['rank'] = df.groupby(['tname', 'fnme'])['rank'].transform('min')
#
# # Step 5: Display the final output
# print(df)


# import pandas as pd
#
# # Sample data as a dictionary
# data = {
#     'tname': ['xyz', 'xyz', 'xyz', 'xyz1', 'xyz1', 'xyz1', 'xyz1'],
#     'fnme': ['abc', 'abc', 'abc', 'abc1', 'abc1', 'abc1', 'abc1'],
#     'dimesnsion': ['completness', 'comformity', 'validity', 'completness', 'comformity', 'validity', 'validity'],
#     'score': [100, 100, 98, 98, 99, 98, 95]
# }
#
# # Step 1: Create a DataFrame from the input data
# df = pd.DataFrame(data)
#
# # Step 2: Define functions to calculate the rank based on the conditions provided
# def get_completness_rank(score):
#     if score == 100:
#         return 0
#     elif score < 98:
#         return 5
#     elif score < 95:
#         return 10
#     else:
#         return 0
#
# def get_comformity_rank(score):
#     if score == 100:
#         return 0
#     else:
#         return 5
#
# def get_validity_rank(score):
#     if score == 100:
#         return 0
#     else:
#         return 30
#
# # Step 3: Calculate the rank based on the conditions and add it as a new column
# df['rank'] = df.apply(lambda row: get_completness_rank(row['score']) +
#                                    get_comformity_rank(row['score']) +
#                                    get_validity_rank(row['score']), axis=1)
#
# # Step 4: Group by 'tname' and 'fnme' and get the minimum rank for each group
# df['rank'] = df.groupby(['tname', 'fnme'])['rank'].transform('min')
#
# # Step 5: Display the final output
# print(df)

#
# import pandas as pd
#
# # Sample data as a dictionary
# data = {
#     'tname': ['xyz', 'xyz', 'xyz', 'xyz1', 'xyz1', 'xyz1', 'xyz', 'pqr'],
#     'fnme': ['abc', 'abc', 'abc', 'abc1', 'abc1', 'abc1', 'abc', 'pqrs'],
#     'dimesnsion': ['completness', 'comformity', 'validity', 'completness', 'comformity', 'validity', 'validity', 'validity'],
#     'score': [100, 100, 98, 98, 99, 98, 95,96]
# }
#
# # Step 1: Create a DataFrame from the input data
# df = pd.DataFrame(data)
#
# # Step 2: Define functions to calculate the rank based on the conditions provided
# def get_completness_rank(score):
#     if score == 100:
#         return 0
#     elif score < 98:
#         return 5
#     elif score < 95:
#         return 10
#     else:
#         return 0
#
#
# def get_comformity_rank(score):
#     if score == 100:
#         return 0
#     else:
#         return 5
#
# def get_validity_rank(score):
#     if score == 100:
#         return 0
#     else:
#         return 30
#
# # Step 3: Calculate the rank based on the conditions and add it as a new column
# df['rank'] = df.apply(lambda row: get_completness_rank(row['score']) +
#                                    get_comformity_rank(row['score']) +
#                                    get_validity_rank(row['score']), axis=1)
#
# # Step 4: Group by 'tname' and 'fnme' and get the minimum validity score for each group
# df['rank'] = df.groupby(['tname', 'fnme'])['rank'].transform('min')
#
# # Step 5: Display the final output
# print(df)


import pandas as pd

df = pd.DataFrame({
    'tname': ['xyz', 'xyz', 'xyz', 'xyz1', 'xyz1', 'xyz1', 'xyz1'],
    'fnme': ['abc', 'abc', 'abc', 'abc1', 'abc1', 'abc1', 'abc1'],
    'dimesnsion': ['completness', 'comformity', 'validity',
                   'completness', 'comformity', 'validity', 'validity'],
    'score': [100, 100, 98, 98, 99, 98, 95]
})

df['validity'] = df.groupby(['tname', 'fnme'])['score'].transform('min')
df['completness'] = df.apply(lambda x: 0 if x['score'] == 100 else (5 if x['score'] < 98 else (10 if x['score'] < 95 else 0)), axis=1)
df['comformity'] = df.apply(lambda x: 0 if x['score'] == 100 else (5 if x['dimesnsion'] == 'comformity' and x['score'] < 100 else 0), axis=1)
df['validity'] = df.apply(lambda x: 0 if x['score'] == 100 else (30 if x['dimesnsion'] == 'validity' and x['score'] < 100 else 0), axis=1)
df['rank'] = df.groupby(['tname', 'fnme'])[['completness', 'comformity', 'validity']].transform('sum').sum(axis=1)

print(df[['tname', 'fnme', 'dimesnsion', 'score', 'rank']])

print("hello")
print("sharad2")