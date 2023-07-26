import pandas as pd

data = {
    'tname': ["xyz", "xyz", "xyz", "xyz1","xyz1","xyz1","xyz1"],
    'fnme': ["abc", "abc", "abc","abc1","abc1","abc1","abc1"],
    'dimesnsion': ["completness", "comformity", "validity","completness", "comformity", "validity","validity"],
    'score': [100, 100, 98, 98, 99, 98,95]
}
df = pd.DataFrame(data)
def calculate_rank(row):
    a1=0
    b1=0
    c1=0
    d1 = 0

    if row['dimesnsion'] == 'completness':
        if row['score'] == 100:
            a1+=0
            return a1
        elif row['score'] < 98:
            a1+=10
            return a1
        elif row['score'] < 95:
            a1+=15
            return a1
    elif row['dimesnsion'] == 'comformity':
        if row['score'] == 100:
            b1+=0
            return b1
        elif row['score'] < 100:
            b1+=5
            return b1
    elif row['dimesnsion'] == 'validity':
        min_validity_score = df.loc[df["dimesnsion"] == "validity", "score"].min()
        if row['score'] == 100:
            c1+=0
            return c1
        elif row['score'] < min_validity_score:
            c1+=30
            return c1
    return a1+b1+c1


df['rank'] = df.apply(calculate_rank, axis=1)

# Calculate the overall rank as the sum of individual ranks
df['rank'] = df.groupby(['tname', 'fnme'])['rank'].transform('sum')


print(df)



