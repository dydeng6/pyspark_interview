# import pandas as pd
#
# data = [('Tom', 43), ('Jerry', 88), ('Kate', 66)]
#
# df = pd.DataFrame(data)
# df.columns = ['name', 'score']
# df_output = df.sort_values(by='score').tail(1).values
# print(df_output)
#
#
# print('================================================================')
# df.sort_values(by='score', ascending=False, inplace=True)
# top = df.head(1)
# tuples = [tuple(x) for x in top.values]
# print(tuples[0])
# print('================================================================')
# data = [('Tom', 43), ('Jerry', 88), ('Kate', 66)]
# def sorting(t):
#     return t[0].lower
# print(type(data))
# data = sorted(data, key=sorting)
# print(data[0])

data = [('Tom', 43), ('Jerry', 88), ('Kate', 66)]
sorter_data = sorted(data,key=lambda x:x[1],reverse=True)
print(sorter_data)