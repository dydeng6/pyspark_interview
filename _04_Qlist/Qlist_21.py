'''
get the tuple with the highest score
'''

data = [('Tom', 43), ('Jerry', 88), ('Kate', 66)]

max_tuple = max(data,key=lambda x:x[1])
print(max_tuple)