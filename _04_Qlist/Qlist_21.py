'''
get the tuple with the highest score
'''

data = [('Tom', 77), ('Jerry', 50), ('Kate', 66)]

max_tuple = max(data,key=lambda x:x[1])
print(max_tuple)