from pyspark.sql.functions import length, struct

input = 'aabbbcddeehhhhh'
# output = [2,3,1,2,2,4]
def f_counters(input:str) -> list:
    counter = 1
    output = []
    for i in range(0, len(input)-1):
        if(input[i] == input[i+1]):
            counter += 1
        else:
            output.append((input[i],counter))
            counter = 1
    output.append((input[-1],counter))
    return output

print(f_counters(input))


