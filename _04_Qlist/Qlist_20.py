'''
Find out common letters between two strings
Find out all letter between two strings
Find out letter which is present in one strings but not present in second string and vice versa

input1 = 'The content of your message'
input2 = 'about the upcoming transition'
'''
import pytest


def f_comparator(input1:str, input2:str)->list:
    input_set1 = set(input1.lower())
    input_set2 = set(input2.lower())
    print('Common letters',input_set1&input_set2)
    print('All letters',input_set1|input_set2)
    print('In set1 but not in set2',input_set1-input_set2)
    print('In set2 but not in set1', input_set2 - input_set1)

input1 = 'The content of your message'
input2 = 'about the upcoming transition'

f_comparator(input1, input2)
