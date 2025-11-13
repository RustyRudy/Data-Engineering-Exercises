x, y, z = 'Blah', 'blah', 'blah blah'
print(x)
print(y)
print(z)

a=b=c= "Bampire"
print(a)
print(b)
print(c)

#unpacking a collection, here a tuple is used for an example
cars = ('ford', 'porsche', 'maserati')
x,y,z = cars    #can be done with lists as well
print(x)
print(y)
print(z)

#using this now
print(x,y,z)

#global variables are all variables outside of functions
def func1():
    print("My other car is a "+x+y+z)
func1()
print('------------************--------------')

fruits = ["apple", "banana", "cherry", "kiwi", "mango"]

newlist = []
for x in fruits:
  if "a" in x:
    newlist.append(x)

# newlist = [x for x in fruits if "a" in x]

print(newlist)
print('------------************--------------')

thisdict = dict(name = "John", age = 36, country = "Norway")
print(thisdict)

thisdict1 = {
  "brand": "Ford",
  "model": "Mustang",
  "year": 1964
}
# x = thisdict1["model"]
# print(x)

x = thisdict1.keys()

print(x) #before the change

thisdict1["color"] = "white"

print(x) #after the change

x = thisdict.values()
print(x)

print('------------------*******************----------------------')
#playing with anagrams
#words that are an anagram of a key, will have the values
#appended to a list of anagrams that an anagram, sorted alphabetically can create
inputList = ['eat', 'tea', 'tan', 'ate', 'nat', 'bat']

x = 'eat'
y = sorted(x)
print(y)
y1 = "".join(y) #creating a
print(y1)
inputDict = {}

for x in inputList:
  sortedWord = "".join(sorted(x))
  if sortedWord not in inputDict: #{'aet': [eat, tea], 'ant': ['tan']}
    inputDict[sortedWord] = [x]
  else:
    inputDict[sortedWord].append(x) #{'aet': [eat, tea], }
#print(inputDict)
print(inputDict.values())
yList = sorted(inputList)
print(yList)

# Write a Python function to convert two lists into a dictionary.
# Input: Keys=['a', 'b', 'c'], Values=[1, 2, 3]
# Output: {'a': 1, 'b': 2, 'c': 3

keys = ['a', 'b', 'c']
values = [1, 2, 3]
counter = 0
newDict = {}
for x in keys:
  newDict[x] = values[counter]
  counter += 1
counter = 0
print(newDict)

# Write a Python function to find commonkeys in two
# dictionaries. Input: {'a': 1, 'b': 2}, {'b': 3, 'c': 4}
# Output: ['b']
dict1 = {'a': 1, 'b': 2}
dict2 = {'b': 3, 'c': 4}
common = []
#my solution, takes a larger complexity
# for dict1key in dict1:
#   for dict2key in dict2:
#     if dict1key == dict2key:
#       common.append(dict1key)
for k in dict1.keys():
  if k in dict2:
    common.append(k)
print(common)

# Write a Python function to remove all duplicate characters in a given
# string while preserving the order of the remaining characters.
# Input: "programming"
# Output: "progamin"
alreadyParsed =""
string = 'programming'

for x in string:
  if x not in alreadyParsed:
    alreadyParsed += x
print(alreadyParsed)

# Find the First Non-Repeating Character
# Write a Python function to find the first non-repeating character in a given string and return its index.
# Input: "swiss"
# Output: 1 (for 'w' in "swiss")

string = "swissdee"
countingDict = {}
freq = 0
for x in string:
  if x not in countingDict:
    countingDict[x] = 1
  else:
    countingDict[x] += 1
print(countingDict)
# {'s': 3, 'w': 1, 'i': 1}

result = ''
for i in string:
  freq = countingDict[i]
  if freq == 1:
    result = i
    break
print(result)











