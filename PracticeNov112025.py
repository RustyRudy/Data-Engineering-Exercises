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




