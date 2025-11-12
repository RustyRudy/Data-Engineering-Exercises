from distutils.command.build_scripts import first_line_re


def comparison(var1, var2):
    if var1 > var2:
        print("var1 is greater than var2")
    elif var1 < var2:
        print("var2 is greater than var1")
    else:
        print("var 'var1' and 'var2' are equal")
def sum(var1, var2):
    return var1+var2


if __name__ == '__main__':
    print ("Hello World.")
    a = 10
    b = 15

    comparison(a,b)

    print("Sum of a and b is:",sum(a,b))

    list1 = [404, 420, 69, 0]

    for i in list1:
        print(i)

    for i in list1:
        if i > 69 and i < 420:
            print(i)

    maximum = max(list1)
    print(maximum)

    for i in range(0, len(list1)):
        print("inside range for:", i)
        print("inside range for:", list1[i])
        print("\n")

    # for i in range(0, len(list1)):
    #     if list1[i] % 2 == 0:
    #         print(list1[i]," is an even number")
    #     elif list1[i] % 2 != 0:
    #         print(list1[i], " is an odd number")
    #     else:
    #         print("It's zero")

    for i in list1:
        if i % 2 == 0:
            print(i," is an even number")
        elif i % 2 != 0:
            print(i, " is an odd number")
        else:
            print("It's zero which is an even number")


    print("Slicing1:", list1[0:4])
    print("Slicing2:", list1[0:])
    print("Slicing3:", list1[:3])
    print("Slicing4:", list1[-4:-1])
    print("Slicing4:", list1[-4:])

    text = "Vlad Tepes Dracula"

    print(len(text))
    print(text[1:6])
    print(text[:6])
    print(text[5:10])
    print(text[0:])

    for i in text:
        print(i)

    print("-------------")

    for i in range(0 , len(text)):
        if i % 2 != 0:
            print(text[i])

    # dictionary actions
    dmap = {'a':1, 'b':2, 'c':3, 'd':4, 'e':5, 'f':6}
    dmap['g'] = 7
    print(dmap)
    dmap['d'] = dmap['d']+1
    print(dmap)
    dmap2 = {}  #empty dictionary
    dmap2['a'] = 22
    print(dmap2)
    letter=['a', 'b', 'c', 'd', 'a', 'c', 'c', 'd', 'd']
    letterCounter = 0

    for i in letter:
        if i == 'c':
            letterCounter += 1
    print("letter counter:", letterCounter)

    dictMessage = 'hello world'
    stringCounter = {}

    for i in dictMessage:
        if i in stringCounter:
            stringCounter[i] = stringCounter[i]+1
        else:
            stringCounter[i] = 1
    print("stringCounter:", stringCounter)
    dictMessage2 = "Whatever has gone wrong with the world?"
    splitWords = dictMessage2.split()
    print("splitWords:", splitWords)

    dictMessage2 = "Whatever,Whatever,has,gone,wrong,with,the,world?"
    splitWords2 = dictMessage2.split(',')
    print("splitWords2:", splitWords2)

    wordCounter = {}
    for i in splitWords2:
        if i in wordCounter:
            wordCounter[i] = wordCounter[i]+1
        else:
            wordCounter[i] = 1
    print("wordCounter:", wordCounter)

    #finding the word that appears the most
    maxWord = ''
    maxWordFreq = 0
    for i in wordCounter.keys():
        if wordCounter[i] > maxWordFreq:
            maxWordFreq = wordCounter[i]
            maxWord = i
    print(maxWord)
    #easier way to parse through for the max number??
    #might not be useful for interviews
    MaximumWordCount = max(wordCounter.values())
    print("MaximumWordCount:", MaximumWordCount)

    #max min values
    max_val = float('inf')
    min_val = float('-inf')

    print ('-------------------------------------')
    #finding the word that appears the least
    minWord = ''
    minWordFreq = min_val
    for i in wordCounter.keys():
        if wordCounter[i] > minWordFreq:
            minWordFreq = wordCounter[i]
            minWord = i
    print(minWord)
    #easier way to parse through for the max number??
    #might not be useful for interviews
    MinimumWordCount = min(wordCounter.values())
    print("MinimumWordCount:", MinimumWordCount)

    print ('-------------------------------------')

    #swapping keys and values
    DictEx = {'a':1, 'b':2, 'c':3 }
    counterDictEx = {}

    for key in DictEx.keys():
        value = DictEx.get(key)
        counterDictEx[value] = key
    print(counterDictEx)

    print('-------------------------------------')
    #single line lambda functions instead of defining everything
    f1 = lambda a, b: a*b
    print(f1(3, 5))


