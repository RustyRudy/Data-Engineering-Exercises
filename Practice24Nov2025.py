def anagramCompare(string1, string2):
    aDict = {}
    bDict = {}

    for i in string1:
        if i in aDict: aDict[i] += 1
        else: aDict[i] = 1
    for j in string2:
        if j in bDict: bDict[j] += 1
        else: bDict[j] = 1

    print("aKeys:", list(aDict.keys()))
    print("aValues:", list(aDict.values()))
    print("bKeys:", list(bDict.keys()))
    print("bValues:", list(bDict.values()))

    if aDict == bDict: print("They're anagrams")
    else: print("They're not anagrams")


if __name__ == '__main__':

    aString = "allyoop"
    bString = "poollya"

    if len(aString) == len(bString):
        if sorted(aString) == sorted(bString):
            print("They're  anagrams")
    else: print("They're not anagrams")

