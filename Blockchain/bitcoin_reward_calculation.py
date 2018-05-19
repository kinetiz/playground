import math

block = int(210000)
baseReward = int(50)
n = 0
numBtc = []
start_year = 2008
round(10.1234568,4)
float("{0:.4f}".format(10.1234568))
for i in range(0,51):
    
    if(i>0):
        lastBtc = numBtc[i-1] 
    else:
        lastBtc = 0
        
    nowBtc = round(int(lastBtc) + int(block)*int(baseReward)/int(math.pow(2,i)),4)
    print('year: '+ str(start_year + (i+1)*4) + ' : '+ str(nowBtc))
    numBtc.append(nowBtc)

diff = 20671875 - 20343750
diffPerYear = diff/4
goal = 20580000
a = 20343750 + 3*diffPerYear
a
if (a > goal):
    print("exceed")
