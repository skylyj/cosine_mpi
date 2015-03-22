import random, os
def gendata(opath, num):
    for iter_i in range(num):
        ofnm = os.path.join(opath, str(iter_i))
        ofp=open(ofnm, 'w')
        uids=range(1000,1000+10)
        iids=range(iter_i*5, (iter_i+1)*5)
        print iter_i, iids
        for u in uids:
            for i in iids:
                score = random.randint(1,10)
                print >>ofp,"%s\t%s\t%s" %(u, i, score)
        ofp.close()

gendata("./data", 10)
