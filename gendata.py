ofp=open("./data","w")
uids=range(1000,1000+10)
iids=range(5)
for u in uids:
    for i in iids:
        print >>ofp,"%s\t%s\t%s" %(u,i,1.0)
ofp.close()
