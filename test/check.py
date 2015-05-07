#coding=utf-8
import sys
from collections import defaultdict
import pprint
import os
from math import sqrt

def get_data(inpath, inv=False):
    if os.path.isdir(inpath):
        fnms =[os.path.join(os.path.abspath(inpath), i) for i in os.listdir(inpath)]
    else:
        fnms = [inpath]
    u_info = defaultdict(list)
    for fnm in fnms:
        with open(fnm) as fp:
            for line in fp:
                uid, tid, score = line.rstrip().split()
                uid = int(uid)
                tid = int(tid)
                score = float(score)
                u_info[uid].append([tid, score])
    u_info = norm(u_info)
    if inv:
        inv_u_info = defaultdict(list)
        for uid, item_vec in u_info.items():
            for iid, sco in item_vec:
                inv_u_info[iid].append((uid, sco))
        return inv_u_info
    else:
        return u_info

def norm(u_dict):
    new_u_dict = defaultdict(list)
    for uid, item_vec in u_dict.items():
        sum = 0.
        for tid, sco in item_vec:
            sum += sco * sco
        sum = sqrt(sum)

        new_item_vec = []
        for tid, sco in item_vec:
            new_item_vec.append((tid, sco/sum))
        new_u_dict[uid] = new_item_vec
    return new_u_dict

def multiply(u_i, i_u):
    sim = {}
    for uid, info in u_i.iteritems():
        sim.setdefault(uid, {})
        for tid, score in info:
            for ouid, oscore in i_u.get(tid, []):
                sim[uid].setdefault(ouid, 0)
                sim[uid][ouid] += score * oscore
    return sim

def output(sim, outfile):
    ofp = open(outfile, 'w')
    for uid, info in sim.iteritems():
        info = ["%s:%s" %(tid, score) for tid, score in \
                sorted(info.iteritems(), key=lambda i:i[1], reverse=True)]
        ostr = "%s\t%s" %(uid, "|".join(info))
        print >>ofp, ostr
    ofp.close()

def exact_check(sim_bench, inpath):
    insim = {}
    for fnm in os.listdir(inpath):
        dname = os.path.abspath(inpath)
        fnm = os.path.join(dname, fnm)
        print 'checking ', fnm
        for line in open(fnm):
            uid, info = line.rstrip().split("\t")
            info = [i.split(":") for i in info.replace('|', ';').split(";")]
            uid = int(uid)
            info = [[int(i), float(j)] for i,j in info]
            binfo = sim_bench[uid]
            binfo = dict(binfo)
            for tid, score in info:
                if abs(score - binfo[tid]) > 0.00001:
                    print 'error in uid', uid , 'tid ', tid, score, 'vs', binfo[tid]
                    print line
    
if __name__=="__main__":
    if len(sys.argv) != 5:
        print 'argument error'
        sys.exit(2)
    u_i = get_data(sys.argv[1], inv=False)
    i_u = get_data(sys.argv[2], inv=True)
    ofnm = sys.argv[3]
    mpipath = sys.argv[4]
    sim = multiply(u_i, i_u)
    output(sim, ofnm)
    exact_check(sim, mpipath)

        
