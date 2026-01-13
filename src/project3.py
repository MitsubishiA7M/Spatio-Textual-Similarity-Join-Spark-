#!/usr/bin/env python3
import sys, os
from math import floor, ceil, sqrt
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession

class project3:
    def run(self, inputpathA, inputpathB, outputpath, d, s):
        def to_uri(path):
            if path.startswith("hdfs://") or path.startswith("file://"):
                return path
            return "file://" + os.path.abspath(path)

        A_path = to_uri(inputpathA)
        B_path = to_uri(inputpathB)
        out_path = to_uri(outputpath)

        d_thr = float(d)
        s_thr = float(s)

        conf = SparkConf().setAppName("SpatioTextualSimilarityJoin")
        sc = SparkContext(conf=conf)
        spark = SparkSession.builder.config(conf=conf).getOrCreate()

        def parse_record(line):
            rid, coord, terms = line.strip().split('#')
            x, y = coord.strip('()').split(',')
            return rid, float(x), float(y), terms.split()

        def grid_cell(x, y):
            return int(floor(x/d_thr)), int(floor(y/d_thr))

        def neighbor_cells(cell):
            i, j = cell
            for di in (-1,0,1):
                for dj in (-1,0,1):
                    yield i+di, j+dj

        def euclid(p, q):
            return sqrt((p[0]-q[0])**2 + (p[1]-q[1])**2)

        def jaccard(a, b):
            inter = len(a & b)
            uni   = len(a | b)
            return inter/uni if uni else 0.0

        A = sc.textFile(A_path).map(parse_record)
        B = sc.textFile(B_path).map(parse_record)

        freq = (A.flatMap(lambda r: r[3])
                 .union(B.flatMap(lambda r: r[3]))
                 .map(lambda t: (t,1))
                 .reduceByKey(lambda a,b: a+b)
                 .collectAsMap())
        bf = sc.broadcast(freq)

        def gen_meta(r):
            rid, x, y, terms = r
            st = sorted(terms, key=lambda t: (bf.value.get(t,0), t))
            L = len(st)
            p = max(0, L - int(ceil(s_thr*L)) + 1)
            return rid, x, y, set(st), st[:p]

        A_meta = A.map(gen_meta)
        B_meta = B.map(gen_meta)

        A_cells = A_meta.flatMap(lambda r:
            [(c, r) for c in neighbor_cells(grid_cell(r[1],r[2]))])
        B_cells = B_meta.flatMap(lambda r:
            [(c, r) for c in neighbor_cells(grid_cell(r[1],r[2]))])

        spatial = (A_cells.join(B_cells)
                        .map(lambda kv: kv[1])
                        .map(lambda ab: ((ab[0][0], ab[1][0]), ab))
                        .reduceByKey(lambda a,b: a)
                        .map(lambda kv: kv[1])
                        .filter(lambda ab:
                            euclid((ab[0][1],ab[0][2]), (ab[1][1],ab[1][2])) <= d_thr))

        pref = spatial.filter(lambda ab:
            bool(set(ab[0][4]) & set(ab[1][4])))

        final_pairs = pref.filter(lambda ab:
            jaccard(ab[0][3], ab[1][3]) >= s_thr)

        def trim(x):
            s = f"{x:.6f}".rstrip('0').rstrip('.')
            return s if '.' in s else s + '.0'

        def to_output(ab):
            ra, rb = ab
            ai = int(ra[0][1:]); bi = int(rb[0][1:])
            dist = euclid((ra[1],ra[2]), (rb[1],rb[2]))
            jac  = jaccard(ra[3], rb[3])
            return ai, bi, f"(A{ai},B{bi}):{trim(dist)},{trim(jac)}"

        out = (final_pairs
               .map(to_output)
               .sortBy(lambda t: (t[0], t[1]))
               .map(lambda t: t[2])
               .coalesce(1))

        out.saveAsTextFile(out_path)

        sc.stop()
        spark.stop()

if __name__ == '__main__':
    if len(sys.argv)!=6:
        print("Wrong arguments"); sys.exit(-1)
    project3().run(*sys.argv[1:])

