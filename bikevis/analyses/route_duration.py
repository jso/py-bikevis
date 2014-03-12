import sqlite3
import matplotlib.pyplot as plt
from numpy import array, mean, median, std

from util import get, put_list, put_val

def symmetry(con, threshold=75):
    data = {}

    cur = con.cursor()
    cur.execute("SELECT from_station_name, to_station_name, count(trip_id) num "
        "FROM trips WHERE from_station_name != to_station_name GROUP BY from_station_name, to_station_name HAVING num >= ?", (threshold,))
    while True:
        rows = cur.fetchmany(1000)
        if not rows: break
        for row in rows:
            put_val(data, (row["from_station_name"], row["to_station_name"]),
                    "num", row["num"])

    print "have %d routes with at least %d trips" % (len(data), threshold)

    # drop pairs where either direction has less than the given threshold
    for pair in list(data):
        rev_pair = tuple(reversed(pair))
        if (get(data, pair, "num", leaf=0) <= threshold
            or rev_pair not in data
            or get(data, rev_pair, "num", leaf=0) <= threshold):
            del data[pair]
            continue

    print "have %d routes remaining after filtering for reverse trip meeting threshold" % len(data)

    i = 0
    for pair in data:
        durs = []
        cur.execute("SELECT trip_duration FROM trips WHERE from_station_name = ? "
                    "AND to_station_name = ?", pair)
        while True:
            rows = cur.fetchmany(1000)
            if not rows: break
            for row in rows:
                durs.append(row["trip_duration"])

        durs_a = array(durs)
        put_val(data, pair, "mean", mean(durs_a))
        put_val(data, pair, "median", median(durs_a))
        put_val(data, pair, "std", std(durs_a))

        #print pair, data[pair]
        i += 1
        if i % 100 == 0:
            print "getting duration stats; %d routes remaining" % (len(data) - i)

    cur.close()

    plot_data = {}

    done = set()
    for pair in data:
        rev_pair = tuple(reversed(pair))
        if rev_pair in done: continue
        done.add(rev_pair)

        for param in ["num", "mean", "median", "std"]:
            val1 = get(data, pair, param)
            val2 = get(data, rev_pair, param)

            put_list(plot_data, param, "x", min(val1, val2))
            put_list(plot_data, param, "y", max(val1, val2))

    for param in ["num", "mean", "median", "std"]:
        fig = plt.figure()
        xs = plot_data[param]["x"]
        ys = plot_data[param]["y"]
        plt.scatter(xs, ys)
        plt.xlabel("min(%s)" % param)
        plt.ylabel("max(%s)" % param)
        plt.savefig("symmetry_%s.pdf" % param, bbox_inches="tight")

if __name__ == "__main__":
    con = sqlite3.connect("bikevis.db")
    con.row_factory = sqlite3.Row

    symmetry(con)

    con.close()
