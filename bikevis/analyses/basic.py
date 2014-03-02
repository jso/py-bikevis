import sqlite3
import matplotlib.pyplot as plt

from util import put_add, put_list, put_val, traverse
from cdf import cdf

def demographics(con):
    data = {}

    cur = con.cursor()
    cur.execute("SELECT gender, birth_year, COUNT(trip_id) trip_count "
        "FROM trips WHERE gender NOT NULL AND birth_year NOT NULL "
        "GROUP BY gender, birth_year ORDER BY trip_count DESC")
    while True:
        rows = cur.fetchmany(1000)
        if not rows: break
        for row in rows:
            put_val(data, row["gender"], row["birth_year"], row["trip_count"])
    cur.close()

    for kvs, data in traverse(data, "gender"):
        gender = kvs["gender"]
        xs = list(sorted(data.keys()))
        ys = [data[x] for x in xs]
        total = sum(ys)

        fig = plt.figure()
        plt.scatter(xs, ys)
        plt.yscale("log")
        plt.xlabel("birth year")
        plt.ylabel("number of rides")
        plt.grid(axis="y")
        plt.title("%s n=%d" % (gender, total))
        plt.savefig("gender.%s.pdf" % gender, bbox_inches="tight")

def trip_duration(con):
    c = cdf(resolution=60)

    cur = con.cursor()
    cur.execute("SELECT trip_duration FROM trips")
    while True:
        rows = cur.fetchmany(1000)
        if not rows: break
        for row in rows:
            c.insert(row["trip_duration"])
    cur.close()

    fig = plt.figure()
    plt.plot(*c.getData())
    plt.xscale("log")
    plt.xlabel("trip duration (s)")
    plt.ylabel("CDF of trips")
    plt.xticks(
        [60, 300, 900, 1800, 3600, 7200, 14400],
        ["1m", "5m", "15m", "30m", "1h", "2h", "4h"])
    plt.grid(axis="y")
    plt.savefig("duration.pdf", bbox_inches="tight")

def station_popularity(con):
    data = {}

    cur = con.cursor()
    cur.execute("SELECT from_station_name, to_station_name FROM trips")
    while True:
        rows = cur.fetchmany(1000)
        if not rows: break
        for row in rows:
            put_add(data, row["from_station_name"], 1)
            put_add(data, row["to_station_name"], 1)
    cur.close()

    keys = list(sorted(data, key=lambda x: data[x], reverse=True))
    xs = list(xrange(len(keys)))
    ys = [data[x] for x in keys]

    fig = plt.figure()
    plt.plot(xs, ys)
    plt.xscale("log")
    plt.yscale("log")
    plt.xlabel("station index")
    plt.ylabel("trip endpoint count")
    plt.savefig("station_popularity.pdf", bbox_inches="tight")

    n = 50
    fig = plt.figure(figsize=(30, 4))
    plt.bar(xs[:n], ys[:n], log=True)
    plt.xlabel("station")
    plt.ylabel("trip endpoint count")
    plt.xticks(xs[:n], keys[:n])
    fig.autofmt_xdate()
    plt.savefig("station_popularity.top%d.pdf" % n, bbox_inches="tight")

def route_popularity(con):
    data = {}

    cur = con.cursor()
    cur.execute("SELECT from_station_name, to_station_name FROM trips")
    while True:
        rows = cur.fetchmany(1000)
        if not rows: break
        for row in rows:
            put_add(data, (row["from_station_name"], row["to_station_name"]), 1)
    cur.close()

    keys = list(sorted(data, key=lambda x: data[x], reverse=True))
    keys_str = ["->".join(x) for x in keys]
    xs = list(xrange(len(keys)))
    ys = [data[x] for x in keys]

    fig = plt.figure()
    plt.plot(xs, ys)
    plt.xscale("log")
    plt.yscale("log")
    plt.xlabel("station pair index")
    plt.ylabel("route count")
    plt.savefig("route_popularity.pdf", bbox_inches="tight")

    n = 25
    fig = plt.figure(figsize=(20, 4))
    plt.bar(xs[:n], ys[:n], log=True)
    plt.xlabel("station pair")
    plt.ylabel("route count")
    plt.xticks(xs[:n], keys_str[:n])
    fig.autofmt_xdate()
    plt.savefig("route_popularity.top%d.pdf" % n, bbox_inches="tight")

def bike_trips(con):
    c = cdf()
    c_dur = cdf()

    cur = con.cursor()
    cur.execute("SELECT COUNT(*) trip_count, SUM(trip_duration) total_time "
        "FROM trips GROUP BY bike_id")
    while True:
        rows = cur.fetchmany(1000)
        if not rows: break
        for row in rows:
            c.insert(row["trip_count"])
            c_dur.insert(row["total_time"])
    cur.close()

    fig = plt.figure()
    plt.plot(*c.getData())
    plt.xscale("log")
    plt.xlabel("number of trips")
    plt.ylabel("CDF of bikes")
    plt.savefig("bike_trips.pdf", bbox_inches="tight")

    fig = plt.figure()
    plt.plot(*c_dur.getData())
    plt.xscale("log")
    plt.xlabel("total trip time (s)")
    plt.ylabel("CDF of bikes")
    plt.savefig("bike_time.pdf", bbox_inches="tight")

if __name__ == "__main__":
    con = sqlite3.connect("bikevis.db")
    con.row_factory = sqlite3.Row

    print "user demographics"
    demographics(con)

    print "trip duration"
    trip_duration(con)

    print "station popularity"
    station_popularity(con)

    print "route popularity"
    route_popularity(con)

    print "bike trips"
    bike_trips(con)

    con.close()
