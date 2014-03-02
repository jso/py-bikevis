""" Load ride data into Sqlite3 database. """

import calendar
import csv
import datetime
import sqlite3
import sys

parse_date = lambda x: datetime.datetime.strptime(x, "%m/%d/%Y %H:%M")
parse_int = lambda x: int(x.replace(",", ""))
opt_int = lambda x: x != "" and parse_int(x) or None
opt = lambda x: x != "" and x or None

TABLE = "trips"

conversions = dict(
    trip_id=parse_int,
    starttime=parse_date,
    stoptime=parse_date,
    bikeid=parse_int,
    tripduration=parse_int,
    gender=opt,
    birthyear=opt_int,

    latitude=float,
    longitude=float,
    dpcapacity=parse_int,
)

name_map = dict(
    starttime="start_time",
    stoptime="stop_time",
    bikeid="bike_id",
    tripduration="trip_duration",
    usertype="user_type",
    birthyear="birth_year",
)

fields_types = dict(
    trip_id="INT",
    start_time="DATETIME",
    stop_time="DATETIME",
    bike_id="INT",
    trip_duration="INT",
    from_station_name="TEXT",
    from_station_latitude="DOUBLE",
    from_station_longitude="DOUBLE",
    from_station_dpcapacity="INT",
    to_station_name="TEXT",
    to_station_latitude="DOUBLE",
    to_station_longitude="DOUBLE",
    to_station_dpcapacity="INT",
    user_type="TEXT",
    gender="TEXT",
    birth_year="INT",
)

table_cols = list(sorted(fields_types.keys()))

insert_sql = "INSERT INTO %s (%s) VALUES (%s)" % (
    TABLE,
    ", ".join(table_cols),
    ", ".join(["?" for x in xrange(len(table_cols))]))

indices = dict( # name -> spec
    time_loc_index=(
        "(start_time, stop_time, from_station_name, to_station_name)"),
    bike_index="(bike_id)",
    duration_index="(trip_duration)",
    user_index="(user_type, gender, birth_year)",
)

def load(fn):
    cols = None

    with open(fn) as f:
        cols = f.readline().strip().split(",")

        for row in csv.DictReader(f, cols):
            for k in cols:
                if k in row and k in conversions:
                    try:
                        row[k] = conversions[k](row[k])
                    except ValueError as e:
                        print "problem converting %s from value %s" % (
                            k, row[k])
                        continue
            yield row

def create(con):
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS %s" % TABLE)
    cur.execute("CREATE TABLE %s(%s)" % (
        TABLE,
        ", ".join(
            [" ".join(x) for x in fields_types.iteritems()])))
    cur.close()
    con.commit()

def create_indices(con):
    cur = con.cursor()
    for index, spec in indices.iteritems():
        cur.execute("DROP INDEX IF EXISTS %s" % index)
        cur.execute("CREATE INDEX %s ON %s %s" % (index, TABLE, spec))
    cur.close()
    con.commit()

def load_rides(fn):
    for row in load(fn):
        bad = False
        for tofrom in ["to", "from"]:
            del row["%s_station_id" % tofrom]

            name_key = "%s_station_name" % tofrom
            if row[name_key] not in stations:
                print "couldn't find station", name_key
                bad = True
                break

            station = stations[row[name_key]]

            for k in station:
                row["%s_%s" % (tofrom, k)] = station[k]
        if bad:
            print "ignoring ride %d; incomplete station info" % row["trip_id"]
            continue

        # apply name changes
        for k, v in name_map.iteritems():
            if k in row:
                row[v] = row.pop(k)

        yield row

def insert_rides(cur, rides):
    if not rides: return

    list_rides = []
    for ride in rides:
        list_rides.append([ride.get(x) for x in table_cols])

    cur.executemany(insert_sql, list_rides)

    del rides[:]

if __name__ == "__main__":
    con = sqlite3.connect("bikevis.db")
    create(con)

    stations = {}

    # generating our own station IDs since the ones in the dataset are f'd up
    print "Loading stations..."
    for row in load(sys.argv[1]):
        stations[row["name"]] = row

    print "have", len(stations)

    print "Loading rides..."
    rides = []
    cur = con.cursor()
    for row in load_rides(sys.argv[2]):
        rides.append(row)
        if len(rides) > 10000:
            insert_rides(cur, rides)
    insert_rides(cur, rides)

    cur.execute("select count(trip_id) from %s" % TABLE)
    print "have", cur.fetchone()[0]

    cur.close()
    con.commit()

    print "Indexing..."
    create_indices(con)

    con.close()
