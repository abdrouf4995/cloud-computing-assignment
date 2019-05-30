import sys
import multiprocessing
import mapper
import reducer
import math
import numpy as np
import time
import threading

outfile = open('results.txt', 'w')

threadCount = 10
processCount = 10
g_flight_from_airport = {}
g_flightID_based_flights = {}

flight_file = "AComp_Passenger_data.csv"
airport_file = "Top30_airports_LatLong.csv"
dataLen, flightdata, airport_data = mapper.mapper(flight_file, airport_file)

thread_arr = []

KILOMETERS_TO_MILES =float(0.6214)
# Calculate the distance between two positions ,given latitudes and longitudes
def getDistanceFromLatLonInKm(lat1, lon1, lat2, lon2):
    R = 6371  # Radius of the earth in km
    dLat = deg2rad(lat2 - lat1)  # deg2rad below
    dLon = deg2rad(lon2 - lon1)
    a = math.sin(dLat / 2) * math.sin(dLat / 2) \
        + math.cos(deg2rad(lat1)) * math.cos(deg2rad(lat2)) \
        * math.sin(dLon / 2) * math.sin(dLon / 2)

    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a));
    d = R * c  # Distance in km
    miles = d * KILOMETERS_TO_MILES
    return miles

# Convert degree to radian
def deg2rad(deg):
    return deg * (math.pi / 180)

# Get the (processID)th data of the (threadID)th thread
def getData(threadID, processID):
    thread_data = []
    for idx in np.arange(processCount):
        thread_data.append(flightdata[threadCount * processCount * (processID - 1)
                                      + processCount * (threadID  - 1) + idx])
    return thread_data

# Calculate reduce and sum up of mapreduce algorithm
def reduceAndSum():

    flight_from_airport = {}
    flightID_based_flights = {}
    for threadID in g_flight_from_airport:
        for idx in g_flight_from_airport[threadID]:
            if not idx in flight_from_airport:
                flight_from_airport[idx] = g_flight_from_airport[threadID][idx]
            else:
                flight_from_airport[idx] += g_flight_from_airport[threadID][idx]
        for idx in g_flightID_based_flights[threadID]:
            if not idx in flightID_based_flights:
                flightID_based_flights[idx] = []
            for i in np.arange(len(g_flightID_based_flights[threadID][idx])):

                flightID_based_flights[idx].append(g_flightID_based_flights[threadID][idx][i])

    # Total number of flights from each airport
    print("\n Total number of flights from every airport:\n ")
    outfile.write("\n Total number of flights from every airport:")
    for idx in flight_from_airport:
        print("\tAirport(" + idx + ") : "
              + str(flight_from_airport[idx]))
        outfile.write("\n \tAirport(" + idx + ") : "
              + str(flight_from_airport[idx]))

    # Calculate the number of passengers on each flight
    print("\n Total list of flights based on flightID:\n ")
    outfile.write("\n Total list of flights based on flightID:")
    for flightID in flightID_based_flights:
        print("\n \n \tFlight ID: " + flightID)
        outfile.write("\n \n \tFlight ID: " + flightID)
        print("\tNumber of passengers: " + str(len(flightID_based_flights[flightID])))
        outfile.write("\n \tNumber of passengers: " + str(len(flightID_based_flights[flightID])))
        print("\tFrom airport(" + flightID_based_flights[flightID][0][1] + ") to airport("
              + flightID_based_flights[flightID][0][2] + ")")
        outfile.write("\n \tFrom airport(" + flightID_based_flights[flightID][0][1] + ") to airport("
              + flightID_based_flights[flightID][0][2] + ")")
        startAirport = flightID_based_flights[flightID][0][1]
        arriveAirport = flightID_based_flights[flightID][0][2]
        startLat = float(airport_data[startAirport][0][0])
        startLon = float(airport_data[startAirport][0][1])
        arriveLat = float(airport_data[arriveAirport][0][0])
        arriveLon = float(airport_data[arriveAirport][0][1])
        dist = getDistanceFromLatLonInKm(startLat, startLon, arriveLat, arriveLon)
        print("\tFlight distance: " + str(dist) + " miles")
        outfile.write("\n \tFlight distance: " + str(dist) + " miles")



# Process data for every thread
def process(threadID):
    processID = 1;
    flight_from_airport = {}
    flightID_based_flights = {}
    while (True):
        flightdata = getData(threadID, processID);

        for row in flightdata:
            if not row[2] in flight_from_airport:
                flight_from_airport[row[2]] = 1
            else:
                flight_from_airport[row[2]] += 1

        # Flights based on the flight ID

        # for row in flightdata:
            flight_data = []
            flight_data.append(row[0])
            flight_data.append(row[2])
            flight_data.append(row[3])
            flight_data.append(row[4])
            flight_data.append(row[5])

            if not row[1] in flightID_based_flights:
                flightID_based_flights[row[1]] = []
            flightID_based_flights[row[1]].append(flight_data)

        processID += 1
        if processID == dataLen // (threadCount * processCount):
            break
    g_flight_from_airport[threadID] = flight_from_airport
    g_flightID_based_flights[threadID] = flightID_based_flights

    if(len(g_flight_from_airport) == threadCount and len(g_flightID_based_flights) == threadCount):
        for idx in g_flight_from_airport:
            print("\nThread " + str(idx) + ":")
            outfile.write("\nThread " + str(idx) + ":")
            print("\n \tThe number of flights from every airport:\n")
            outfile.write("\n \tThe number of flights from every airport:\n ")
            for flight in g_flight_from_airport[idx]:
                print("\t\tAirport(" + flight + ") : "
                      + str(g_flight_from_airport[idx][flight]))
                outfile.write("\n \t\tAirport(" + flight + ") : "
                      + str(g_flight_from_airport[idx][flight]))

            print("\n \tList of flights based on flightID:\n ")
            outfile.write("\n \tList of flights based on flightID:\n ")
            for flightID in g_flightID_based_flights[idx]:
                print("\t\tFlight ID: " + flightID)
                outfile.write("\n \t\tFlight ID: " + flightID)
                print("\t\tNumber of passengers: " + str(len(g_flightID_based_flights[idx][flightID])))
                outfile.write("\n \t\tNumber of passengers: " + str(len(g_flightID_based_flights[idx][flightID])))
                print("\n \t\tFrom airport(" + g_flightID_based_flights[idx][flightID][0][1] + ") to airport("
                      + g_flightID_based_flights[idx][flightID][0][2] + ")")
                outfile.write("\t\tFrom airport(" + g_flightID_based_flights[idx][flightID][0][1] + ") to airport("
                      + g_flightID_based_flights[idx][flightID][0][2] + ")")
                startAirport = g_flightID_based_flights[idx][flightID][0][1]
                arriveAirport = g_flightID_based_flights[idx][flightID][0][2]
                startLat = float(airport_data[startAirport][0][0])
                startLon = float(airport_data[startAirport][0][1])
                arriveLat = float(airport_data[arriveAirport][0][0])
                arriveLon = float(airport_data[arriveAirport][0][1])
                dist = getDistanceFromLatLonInKm(startLat, startLon, arriveLat, arriveLon)
                print("\t\tFlight distance: " + str(dist) + " Km\n ")
                outfile.write("\n \t\tFlight distance: " + str(dist) + " Km\n ")
        reduceAndSum()

def main():

    count = 0
    thread_data = []

    for idx in np.arange(threadCount):
        # thread = multiprocessing.Process(target=process, args=(idx + 1,))
        # thread.start()
        thread = threading.Thread(target=process, args=(idx + 1,))
        thread.start()
        thread_arr.append(thread)



    print ("File written successfully in results.txt")
    # //sys.exit(1)


if __name__ == '__main__':
    start = time.time()
    main()
    for idx in np.arange(len(thread_arr)):
        thread_arr[idx].join()
    end = time.time()


    # print("Process time: " + str(end - start))
    print("Process finished!")
