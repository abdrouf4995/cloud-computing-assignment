import numpy as np
# map takes in raw-text and parses them by isolating each row
# and storing these in an dictionary

airport_name = []

def findCorrect(name):
    flag = False
    for i in np.arange(len(airport_name)):
        if not(len(name) == len(airport_name[i])):
            continue
        count = 0
        for idx in np.arange(len(name)):
            if name[idx] == airport_name[i][idx]:
                count += 1
            if count == len(name) - 1:
                flag = True
                return flag, airport_name[i]
    return flag, string1
# Fix error of data from csv files
def fix_error(data):
    start_name = data[2]
    arrive_name = data[3]
    length = len(data)
    index = [2, 3]
    for idx in index:
        # for index in np.arange(3):
        for index, letter in enumerate(data[idx]):
            ascii_code = ord(letter)
            if not ((ascii_code >= 97 and ascii_code <= 122) or
                    (ascii_code >= 65 and ascii_code <= 90) or
                    (ascii_code >= 48 and ascii_code <= 57)):

                if idx == 2:
                    _, start_name = findCorrect(data[2])
                    data[2] = start_name
                elif idx == 3:
                    _, arrive_name = findCorrect(data[3])
                    data[3] = arrive_name

    return data

# Read flight data from csv file
def read_flight(csv_file):
    data = []
    with open(csv_file, 'r') as f:

        # create a list of rows in the CSV file
        rows = f.readlines()

        # strip white-space and newlines
        rows = list(map(lambda x: x.strip(), rows))

        for row in rows:

            # further split each row into columns assuming delimiter is comma
            row = row.split(',')
            if row[0] is "":
                continue
            fixed = fix_error(row)
            # append to data-frame our new row-object with columns
            data.append(fixed)

    return data

# Read airport data from csv file
def read_airport(csv_file):
    data = []
    with open(csv_file, 'r') as f:

        # create a list of rows in the CSV file
        rows = f.readlines()

        # strip white-space and newlines
        rows = list(map(lambda x: x.strip(), rows))

        for row in rows:

            # further split each row into columns assuming delimiter is comma
            row = row.split(',')
            if row[0] is "":
                continue
            # if not fix_error(row):
            #     continue
            # append to data-frame our new row-object with columns
            data.append(row)

    return data
# map takes in raw-text and parses them by isolating each word
# and storing these in an array
def mapper(flight_file, airport_file):
    # Read airport data
    csv_airport = airport_file
    airportdata = read_airport(csv_airport)
    airport_data = {}
    for row in airportdata:
        airport = []
        airport.append(row[2])
        airport.append(row[3])
        if not row[1] in airport_data:
            airport_data[row[1]] = []
            airport_name.append(row[1])
        airport_data[row[1]].append(airport)

    # Read flight data
    csv_flight = flight_file
    flightdata = read_flight(csv_flight)
    dataLen = len(flightdata)

    return dataLen, flightdata, airport_data
