import numpy as np
import pandas as pd
colnames = ['Pass_id', 'f_id', 'from', 'dest', 'dept_time','tot_time']
flights = pd.read_csv(r"C:\Users\Desktop\AComp_Passenger_data_no_error.csv",names=colnames)
airports = pd.read_csv(r"C:\Users\Desktop\Top30_airports_LatLong.csv")

#part 1
port1= airports['IATA']
port2 =flights['from']
x = []
y = []

for p1 in port1 :
    if p1 not in port2.values:
        x.append(p1)
    elif p1 in port2.values:
        y.append(p1)
print("Used Airports:")
print(*y, sep = "\n")
print("\nUnused Airports:")
print(*x , sep = "\n")
