
CREATE DATABASE mydemo;
USE mydemo;
DROP TABLE IF EXISTS flight_delay;

CREATE TABLE IF NOT EXISTS flight_delay( Year int, Month int, DayofMonth int, DayOfWeek STRING, DepTime STRING, CRSDepTime STRING, ArrTime STRING, CRSArrTime STRING, UniqueCarrier STRING, FlightNum STRING, TailNum STRING, ActualElapsedTime STRING, CRSElapsedTime STRING, AirTime STRING, ArrDelay STRING, DepDelay STRING, Origin STRING, Dest STRING, Distance INT, TaxiIn STRING, TaxiOut STRING, Cancelled STRING, CancellationCode STRING, Diverted STRING, CarrierDelay STRING, WeatherDelay STRING, NASDelay STRING, SecurityDelay STRING, LateAircraftDelay STRING) row format delimited fields terminated by ',';

LOAD DATA INPATH 's3://delayflightbucket/dataset/DelayedFlights-updated.csv' INTO TABLE flight_delay;

SELECT Year, avg((CarrierDelay /ArrDelay)*100) from mydemo.flight_delay GROUP BY Year;
SELECT Year, avg((NASDelay /ArrDelay)*100) from mydemo.flight_delay GROUP BY Year;
SELECT Year, avg((WeatherDelay /ArrDelay)*100) from mydemo.flight_delay GROUP BY Year;
SELECT Year, avg((LateAircraftDelay /ArrDelay)*100) from mydemo.flight_delay GROUP BY Year;
SELECT Year, avg((SecurityDelay /ArrDelay)*100) from mydemo.flight_delay GROUP BY Year;