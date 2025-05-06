# Inserting data into HBase
put 'flight_info', 'flight1', 'delay:departure_delay', '10'
put 'flight_info', 'flight1', 'schedule:departure_time', '2025-05-06 08:00'
put 'flight_info', 'flight1', 'schedule:arrival_time', '2025-05-06 11:00'

put 'flight_info', 'flight2', 'delay:departure_delay', '5'
put 'flight_info', 'flight2', 'schedule:departure_time', '2025-05-06 09:00'
put 'flight_info', 'flight2', 'schedule:arrival_time', '2025-05-06 12:00'
