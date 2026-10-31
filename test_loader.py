import sys
import time

sys.path.insert(0, "include")

from opensky_client import get_access_token, get_flights
from snowflake_loader import get_snowflake_connection, load_flights

token = get_access_token()

end = int(time.time())
begin = end - 86400  # last 24 hours

flights = get_flights(token, "EGLL", begin, end, "arrival")
print("Flights fetched:", len(flights))

if flights:
    conn = get_snowflake_connection()
    loaded = load_flights(conn, flights[:5])
    print("Rows loaded:", loaded)
    conn.close()
else:
    print("No flights - try a different airport or time window")