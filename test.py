from datetime import datetime
import jqdatasdk as jq
from util import jquant_util

jquant_util.auth()
data = jq.get_price(
        security=['IH2206.CCFX'], 
        end_date=datetime.now(),
        count=1,
        frequency="1m",
        fields=["open", "high", "low", "close", "volume"],
    ) 
print(data['time'])
print(data)