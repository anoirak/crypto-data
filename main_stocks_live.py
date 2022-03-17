import classes.Live.getLive as l
from classes.Live.getPancake import getPancake

# Get Live Data
l.getData()
#Run PancakeSwap API
tokens = getPancake()
try:
    tokens.main()
except Exception as e:
    print(e)