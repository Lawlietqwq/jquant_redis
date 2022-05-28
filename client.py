from mq.consumer import Consumer
from strategy.stop_loss import StopLossStrategy

# client1 = Consumer("client1", pool)
# client1.subscribe("1m", client1.process_message)

# stoploss_strategy = StopLossStrategy("stoploss-client", pool, "IH2206.CCFX")
stoploss_strategy = StopLossStrategy("stoploss-client", "IH2206.CCFX")
stoploss_strategy.subscribe("1m", stoploss_strategy.process_message)
