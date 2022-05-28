import whorder
from util.mysingleton import singleton

@singleton
class clientAPI:
    """
    load accounts
    """
    def __init__(self, info_account, deal_account):
        self.api = whorder.PyApi()
        self.info_account = info_account
        self.deal_account = deal_account
        self.api.Init(info_account)
        

    def handleOrder(self, code, buyOrSell, lot, price, FokFakFlag = 0, EntryOrExit = 0, FormulaName="根据盘口智能分批下单"):
        code = code[:code.index('.')]
        self.api.SendOrderToAlgo(Account=self.deal_account,Contract=code,BuyOrSell=buyOrSell,EntryOrExit=EntryOrExit,
        Lot=lot,Price=price, FokFakFlag=FokFakFlag, FormulaName=FormulaName)