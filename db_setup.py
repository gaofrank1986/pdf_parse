from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.automap import automap_base

engine = create_engine('sqlite:///info.db', convert_unicode=True, echo = False)
db_info = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=engine))
#  Base = automap_base()
#  Base.prepare(engine, reflect=True)
Base = declarative_base()
Base.metadata.reflect(engine)
class Info(Base):
    __table__ = Base.metadata.tables['Basic_Info']
    def __init__(self,date,stock_id):
        self.fm_id = str(stock_id)+'_'+str(date)
        self.stock_id = stock_id
        self.stock_name = "unkown"
        self.unit = 0.0
        self.date = date
        self.date_postfix = 'A'

        self.revenue_incr= 0.0
        self.gross_margin = 0.0
        self.total_expense_rate = 0.0
        self.sale_expense_rate = 0.0
        self.admin_expense_rate = 0.0
        self.finance_expense_raet = 0.0
        self.asset_debt_ratio = 0.0
        self.acc_rcv_over_revenue = 0.0
        self.net_working_cap = 0.0
        self.cash_eq_over_total_asset = 0.0
        self.fixed_asset_ratio = 0.0
        self.on_building_over_fixed = 0.0
        self.ROE = 0.0
        self.net_profit_margin = 0.0
        self.total_turn_over = 0.0
        self.finance_leverage = 0.0
        self.total_asset_incr = 0.0
        self.op_cash_over_net_profit = 0.0

class Concise_Table(Base):
    __table__ = Base.metadata.tables['Concise_Finance_Data']
    def __init__(self,date,stock_id):
        self.fm_id = str(stock_id)+'_'+str(date)
        self.stock_id = stock_id
        self.stock_name = "unkown"
        self.unit = 0.0
        self.date = date
        self.date_postfix = 'A'

        self.cash_and_eq = 0.0
        self.note_rcv    = 0.0
        self.acc_rcv     = 0.0
        pre_paid     = 0.0
        other_rcv     = 0.0
        self.inventory   = 0.0
        self.total_liquid_asset = 0.0
        self.total_asset = 0.0

        note_pyb    = 0.0
        acc_pyb    = 0.0
        pre_rcv    = 0.0
        other_pyb    = 0.0
        self.fixed_asset = 0.0
        self.on_building = 0.0
        self.total_liquid_debt  = 0.0
        self.total_debt         = 0.0
        self.equity      = 0.0

        total_revenue = 0.0
        self.revenue = 0.0
        self.op_cost = 0.0
        self.sale_expense = 0.0
        self.admin_expense = 0.0
        self.finance_expense = 0.0
        invst_gain = 0.0
        self.op_profit = 0.0
        total_profit = 0.0
        incm_tax = 0.0
        self.net_profit = 0.0

        cash_inflow_from_op = 0.0
        self.net_cash_from_op = 0.0

        smr_deducted_net_profit = 0.0
        smr_avg_roe = 0.0
