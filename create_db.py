from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import Table, Column, Integer, Numeric, String

Base = declarative_base()
class Basic_Info(Base):
    __tablename__ = 'Basic_Info'

    fm_id = Column(String(9),primary_key=True,nullable = False)
    stock_id = Column(String(6),nullable = False)
    stock_name = Column(String())
    date = Column(String(6))
    date_postfix = Column(String(2),nullable = False)
    #------------------------------------
    revenue_incr= Column(Numeric(3,2),default = 0.0)
    gross_margin = Column(Numeric(3,2),default = 0.0)
    total_expense_rate = Column(Numeric(3,2),default = 0.0)
    sale_expense_rate = Column(Numeric(3,2),default = 0.0)
    admin_expense_rate = Column(Numeric(3,2),default = 0.0)
    finance_expense_rate = Column(Numeric(3,2),default = 0.0)
    reduced_net_profit_incr = Column(Numeric(3,2),default = 0.0)
    asset_debt_ratio = Column(Numeric(3,2),default = 0.0)
    acc_rcv_over_revenue = Column(Numeric(3,2),default = 0.0)
    net_working_cap = Column(Numeric(9,2),default = 0.0)
    cash_eq_over_total_asset = Column(Numeric(3,2),default = 0.0)
    fixed_asset_ratio = Column(Numeric(3,2),default = 0.0)
    on_building_over_fixed = Column(Numeric(3,2),default = 0.0)
    ROE = Column(Numeric(3,2),default = 0.0)
    net_profit_margin = Column(Numeric(3,2),default = 0.0)
    total_turn_over = Column(Numeric(3,2),default = 0.0)
    finance_leverage = Column(Numeric(3,2),default = 0.0)
    total_asset_incr = Column(Numeric(3,2),default = 0.0)
    op_cash_over_net_profit = Column(Numeric(3,2),default = 0.0)
    ##----------------------
class Concise_Table(Base):
    __tablename__ = 'Concise_Finance_Data'
    fm_id = Column(String(9),primary_key=True,nullable = False)
    stock_id = Column(String(6),nullable = False)
    stock_name = Column(String())
    date = Column(String(6))
    date_postfix = Column(String(2),nullable = False)
    unit = Column(String())
    #-------------------------
    cash_and_eq = Column(Numeric(),default = 0.0)
    note_rcv    = Column(Numeric(),default = 0.0)
    acc_rcv     = Column(Numeric(),default = 0.0)
    pre_paid     = Column(Numeric(),default = 0.0)
    other_rcv     = Column(Numeric(),default = 0.0)
    inventory   = Column(Numeric(),default = 0.0)
    total_liquid_asset = Column(Numeric(),default = 0.0)
    total_asset = Column(Numeric(),default = 0.0)
    #-------------------------
    note_pyb    = Column(Numeric(),default = 0.0)
    acc_pyb    = Column(Numeric(),default = 0.0)
    pre_rcv    = Column(Numeric(),default = 0.0)
    other_pyb    = Column(Numeric(),default = 0.0)
    fixed_asset = Column(Numeric(),default = 0.0)
    on_building = Column(Numeric(),default = 0.0)
    total_liquid_debt  = Column(Numeric(),default = 0.0)
    total_debt         = Column(Numeric(),default = 0.0)
    equity      = Column(Numeric(),default = 0.0)
    #-------------------------
    total_revenue = Column(Numeric(),default = 0.0)
    revenue = Column(Numeric(),default = 0.0)
    op_cost = Column(Numeric(),default = 0.0)
    sale_expense = Column(Numeric(),default = 0.0)
    admin_expense = Column(Numeric(),default = 0.0)
    finance_expense = Column(Numeric(),default = 0.0)
    invst_gain = Column(Numeric(),default = 0.0)
    op_profit = Column(Numeric(),default = 0.0)
    total_profit = Column(Numeric(),default = 0.0)
    incm_tax = Column(Numeric(),default = 0.0)
    net_profit = Column(Numeric(),default = 0.0)

    cash_inflow_from_op = Column(Numeric(),default = 0.0)
    net_cash_from_op = Column(Numeric(),default = 0.0)
    capital_expenditure = column(numeric(),default = 0.0)

    smr_deducted_net_profit = Column(Numeric(),default =0.0)
    smr_avg_roe = Column(Numeric(),default =0.0)





    ##----------------------
engine = create_engine('sqlite:///info.db', convert_unicode=True, echo = False)
db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=engine))

Base.metadata.create_all(engine)

