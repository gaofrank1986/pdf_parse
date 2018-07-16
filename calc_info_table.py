from fm_utils import FmUtils
from db_setup import Info,db_info,Concise_Table
from sqlalchemy import exists

class Info_Record():
    def __init__(self):
        pass

    rec = dict()
    rec = # load trans 
    # remove the ones has value 0
    # swap dict
    rec[name[i]] = db_info.query(getattr(Concise_Table, name[i])).filter(and_(Info.date==year,Info.stock_id==stock_id)).all()


    (res,) =db_info.query(exists().where(Info.fm_id==key))
    if not(res[0]):
        raise("Entry not existing")



    (res,) =db_info.query(exists().where(Info.fm_id==a_key))
    if not(res[0]):
        db_info.add(Info(stock_id = v['stock_id'],date = a_key[-4:]))
        db_info.commit()
