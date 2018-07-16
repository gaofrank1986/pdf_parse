from fm_utils import FmUtils
from db_setup import Info,db_info
from sqlalchemy import exists

a = FmUtils()

stock_id = 600298

for year in range(2013,2014):
    path = "./aqjm_"+str(year)+".json"
    r,e = a.get_ratios(a.load_from_json(path))

    key = str(stock_id)+str(year)
    name = a.load_from_json("./trans.json")
    (res,) =db_info.query(exists().where(Info.fm_id==key))
    if not(res[0]):
        db_info.add(Info(stock_id = stock_id,date = year))
        db_info.commit()
    #  是否考虑，整体
    for i in r.keys():
        db_info.query(Info).filter(Info.fm_id == key).update({name[i]: r[i]})
    db_info.commit()


