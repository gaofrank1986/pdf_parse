from fm_utils import FmUtils
from db_setup import Info,db_info
from sqlalchemy import exists,and_

a = FmUtils()

stock_id = 1234

for year in range(2013,2018):
    path = "./pdf/dfyh"+str(year)+"A0.pdf"
    d = a.read_and_clean(path,check_multi=True)
    r,e = a.get_ratios(d)

    key = str(stock_id)+str(year)
    name = a.load_from_json("./trans.json")
    (res,) =db_info.query(exists().where(Info.fm_id==key))
    if not(res[0]):
        raise("Entry not existing")
    #  是否考虑，整体
    for i in r.keys():
        #  db_info.query(Info).filter(Info.fm_id == key).update({name[i]: r[i]})
        res = db_info.query(getattr(Info, name[i])).filter(and_(Info.date==year,Info.stock_id==stock_id)).all()
        print(res[0][0],r[i])


