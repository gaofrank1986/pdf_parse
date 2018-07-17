from fm_utils import FmUtils
from db_setup import Info,db_info,Concise_Table
from sqlalchemy import exists,and_,or_

a = FmUtils()
stock_id = '002372'
for ayear in range(2010,2018):
    year = [str(ayear),str(ayear-1)]

    rec = a.load_from_json("./json/table_trans.json")# load trans 
    rec = dict(rec)
    tmp = []
    for i in rec:
        if rec[i] =='0':
           tmp.append(i)
    for i in tmp:
        rec.pop(i)
    rec = a.swap_dict(rec)

    last = dict()
    this = dict()

    for i in rec.keys():
        tmp = float(db_info.query(getattr(Concise_Table,i)).filter(and_(Concise_Table.date ==year[0]\
                ,Concise_Table.stock_id==stock_id)).all()[0][0])

        tmp1 = float(db_info.query(getattr(Concise_Table,i)).filter(and_(Concise_Table.date ==year[1]\
                ,Concise_Table.stock_id==stock_id)).all()[0][0])

        last[i] = tmp1
        this[i] = tmp

    # TODO check revenue and total revenue
    r = a.get_ratios(this,last)

    fm_id = stock_id+'_'+year[0]
    (res,) =db_info.query(exists().where(Info.fm_id==fm_id))
    if not(res[0]):
        db_info.add(Info(stock_id = stock_id,date = fm_id[-4:]))
        db_info.commit()
        #  是否考虑，整体
    for i in r.keys():
        db_info.query(Info).filter(Info.fm_id ==fm_id).update({i: r[i]})
    db_info.commit()




