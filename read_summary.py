from fm_utils import FmUtils
from db_setup import Info,db_info,Concise_Table
from sqlalchemy import exists
from pprint import pprint

a = FmUtils()
for ii in range(2010,2018):
    b = a.load_from_json("./伟星新材/info.json")
    key = '伟星新材'+str(ii)+'.pdf'
    v = b[key]
    c = v['summary_pages']
    path = './伟星新材/'+key
    pages = str(c[0])+'-'+str(c[1])
    print(path)
    d = a.read_summary(path,pages)
    pprint(d)

    res = []
    for i in d:
        if '经常性损益的净' in i:
            res.append(d[i])
        if '资产收益率' in i:
            res.append(d[i])
    pprint(res) 

    keys = [str(v['stock_id'])+'_'+str(v['year']), str(v['stock_id'])+'_'+str(int(v['year'])-1)]
    for a_key in keys:
        (r,) =db_info.query(exists().where(Concise_Table.fm_id==a_key))
        if not(r[0]):
            db_info.add(Concise_Table(stock_id = v['stock_id'],date = a_key[-4:]))
            db_info.commit()


    db_info.commit()
    
    name = ['smr_deducted_net_profit','smr_avg_roe']
    db_info.query(Concise_Table).filter(Concise_Table.fm_id == keys[0]).update({name[0]: res[0][0]})
    db_info.query(Concise_Table).filter(Concise_Table.fm_id == keys[1]).update({name[0]: res[0][1]})
    db_info.query(Concise_Table).filter(Concise_Table.fm_id == keys[0]).update({name[1]: res[1][0]})
    db_info.query(Concise_Table).filter(Concise_Table.fm_id == keys[1]).update({name[1]: res[1][1]})

    db_info.commit()


