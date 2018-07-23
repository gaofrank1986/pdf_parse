from fm_utils import FmUtils
from db_setup import Info,db_info,Concise_Table
from sqlalchemy import exists
from pprint import pprint

a = FmUtils()
stock_name = '万华化学'

for ii in range(2011,2018):
    b = a.load_from_json('./'+stock_name+'/info.json')

    key = stock_name+str(ii)+'.pdf'
    v = b[key]
    c = v['summary_pages']
    path = './'+stock_name+'/'+key
    pages = str(c[0])+'-'+str(c[1])
    print(path)
    try:
        d = a.read_summary(path,pages)
        pprint(d)

        res = [(0,0),(0,0)]
        for i in d:
            if '经常性损益的净' in i or '经常性损益后的净' in i:
                if res[0] == (0,0):
                    res[0] = d[i]
            if '资产收益率' in i:
                res[1] = d[i]
        pprint(res) 

        keys = [str(v['stock_id'])+'_'+str(v['year']), str(v['stock_id'])+'_'+str(int(v['year'])-1)]
        for a_key in keys:
            (r,) =db_info.query(exists().where(Concise_Table.fm_id==a_key))
            if not(r[0]):
                db_info.add(Concise_Table(stock_id = v['stock_id'],date = a_key[-4:]))
                db_info.commit()
        db_info.commit()

        name = ['smr_deducted_net_profit','smr_avg_roe']
        if res[0] != (0,0):
            db_info.query(Concise_Table).filter(Concise_Table.fm_id == keys[0]).update({name[0]: res[0][0]})
            db_info.query(Concise_Table).filter(Concise_Table.fm_id == keys[1]).update({name[0]: res[0][1]})
        if res[1] != (0,0):
            db_info.query(Concise_Table).filter(Concise_Table.fm_id == keys[0]).update({name[1]: res[1][0]})
            db_info.query(Concise_Table).filter(Concise_Table.fm_id == keys[1]).update({name[1]: res[1][1]})

        db_info.commit()
    except:
        #write in log
        with open("./log.txt","w") as f:
            f.write("Error when extracting summary from " + key +"\n")


