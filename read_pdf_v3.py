from fm_utils import FmUtils
from db_setup import Info,db_info,Concise_Table
from sqlalchemy import exists
from pprint import pprint

a = FmUtils()
for ii in range(2011,2018):
    name = '康力电梯'
    b = a.load_from_json("./"+name+"/info.json")
    key = name+str(ii)+'.pdf'
    v = b[key]
    c = v['table_pages']
    path = './' + name + '/' + key
    pprint(path)
    try:
        pages = str(c[0])+'-'+str(c[1])
        d = a.read_and_clean(path,pages,mode = 1)
        pprint(d)

        keys = [str(v['stock_id'])+'_'+str(v['year']), str(v['stock_id'])+'_'+str(int(v['year'])-1)]
        name = a.load_from_json("./json/table_trans.json")
        for a_key in keys:
            (res,) =db_info.query(exists().where(Concise_Table.fm_id==a_key))
            if not(res[0]):
                db_info.add(Concise_Table(stock_id = v['stock_id'],date = a_key[-4:]))
                db_info.commit()

        for a_key in keys:
            db_info.query(Concise_Table).filter(Concise_Table.fm_id == a_key).\
                    update({"stock_name": v['stock_name']})
            db_info.query(Concise_Table).filter(Concise_Table.fm_id == a_key).update({"unit": v['unit']})
        db_info.commit()
        for i in d.keys():
            tmp = name.get(i,-99)
            if tmp!="0" and tmp!=-99:
                db_info.query(Concise_Table).filter(Concise_Table.fm_id == keys[0]).update({name[i]: d[i][0]})
                db_info.query(Concise_Table).filter(Concise_Table.fm_id == keys[1]).update({name[i]: d[i][1]})
            elif tmp==-99:
                print(i)

        db_info.commit()
    except:
        #write in log
        with open("./log.txt","w") as f:
            f.write("Error when extracting tables from " + key +"\n")



