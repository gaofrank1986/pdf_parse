from fm_utils import FmUtils
from db_setup import Info,db_info,Concise_Table
from sqlalchemy import exists
from pprint import pprint
import os

a = FmUtils()
for ii in range(2012,2018):
    stock_name = '大华股份'
    mode = 1
    # -------------init -------------------
    b = a.load_from_json("./"+stock_name+"/info.json")
    key = stock_name+str(ii)+'.pdf'
    v = b[key]
    if v['mode']!= 99:
        mode = v['mode']
    path = os.path.join('./',stock_name,key)
    #  dirc = './' + stock_name + '/'
    #  pprint(dirc+key)
    print(path)
    d = a.read_and_clean(path,v['table_pages'],mode = mode)
    pprint(d)

    # ------------ init json ------------
    name = a.load_from_json("./json/table_trans.json")
    json_loc = dirc + stock_name + str(int(v['year'])-1)+'.json'
    if os.path.exists(json_loc):
        set_last = a.load_from_json(json_loc)
        print("found")
    else:
        set_last = a.load_from_json("./json/reports_template.json")
    set_this = a.load_from_json("./json/reports_template.json")

    # ---------operate db----------------
    db_keys = [str(v['stock_id'])+'_'+str(v['year']), str(v['stock_id'])+'_'+str(int(v['year'])-1)]
    for a_key in db_keys:
        (res,) =db_info.query(exists().where(Concise_Table.fm_id==a_key))
        if not(res[0]):
            db_info.add(Concise_Table(stock_id = v['stock_id'],date = a_key[-4:]))
            db_info.commit()

    for a_key in db_keys:
        db_info.query(Concise_Table).filter(Concise_Table.fm_id == a_key).\
                update({"stock_name": v['stock_name']})
        db_info.query(Concise_Table).filter(Concise_Table.fm_id == a_key).update({"unit": v['unit']})
    db_info.commit()

    for i in d.keys():
        if i in set_last.keys():
            buf = set_last.pop(i)
        if i in set_this.keys():
            buf = set_this.pop(i)
        tmp = name.get(i,-99)
        if tmp!="0" and tmp!=-99:
            db_info.query(Concise_Table).filter(Concise_Table.fm_id == db_keys[0]).update({name[i]: d[i][0]})
            db_info.query(Concise_Table).filter(Concise_Table.fm_id == db_keys[1]).update({name[i]: d[i][1]})
        elif tmp==-99:
            print(i)

    db_info.commit()

    # ----------save json --------------
    path = './' + stock_name + '/' + stock_name + str(v['year'])+'.json'
    if len(set_this)!=0:
        a.save_dict_to_json(path,set_this)
    else:
        if os.path.exists(path):
            os.remove(path)

    path = './' + stock_name + '/' + stock_name + str(int(v['year'])-1)+'.json'
    if len(set_this)!=0:
        # 保留原有输入数据
        if os.path.exists(path):
            tmp = a.load_from_json(path)
            for i in set_this:
                if i in tmp:
                    set_this[i] = tmp[i]
        a.save_dict_to_json(path,set_last)
    else:
        if os.path.exists(path):
            os.remove(path)

    
    #  except:
        #  #write in log
        #  with open("./log.txt","w") as f:
            #  f.write("Error when extracting tables from " + key +"\n")



