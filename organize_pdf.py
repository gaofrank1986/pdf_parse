import os
from parse_pdf_v2 import PDFTarget
from collections import OrderedDict
from fm_utils import FmUtils

a = FmUtils()
cur_dir = "./"
for file in os.listdir(cur_dir):
    try:
        if (file[-4:].lower() == '.pdf'):
            w = PDFTarget(cur_dir+file)
            info = w.generate_info()
            old_path = cur_dir+file
            new_name = info['stock_name'] + w.year +'.pdf'
            new_dir =  cur_dir+info['stock_name']
            print(new_dir ,new_name)
            print('\n\n')
            if not(os.path.exists(new_dir)):
                os.mkdir(new_dir)
                os.rename(old_path,new_dir+'/'+new_name)
            else:
                os.rename(old_path,new_dir+'/'+new_name)

            if os.path.exists(new_dir+'/info.json'):
                info = a.load_from_json(new_dir+'/info.json')
                info[new_name] = w.generate_info()
                a.save_dict_to_json(new_dir+'/info.json',info,sort=True)
            else:
                info = OrderedDict()
                info[new_name] = w.generate_info()
                a.save_dict_to_json(new_dir+'/info.json',info,sort=True)
    except:
        #write in log
        with open("./log.txt","w") as f:
            f.write("Error when processing " + file +"\n")




