from fm_utils import FmUtils
from db_setup import Info,db_info
from sqlalchemy import exists,desc


def get_common_row(stock_id,col_name,color=False,trailing=""):
    a = FmUtils()
    trans = a.load_from_json("./json/trans.json")
    trans = a.swap_dict(trans)
    if col_name != "date":
        decor = a.load_from_json("./json/decor.json")
        decor = decor[trans[col_name]]
    else:
        decor =[False,"",False]
    if decor[1]=="%":
        decor[1]="\\%"
    print(col_name)
    print(decor)
    #  db_info.query(Info.date).filter(Info.stock_id==1234).order_by(desc(Info.date)).all()
    res = db_info.query(getattr(Info, col_name)).filter(Info.stock_id==stock_id).order_by(desc(Info.date)).all()
    if col_name != "date":
        line = trans[col_name]
    else:
        line =""
    # check if exists
    for i in res:
        line = line+ "& "+ str(i[0])+decor[1]+" "
    line+="\\\\"
    if decor[0]:
        line="\\rowcolor[gray]{0.9}"+line
    if decor[2] == 'dot':
        line=line+"\\hdashline"
    if decor[2] == 'solid':
        line=line+"\midrule"
    return(line)

def write_tex(path,stock_id,flag_rotate = 0):
    a = FmUtils()
    trans = a.load_from_json("./json/trans.json")
    num_rec = db_info.query(Info).filter(Info.stock_id==stock_id).count()
    stock_name = db_info.query(Info.stock_name).filter(Info.stock_id==stock_id).all()[0][0]
    row_one = get_common_row(stock_id,"date")
    row_two=""
    tab_title = str(stock_id+" "+stock_name+" 基本信息")
    for i in range(1,num_rec+1):
        row_two = row_two+"\cmidrule(l){"+str(i+1)+"-"+str(i+1)+"}"

    with open(path, 'w') as f:
        f.write("\\documentclass[a4paper,12pt]{article} \n")
        f.write("\\usepackage{booktabs}\n")
        f.write("\\usepackage{colortbl}\n")
        f.write("\\usepackage{tabu}\n")
        f.write("\\usepackage{xeCJK}\n")
        f.write("\\usepackage{adjustbox}\n")
        f.write("\\usepackage{fontspec} \n")
        f.write("\\setCJKmainfont[Scale=0.8]{SimSun} \n")
        f.write("\\setCJKmonofont{SimSun}\n")
        f.write("\\setmainfont[Scale=0.85, Ligatures={Required,Common,Contextual,TeX}]{TeX Gyre Schola} \n")
        #---------------
        f.write("\n\n\n")
        #---------------
        f.write("%dashed line\n")
        f.write("\\usepackage{array}\n")
        f.write("\\usepackage{arydshln}\n")
        f.write("\\setlength\\dashlinedash{0.9pt}\n")
        f.write("\\setlength\\dashlinegap{1.5pt}\n")
        f.write("\\setlength\\arrayrulewidth{0.3pt}\n")

        f.write("\\begin{document}\n")
        f.write("\\begin{table}[!htbp]\n")
        f.write("\\centering\n")
        f.write("\\caption{"+tab_title+"}\\label{tab:aStrangeTable}\n")
        if(flag_rotate):
            f.write("\\begin{adjustbox}{angle=90}\n")
        seg = "l"+"r"*(num_rec)
        f.write("\\begin{tabular}{"+seg+"}\n")
        f.write("\\toprule\n")
        f.write(row_one+"\n")
        f.write(row_two+"\n")
        for i in trans:
            f.write(get_common_row(stock_id,trans[i])+"\n")

        f.write("\\bottomrule\n")
        f.write("\\end{tabular}\n")
        if(flag_rotate):
            f.write("\\end{adjustbox}\n")
        f.write("\\end{table}\n")
        f.write("\\end{document}\n")


write_tex("./test.tex","002372",flag_rotate = 1)
