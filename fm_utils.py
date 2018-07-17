import PyPDF2
from tabula import read_pdf
import json
import os
import logging
from math import isnan
from collections import OrderedDict
import pandas as pd
import re


class FmUtils(object):

    def __init__(self):
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
        logging.disable(level=logging.INFO)

    def read_summary(self,path,pages):
        indic =['营业收入','营业总收入','加权平均']
        pos =[[],[],[]]
        df = read_pdf(path, pages = pages,silent=True, multiple_tables=True, pandas_option={'header':None})
        self._summary = df

        ww = pd.concat(df[:])
        ww.index = range(0,ww.shape[0])
        self._buf = ww

        for d in range(0,ww.shape[0]):
            f1 = ww.loc[d,:].isnull()
            skip_flag = False
            if not(f1[0]) and self._is_valid_line(ww.loc[d,:]):
            # first cell is not nan
                if isinstance(ww.loc[d,0],str):
                    skip_flag = self._is_a_stop(ww.iloc[d:,0])
                for kk in range(1,4):
                    if d == ww.shape[0] - kk or (d+kk) > ww.shape[0] or skip_flag:
                        pass
                    else:
                        f1 = ww.loc[d+kk,:].isnull()
                        # 如果 [0] is nan 同时 [1] isnot nana
                        # [0] not nan, 非nan最多2个
                        if (f1[0] and not(f1[1])) or (not(f1[0]) and  ww.shape[1] - sum(f1)<3):
                            ww.loc[d,:] = ww.loc[d,:].fillna(' ')
                            if(self._is_valid_line(ww.loc[d+kk,:])):
                                ww.loc[d,:] += ww.loc[d+kk,:].fillna('')
                            skip_flag = self._is_a_stop(ww.iloc[d+kk:,0])
        ww = ww.fillna(' ')
        self._buf2 = ww


        # 保留前三列
        for d in range(3,ww.shape[1]):
            ww = ww.drop(d,axis = 1) 
        ww.index = range(0,ww.shape[0])
        # dataframe 转换成 list
        res = []
        for i in range(ww.shape[0]):
            out = ' '.join(list(ww.loc[i,:]))
            res.append(out)
        # 处理每行
        self._buf3 = res
        res = [ self.process_line(x) for x in res ]
        res = [ x for x in res if len(x) > 2 ]
        # 结果转存在有序dict里面
        #  e,tmp = self._formatted_list_to_ordered_dic(res)
        mark =[]
        for i in range(len(res)):
            if '营业收入' in res[i]:
                mark.append(i)
        if len(mark) > 1:
            e = res[:mark[1]]
        else:
            e = res

        self._buf4 = e
        ans = OrderedDict()
        for d in e:
            d = d.split()
            ans[d[0]] = (self._comma_sep_string_to_num(d[1]),self._comma_sep_string_to_num(d[2]))

        return(ans)

    def _is_a_stop(self,s):
        # 包括当前字节
        flagline = list(s.isnull())
        s = list(s)
        flag = False
        if(len(s) == 1):
            flag = True

        if not(flag) and not(flagline[0]):
            if s[0][-1] in ['入',')','额','润']:
            # find first valid string
                for i in range(1,len(s)):
                    if(not(flagline[i])):
                        break
                if(s[i][0] == '('):
                    pass
                else:
                    flag = True
        return flag


    def _is_valid_line(self,s):
        s = ' '.join(list(s.fillna(' ')))
        ans = True
        if "年" in s:
            ans = False
        for i in s.split():
            if re.sub('[^0-9]','', i) in range(1990,2030):
                ans = False
                break
        for i in s.split():
            if '季度' in i:
                ans = False
                break
        return ans


    def read_and_clean(self, path, pages="",check_multi = True, mode = 1):

        if not(pages):
            pages = "all"
        df = read_pdf(path, pages = pages,silent=True, multiple_tables=True, pandas_option={'header':None})

        for k in range(len(df)):
            ncol = df[k].shape[1]
            # 如果在前2/3的部分发现大于6列则判定双列表
            if  ncol > 6 and k < round(len(df)*2/3) and mode!=3 :
                parallel_tab_flag = True
                #  raise Exception(path,ncol,"parallel table")

        df  =pd.concat(df[:]).fillna(' ')
        df.index = range(0,df.shape[0])

        self._buf = df
        # 清理数据
        res = self._clean_df(df,mode)
        # 标准化index
        res = self.normalize_table(res)

        if mode == 3:
            new_out = []
            for i in res:
                if len(i.split()) == 5:
                    i = i.split()
                    new_out.append(' '.join([i[0],i[1],i[3]]))
        else:
            new_out = res
        self._buf2 = new_out


        # 处理子母表
        # a. 如果有重复
        #       起始为m[0]
        #       如果m[0]和[m1]相差1
        #       如果有m2,结束为m2,否则为下个pos的m0,或者表的最后一行
        #       
        new_output = []
        pos = self.mark_multi_table(new_out)
        self._buf3 = pos.copy()
        if check_multi:
            flag = (len(pos['t1'])==0) or (len(pos['t2'])==0) or (len(pos['t3'])==0)
            if flag:
                raise Exception("table missing")
            flag = (len(pos['t1'])>1) or (len(pos['t2'])>1) or (len(pos['t3'])>1)
            if flag:
                new_output=[]
                for i in range(1,4):
                    # 一般情况
                    # start = m[0]
                    # end = 下个表格其实/所有记录的最后一行位置
                    m = pos['t'+str(i)]
                    start = m[0]
                    if(i<2):
                        end = pos['t'+str(i+1)][0]
                    else:
                        end = len(new_out)-1

                    if len(m) > 1:
                        if (m[1]-m[0])==1:
                            if(len(m)>2):
                                end = m[2]
                        else:
                            end = m[1]

                    for j in range(start,end):
                        new_output.append(new_out[j])
                    pos['t'+str(i)] = {'start':start,'end':end}
            new_out = new_output
            self._buf4 = pos
        #--------------
        # if not check multi table, the value is corrupted
        e,tmp = self._formatted_list_to_ordered_dic(new_out)

        return(e)





    def process_line(self,res, mode = 1):
        # 处理单行
        # 1. 对每行用_format_index进行预处理
        # 2.
        #    a. 去掉长度小于2的（一般是注释）(不然‘库存’‘商誉’)
        #    c. 查看每个comp是否是合理的数字，生成newline,flagline
        # 3. a. 如果数字出现少于2个，就去掉（需要针对two col进行修改）
        #    b. 如果最后一个comp是文字，就去掉
        #    c. 如果前两个都是文字，去掉第一个
        x = [self._format_index(i) for i in res.split()]

        blist = [str(i) for i in range(10,100)]

        newline=[]
        flagline=[]
        for comp in x:
            if len(comp)<2 or (comp in blist): #从3变为2，'存货'被删掉了 增加对10-99数字判断
                # some comment is like 1,2,3...11,12
                continue
            flagline.append(self._is_str_valid(comp))
            newline.append(comp)

        if flagline.count(True) < 2:
            return('')
        else:
            if flagline[0] == True:
                # abandon the line start with a valid number
                newline=['']
            elif flagline[-1] == False:
                #如果最后一个不是数字，删除
                newline.pop();
            elif (len(flagline[:-2]) > 1 and mode == 2):
                # 去掉第一个，和最后两个数字中间的
                for kk in range(len(flagline[:-2])-1):
                    # newline在删element过程中会变化，所以用len(flagline)
                    newline.remove(newline[len(flag[:-2])-1-kk])
            #  elif (mode == 3):
                #  newline = [newline[0],newline[1],newline[5]]

                #  newline.remove(newline[1])
            return(' '.join(newline))


    def normalize_table(self,output):
        for i in range(len(output)):
            tmp = output[i].split()
            if('销售商品、提供劳务收' in tmp[0]):
                tmp[0] = '销售商品、提供劳务收到的现金'
            if('经营活动产生的现' in tmp[0]):
                tmp[0] = '经营活动产生的现金流量净额'
            if tmp[0] in ['股东权益合计','所有者权益合计']:
                tmp[0] = '所有者权益'
            output[i]  = ' '.join(tmp)
        return(output)


    def mark_multi_table(self,output):
        # 通过下面的string标记合并报表和母公司报表的起始位置
        t1 = set(['货币资金'])
        t2 = set(['营业收入','营业总收入'])
        t3 = set(['销售商品、提供劳务收到的现金'])
        pos ={'t1':[],'t2':[],'t3':[]}
        for i in range(len(output)):
            tmp =set()
            tmp.add(output[i].split()[0])
            if len(tmp.intersection(t1)) != 0:
                pos['t1'].append(i)
            elif len(tmp.intersection(t2)) != 0:
                pos['t2'].append(i)
            elif len(tmp.intersection(t3)) != 0:
                pos['t3'].append(i)
        return(pos)

    def _is_str_valid(self,s):
        '''check if this string is a valid number'''
        if not(isinstance(s,str)):
            s = str(s)
        ss = s.replace(',','')
        ss = ss.replace('.','')
        ss = ss.replace('-','')
        ss = ss.replace('%','')
        return(bytes.isalnum(ss.encode()))

    # 清理表格
    #    a.合并dataframe
    #    b.处理单行
    def _clean_df(self,df,mode):
        #dataframe 变成 list
        for i in range(df.shape[1]):
            if i == 0:
                tmp = df.loc[:,i]
            else:
                tmp = tmp+' '+df.loc[:,i]
        res = list(tmp)
        # 清理每行
        res = [ x for x in res if len(x.split())>2] 
        res = [ self.process_line(x,mode = mode) for x in res ]
        res = [ x for x in res if len(x.split()) > 2 ]

        return(res)


    # 转为有序dict
    def _formatted_list_to_ordered_dic(self,new_out):
        e = OrderedDict()
        # 记录重复次数
        count_d = dict()
        for i in new_out:
            tmp = i.split()
            flag = e.get(tmp[0],-1)
            if flag==-1:
                e[tmp[0]]=[self._comma_sep_string_to_num(tmp[1]),self._comma_sep_string_to_num(tmp[2])]
                count_d[tmp[0]] = 0
            else:
                count_d[tmp[0]] +=1
        flag = [x > 2 for x in count_d.values()]
        if any(flag):
            raise Exception("Too many duplicates")

        return (e,count_d)


    def _format_index(self,s):
        assert(len(s.split())<2)
        #normalize 单个string格式
        # 去掉
        # 1. 带有alist的
        # 2. 对于非数字包含blist
        # 3. 带有括号和括号内的
        flag = True

        alist =['一、','二、','三、','四、','五、','六、','七、','八、','九、']
        for i in alist:
            if i in s:
                s = s.split(i)[1]
        alist =['一.','二.','三.','四.','五.','六.','七.']
        for i in alist:
            if i in s:
                s = s.split(i)[1]

        if(self._is_str_valid(s)):
            blist=[]
            if len(s)<3:
                s=''
                flag = False
        else:
            s = s.replace('-','')
            blist = [str(i)+'.' for i in range(0,20)]
            #remove 1. , 2. ,...........
            #如果非数字string长度小于2，删除
            if len(s)<2:
                s=''
                flag = False
        if flag:
            clist = ['其中:','减:','加:']
            for i in blist+clist:
                s = s.replace(i,'')
            tmp = s.find('(')
            if not(tmp==-1):
                if tmp<3:
                    tmp2 = s.split(')')
                    if len(tmp2)>1:
                        s = tmp2[1]
                tmp2 = s.split('(')
                s = tmp2[0]
            tmp = s.find(')')
            if tmp!=-1:
                if len(s)<3:
                    s=''
        return(s)

    def _comma_sep_string_to_num(self,s):
        ##将逗号分隔的数字转成数字
        if not(isinstance(s,str)):
            raise Exception('no string input cannot be casted into float')
        if not(s):
            return(0)
        ss = s.replace(',','')
        ss = ss.replace('%','')
        logging.debug(ss)
        logging.debug(ss[0])
        sign = 1
        if (ss[0] == '-')and(ss.count('-')==1):
            sign = -1
            ss = ss.replace('-','')
        if ss.count('.') > 1:
            #  raise Exception(ss,'not valid format')
            print(ss)
            ss = '999999999'
        tmp = ss.replace('.','')
        if not(tmp.isdigit()):
            print(ss)
            ss = '999999999'
            #  raise Exception(ss,'string contain non number component')

        return(sign*float(ss))


    def swap_dict(self,idict):
        # best way to exchange key and value in a dict
        # res = dict((v,k) for k,v in a.items())
        return(dict((v,k) for k,v in idict.items()))

    def save_dict_to_json(self,path,idict,sort=False):

        with open(path, 'w') as fp:
            # add chinese output
            # add indentation
            #  json.dump(idict, fp,ensure_ascii=False,indent=4)
            json.dump(idict, fp,ensure_ascii=False,indent=4, sort_keys=sort)


    def load_from_json(self,path):
        data = json.load(open(path),object_pairs_hook=OrderedDict)
        return(data)

    def deal_with_duplicate_dict_values(self,idt):
        acc = 0
        for i in idt.keys():
            idt[i] = acc
            acc+=1


    #  def get_ratios(self,e):

        #  result = OrderedDict()
        #  prec = 2
        #  #  print(e)
        #  result['营收增幅'] = e['营业收入'][0]/e['营业收入'][1] -1
        #  result['毛利率'] = 1 - e['营业成本'][0]/e['营业收入'][0]
        #  result['三项费用率'] = (e['管理费用'][0]+e['销售费用'][0]+e['财务费用'][0])/e['营业收入'][0]
        #  result['销售费用率'] = e['销售费用'][0]/e['营业收入'][0]
        #  result['管理费用率'] = e['管理费用'][0]/e['营业收入'][0]
        #  result['财务费用率'] = e['财务费用'][0]/e['营业收入'][0]
        #  result['扣非净利润增幅'] = e['营业利润'][0]/e['营业利润'][1] - 1
        #  result['资产负债率'] = e['负债合计'][0]/e['资产总计'][0]
        #  result['应收账款占收入'] = (e['应收账款'][0]+e['应收票据'][0])/e['营业收入'][0]
        #  result['净营运资本'] = (e['流动资产合计'][0]-e['流动负债合计'][0])/10**8
        #  result['固定资产占总资产比重'] = (e['固定资产'][0])/e['资产总计'][0]
        #  result['现金资产占总资产比重'] = (e['货币资金'][0])/e['资产总计'][0]
        #  result['在建工程占固定资产比重'] = (e['在建工程'][0])/e['固定资产'][0]
        #  result['净资产收益率'] = e['净利润'][0]/((e['所有者权益'][0]+e['所有者权益'][1])/2)
        #  result['净利润率'] =  e['净利润'][0]/e['营业收入'][0]
        #  result['总周转率'] =  e['营业收入'][0]/((e['资产总计'][0]+e['资产总计'][1])/2)
        #  result['财务杠杆'] =  e['资产总计'][0]/(e['资产总计'][0] -  e['负债合计'][0])
        #  result['总资产增长率'] = e['资产总计'][0]/e['资产总计'][1]
        #  result['经营性现金率/净利润'] =e['经营活动产生的现金流量净额'][0]/ e['净利润'][0]

        #  for i in result.keys():
            #  result[i] = round(result[i]*100,prec)
        #  result['净营运资本'] = round(result['净营运资本']/100,prec)
        #  result['财务杠杆'] = round(result['财务杠杆']/100,prec)
        #  result['总周转率'] = round(result['总周转率']/100,prec)

        #  return(result,e)


    def get_ratios(self,this,last):
        # ----------------------------------------------
        result = OrderedDict()
        prec = 2
        #  result['营收增幅'] = e['营业收入'][0]/e['营业收入'][1] -1
        # total revenue
        result['revenue_incr'] = this['revenue']/last['revenue'] - 1
        #  result['毛利率'] = 1 - e['营业成本'][0]/e['营业收入'][0]
        result['gross_margin'] = 1 - this['op_cost']/this['revenue']
        #  result['三项费用率'] = (e['管理费用'][0]+e['销售费用'][0]+e['财务费用'][0])/e['营业收入'][0]
        result['total_expense_rate'] = (this['admin_expense']+this['sale_expense']+\
                this['finance_expense'])/this['revenue']
        #  result['销售费用率'] = e['销售费用'][0]/e['营业收入'][0]
        result['sale_expense_rate'] = this['sale_expense']/this['revenue']
        #  result['管理费用率'] = e['管理费用'][0]/e['营业收入'][0/this['revenue']]
        result['admin_expense_rate'] = this['admin_expense']/this['revenue']
        #  result['财务费用率'] = e['财务费用'][0]/e['营业收入'][0]
        result['finance_expense_rate'] = this['finance_expense']/this['revenue']
        #  result['扣非净利润增幅'] = e['营业利润'][0]/e['营业利润'][1] - 1
        result['reduced_net_profit_incr'] = this['op_profit']/last['op_profit'] - 1
        #  result['资产负债率'] = e['负债合计'][0]/e['资产总计'][0]
        result['asset_debt_ratio'] = this['total_debt']/this['total_asset']
        #  result['应收账款占收入'] = (e['应收账款'][0]+e['应收票据'][0])/e['营业收入'][0]
        result['acc_rcv_over_revenue'] = (this['acc_rcv']+this['note_rcv'])/this['revenue']
        #  result['净营运资本'] = (e['流动资产合计'][0]-e['流动负债合计'][0])/10**8
        result['net_working_cap'] = this['note_rcv']+ this['acc_rcv']+this['pre_paid']+this['other_rcv']\
                +this['inventory']-this['note_pyb']-this['acc_pyb']-this['pre_rcv']
        #  result['固定资产占总资产比重'] = (e['固定资产'][0])/e['资产总计'][0]
        result['fixed_asset_ratio'] = this['fixed_asset']/this['total_asset']
        #  result['现金资产占总资产比重'] = (e['货币资金'][0])/e['资产总计'][0]
        result['cash_eq_over_total_asset'] = this['cash_and_eq']/this['total_asset']
        #  result['在建工程占固定资产比重'] = (e['在建工程'][0])/e['固定资产'][0]
        result['on_building_over_fixed'] = this['on_building']/this['fixed_asset']
        #  result['净资产收益率'] = e['净利润'][0]/((e['所有者权益'][0]+e['所有者权益'][1])/2)
        result['ROE'] = this['net_profit']/(0.5*this['equity']+0.5*last['equity'])
        #  result['净利润率'] =  e['净利润'][0]/e['营业收入'][0]
        result['net_profit_margin'] = this['net_profit']/this['revenue']
        #  result['总周转率'] =  e['营业收入'][0]/((e['资产总计'][0]+e['资产总计'][1])/2)
        result['total_turn_over'] = this['revenue']/(0.5*this['total_asset']+0.5*last['total_asset'])
        #  result['财务杠杆'] =  e['资产总计'][0]/(e['资产总计'][0] -  e['负债合计'][0])
        result['finance_leverage'] = this['total_asset']/(this['total_asset']-this['total_debt'])
        #  result['总资产增长率'] = e['资产总计'][0]/e['资产总计'][1] - 1
        result['total_asset_incr'] = this['total_asset']/last['total_asset'] - 1
        #  result['经营性现金率/净利润'] =e['经营活动产生的现金流量净额'][0]/ e['净利润'][0]
        result['op_cash_over_net_profit'] = this['net_cash_from_op']/this['net_profit']
        result['smr_net_profit_incr'] = this['smr_deducted_net_profit']/last['smr_deducted_net_profit'] - 1
        result['smr_avg_roe'] = this['smr_avg_roe']

        for i in result.keys():
            result[i] = round(result[i]*100,prec)
        result['net_working_cap'] = round(result['net_working_cap']/10**8/100,prec)
        result['finance_leverage'] = round(result['finance_leverage']/100,prec)
        result['total_turn_over'] = round(result['total_turn_over']/100,prec)
        result['smr_avg_roe'] = round(result['smr_avg_roe']/100,prec)

        return(result)

    def read_and_clean_v2(self, path, pages="",check_multi = True):
        # 读取pdf表格，并清理
        #  parallel_tab_flag = False
        #  pdfFileObj = open(path,'rb')
        #  pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
        if not(pages):
            pages = "all"
        df = read_pdf(path, pages = pages,silent=True, multiple_tables=True, pandas_option={'header':None})
        self._buf = df
        output=[]

        # read_pdf 返回list,对应每个表格
        # 1. 如果col数目大于5，认为表格是双排
        # 2. 对于 n/a 填充空格
        # 3. 合并每行的内容
        # 4. 清理数据
        #    a. 如果每行 少于两个 comp 则丢弃
        #       对于双排表格进行处理
        #    b. process_line 清理
        for k in range(len(df)):
            ncol = df[k].shape[1]
            if  ncol > 5:
                parallel_tab_flag = True
            df[k] = df[k].fillna(' ')
            #dataframe 变成 list
            for i in range(ncol):
                if i == 0:
                    tmp = df[k].loc[:,i]
                else:
                    tmp = tmp+' '+df[k].loc[:,i]
            res = list(tmp)

            res = [ x for x in res if len(x.split())>2] 
            res = [ self.process_line(x) for x in res ]
            res = [ x for x in res if len(x) > 2 ]
            output.append(res)

        # combine list in result
        new_out = []
        for i in output:
            new_out+=i
        new_out = self.normalize_table(new_out)
        self._buf2 = new_out

        new_output = []
        pos = self.mark_multi_table(new_out)
        if check_multi:
            flag = (len(pos['t1'])>1) or (len(pos['t2'])>1) or (len(pos['t3'])>1)
            if flag:
                new_output=[]
                for i in range(1,4):
                    m = pos['t'+str(i)]
                    if len(m) > 1:
                        start = m[0]
                        if (m[1]-m[0])==1:
                            if(len(m)>2):
                                end = m[2]
                            else:
                                if(i<2):
                                    end = pos['t'+str(i+1)][0]
                                else:
                                    end = len(new_out)-1
                        else:
                            end = m[1]
                        for j in range(start,end):
                            new_output.append(new_out[j])
                    else:
                        # 处理有的不带第二张表情况
                        # deal with not all have child table
                        pass
            new_out = new_output 
        self._buf3 = new_out
        #--------------
        # if not check multi table, the value is corrupted
        # update 不记录第二次出现数据
        e,tmp = self._formatted_list_to_ordered_dic(new_out)

        return(e)
