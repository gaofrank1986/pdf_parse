# https://blog.csdn.net/u011389474/article/details/60139786

import pdfparser.poppler as pdf
from collections import OrderedDict
from progressbar import ProgressBar,Percentage,Bar,Timer,ETA
import re
from random import randint

class PDFTarget(object):


    def __init__(self,path):
        self.path = path
        self.buf = []
        self.year= None
        self.__gap = 1

        doc = pdf.Document(path.encode())
        tmp = 0
        total = doc.no_of_pages
        print(path)
        with ProgressBar() as pbar:
            for page in doc:
                pa = []
                tmp+=1
                pbar.update(int((tmp / (total - 1)) * 100))
                for f in page:
                    for bbox in f:
                        for line in bbox:
                            pa.append(line.text)
                self.buf.append(pa)


        found = False
        for j in range(len(self.buf)):
            for i in self.buf[j]:
                if '年度报告' in i:
                    self.year = re.sub('[^0-9]','', i) 
                    if self.year.isdigit():
                        found = True
                        break
            if found:
                break
        if self.year == '':
            self.year = randint(1,1000)



    def _search_string(self,s):
        res= []
        for i in range(len(self.buf)):
            d = [(s in x)and(len(x)<len(s)+10) for x in self.buf[i]]
            if(any(d)):
                res.append(i)
        return(res)

    def _search_company_info(self,mode):
        if(mode == 2):
            pool = ['证券简称','公司简称','股票简称']
        elif(mode == 1):
            pool = ['公司代码','证券代码','股票代码']
        else:
            raise Exception("wrong mode")

        found = False
        ans = ''
        for i in self.buf[0]:
            #  for j in i.split():
            for aitem in pool:
                if aitem in i:
                    ans = i.split('：')[1]
                    found = True
                    break
            if found:
                break
        if not(found):
            tmp = self._search_string("公司信息")
            tmp = self.buf[tmp[0]] + self.buf[tmp[0]+1]
            for i in range(len(tmp)):
                for aitem in pool:
                    if aitem == tmp[i]:
                        if(mode == 1):
                            #寻找下一个纯数字string
                            counter = i
                            while(counter < len(tmp)-1):
                                if(tmp[counter+1].isdigit()):
                                     ans = tmp[counter+1]
                                     self.__gap = counter+1-i
                                     found = True
                                     break
                                counter+=1
                        else:
                            found = True
                            ans = tmp[i+self.__gap]
                    if(found):
                        break
                if(found):
                    break
        return(ans)

    def _search_unit(self):
        s = '单位：'
        found = False
        ans =''
        for page in self.buf:
            for line in page:
                if s in line:
                    if '元' in line:
                        found = True
                        res = line
                        break
            if(found):
                break
        if(found):

            for i in res.split():
                if s in i:
                    ans = i.split('：')[1]
        return(ans)


    def _get_page_range(self):
        d = ['非经常性损益项目', '主要会计数据', '合并资产负债表', '合并利润表', '合并现金流量表', '合并所有者权益变动表']
        res=[]
        for i in d:
            res.append(self._search_string(i))
        ans=[]
        try:
            ans.append((res[1][0]+1,res[0][0]+1))
        except:
            ans.append(None)
        try:
            ans.append((res[2][0]+1,res[5][0]))
        except:
            ans.append(None)
        return ans

    def generate_info(self):
        info = OrderedDict()
        info['unit']= self._search_unit()
        info['stock_id'] = self._search_company_info(mode=1)
        info['stock_name'] = self._search_company_info(mode=2)
        info['year'] = self.year
        ans = self._get_page_range()
        info['summary_pages'] = ans[0]
        info['table_pages'] = ans[1]
        return info



    #  def extract_tables(self):
        #  pg = self._get_page_range()
        #  pages = str(pg[1][0])+'-'+str(pg[1][1])
        #  r = self.util.read_and_clean(self.path, pages, check_multi = True)
        #  return(r)

    #  def extract_summary(self):
        #  pg = self._get_page_range()
        #  pages = str(pg[0][0])+'-'+str(pg[0][1])
        #  r = self.util.read_summary(self.path, pages)
        #  return(r)
