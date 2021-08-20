# coding: utf-8
import os
import timeit
import warnings
import datetime
import cx_Oracle
import pandas as pd
import numpy as np
import pymysql
import sqlalchemy
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
from icf import compute_raw_ue
from icf import mine_type
from icf import generate_base_data_all
from icf import compute_dfall
from icf_model import generate_interval_score_lx_lb
from icf_model import generate_base_inf_all
from icf_model import generate_missing_data_imputation
from icf_model import generate_comparison_matrix
from icf_model import weight_models
from icf_model import cmopute_score_all
from icf_model import deduce_score
from icf_model import pivot_table_data
from icf_model import compute_explain
from icf_model import base_raw
from icf_model import compare
from pyspark import SparkConf,SparkContext
from pyspark.sql.session import SparkSession
SparkContext.setSystemProperty('spark.driver.maxResultSize', '16g')
#from pyspark.sql.functions import *
from pyspark.sql.functions import when,regexp_replace,current_timestamp,to_timestamp,col,substring
from pyspark.sql.types import *
sc= SparkContext.getOrCreate()
spark=SparkSession(sc)
warnings.filterwarnings("ignore")

#创建数据库连接
host = "10.18.34.92"
port = "1521"
sid = "AJ1KMJ"
dsn = cx_Oracle.makedsn(host, port, sid) 
conn = cx_Oracle.connect("sfwybi", "Jtwmy6446?>", dsn)
engine1=create_engine('mysql://sfwy:Sfwy#64466446>@10.18.34.139:3306/znzf_model?charset=utf8')
engine1=engine1.connect()
engine2=create_engine('mysql://root:cdsf@119@10.18.34.226:3306/sfwy?charset=utf8')
engine2=engine2.connect()
engine3=create_engine('mysql://sifangbzhuser:Cdsf@119@10.18.33.129:3306/standardized_pro?charset=utf8') #评分系统库
engine3=engine3.connect()
engine4=create_engine('mysql://root:cdsf@119@10.18.33.227:3306/coal_mine_supervision?charset=utf8')
engine4=engine4.connect()

start=timeit.default_timer()



#pyspark连接oracle配置信息
url="jdbc:oracle:thin:@10.18.34.92:1521/AJ1KMJ" #oracle连接地址
user="sfwybi" #用户
password="Jtwmy6446?>" #密码
driver="oracle.jdbc.driver.OracleDriver" #驱动名称
#pyspark连接mysql  sfwy配置信息
urls='jdbc:mysql://10.18.34.226:3306/sfwy'
users='root'
passwords='cdsf@119'
drivers="com.mysql.cj.jdbc.Driver"
#pyspark连接mysql  znzf_model配置信息
url_znzf_model='jdbc:mysql://10.18.34.139:3306/znzf_model'
user_znzf_model='sfwy'
password_znzf_model='Sfwy#64466446>'
#pyspark连接mysql  standardized_pro配置信息
url_standardized_pro='jdbc:mysql://10.18.33.129:3306/standardized_pro'
user_standardized_pro='sifangbzhuser'
password_standardized_pro='Cdsf@119'
#pyspark连接mysql  coal_mine_supervision配置信息
url_coal_mine_supervision='jdbc:mysql://10.18.33.227:3306/coal_mine_supervision'
user_coal_mine_supervision='root'
password_coal_mine_supervision='cdsf@119'


#########################################################数据读取#########################################
#从业人员统计信息：REPORT_YEAR：报告年度；CYRYT_ZS：总数；CYRYT_ZZRS：中专及以上人数；CYRYT_GZ：高中；CYRYT_CZ：初中；CYRYT_CZYX：初中以下；DEL_FLAG：删除标记
#T_AJ1_MJ_JCXX_CYRYTJ = pd.read_sql('select CORP_ID,REPORT_YEAR,CYRYT_ZS,CYRYT_ZZRS,CYRYT_GZ,CYRYT_CZ,CYRYT_CZYX,DEL_FLAG from mj.T_AJ1_MJ_JCXX_CYRYTJ',conn)
#spark code
query = "(select CORP_ID,REPORT_YEAR,CYRYT_ZS,CYRYT_ZZRS,CYRYT_GZ,CYRYT_CZ,CYRYT_CZYX,DEL_FLAG from mj.T_AJ1_MJ_JCXX_CYRYTJ) emp"
T_AJ1_MJ_JCXX_CYRYTJ=spark.read.format("jdbc").option("url",url).option("dbtable",query).option("user", user).option("password", password).option("driver",driver).load().toPandas()

for ft in ['CYRYT_ZS','CYRYT_ZZRS','CYRYT_GZ','CYRYT_CZ','CYRYT_CZYX']:
    T_AJ1_MJ_JCXX_CYRYTJ[ft]=T_AJ1_MJ_JCXX_CYRYTJ[ft].astype('float64')

#专业技术人员统计信息：ZYJSR_SZRS：大专人数；ZYJSR_BKYS：本科及以上人数；A_ZYJSR_ZCGC：高级；A_ZYJSR_ZCZJ：中级；A_ZYJSR_ZCCJ：初级；A_ZYJSR_ZCJX：见习
#T_AJ1_MJ_JCXX_ZYJSRYTJ = pd.read_sql('select CORP_ID,REPORT_YEAR,ZYJSR_SZRS,ZYJSR_BKYS,A_ZYJSR_ZCGC,A_ZYJSR_ZCZJ,A_ZYJSR_ZCCJ,A_ZYJSR_ZCJX,DEL_FLAG from mj.T_AJ1_MJ_JCXX_ZYJSRYTJ',conn)
#spark code
query = "(select CORP_ID,REPORT_YEAR,ZYJSR_SZRS,ZYJSR_BKYS,A_ZYJSR_ZCGC,A_ZYJSR_ZCZJ,A_ZYJSR_ZCCJ,A_ZYJSR_ZCJX,DEL_FLAG from mj.T_AJ1_MJ_JCXX_ZYJSRYTJ) emp"
T_AJ1_MJ_JCXX_ZYJSRYTJ=spark.read.format("jdbc").option("url",url).option("dbtable",query).option("user", user).option("password", password).option("driver",driver).load().toPandas()

for ft in ['ZYJSR_SZRS','ZYJSR_BKYS','A_ZYJSR_ZCGC','A_ZYJSR_ZCZJ','A_ZYJSR_ZCCJ','A_ZYJSR_ZCJX']:
    T_AJ1_MJ_JCXX_ZYJSRYTJ[ft]=T_AJ1_MJ_JCXX_ZYJSRYTJ[ft].astype('float64')

#安全管理人员统计信息：AQGLR_FZRPX：负责人培训数；AQGLR_GLPX：安全生产管理人员培训数,AQGLR_ZYFZRS：负责人数,AQGLR_ZYFZR：负责人持证数（安全资格证）,AQGLR_TZZY：特种作业人员数,AQGLR_CZS：持证数（特种作业资格证）,AQGLR_AQSC：安全生产管理人员数,AQGLR_AQSCGL：安全生产管理人员持证数（管理人员安全资格证）,AQGLR_TZPX：特种作业人员培训数
#T_AJ1_MJ_JCXX_AQGLRYTJ = pd.read_sql('select CORP_ID,REPORT_YEAR,AQGLR_FZRPX,AQGLR_GLPX,AQGLR_ZYFZRS,AQGLR_ZYFZR,AQGLR_TZZY,AQGLR_CZS,AQGLR_AQSC,AQGLR_AQSCGL,AQGLR_TZPX,DEL_FLAG from mj.T_AJ1_MJ_JCXX_AQGLRYTJ',conn)
query = "(select CORP_ID,REPORT_YEAR,AQGLR_FZRPX,AQGLR_GLPX,AQGLR_ZYFZRS,AQGLR_ZYFZR,AQGLR_TZZY,AQGLR_CZS,AQGLR_AQSC,AQGLR_AQSCGL,AQGLR_TZPX,DEL_FLAG from mj.T_AJ1_MJ_JCXX_AQGLRYTJ) emp"
T_AJ1_MJ_JCXX_AQGLRYTJ=spark.read.format("jdbc").option("url",url).option("dbtable",query).option("user", user).option("password", password).option("driver",driver).load().toPandas()

for ft in ['AQGLR_FZRPX','AQGLR_GLPX','AQGLR_ZYFZRS','AQGLR_ZYFZR','AQGLR_TZZY','AQGLR_CZS','AQGLR_AQSC','AQGLR_AQSCGL','AQGLR_TZPX']:
    T_AJ1_MJ_JCXX_AQGLRYTJ[ft]=T_AJ1_MJ_JCXX_AQGLRYTJ[ft].astype('float64')

#主要安全管理人员信息：A_PXKHQK：培训考核情况
#T_AJ1_MJ_JCXX_AQGLRYPB = pd.read_sql('select CORP_ID,UPDATE_DATE,A_PXKHQK,DEL_FLAG from mj.T_AJ1_MJ_JCXX_AQGLRYPB',conn)
query = "(select CORP_ID,UPDATE_DATE,A_PXKHQK,DEL_FLAG from mj.T_AJ1_MJ_JCXX_AQGLRYPB) emp"
T_AJ1_MJ_JCXX_AQGLRYPB=spark.read.format("jdbc").option("url",url).option("dbtable",query).option("user", user).option("password", password).option("driver",driver).load().toPandas()

#安全费用管理信息：AQFYT_TQJE：,AQFYT_PJDM：,AQFYT_SYYE：,A_AQFYT_JYJE：
#T_AJ1_MJ_JCXX_AQFYGL = pd.read_sql('select CORP_ID,REPORT_YEAR,AQFYT_TQJE,AQFYT_PJDM,AQFYT_SYYE,A_AQFYT_JYJE,DEL_FLAG from mj.T_AJ1_MJ_JCXX_AQFYGL',conn)
query = "(select CORP_ID,REPORT_YEAR,AQFYT_TQJE,AQFYT_PJDM,AQFYT_SYYE,A_AQFYT_JYJE,DEL_FLAG from mj.T_AJ1_MJ_JCXX_AQFYGL) emp"
T_AJ1_MJ_JCXX_AQFYGL=spark.read.format("jdbc").option("url",url).option("dbtable",query).option("user", user).option("password", password).option("driver",driver).load().toPandas()

for ft in ['AQFYT_TQJE','AQFYT_PJDM','AQFYT_SYYE','A_AQFYT_JYJE']:
    T_AJ1_MJ_JCXX_AQFYGL[ft]=T_AJ1_MJ_JCXX_AQFYGL[ft].astype('float64')

#瓦斯综合防治管理信息：GASGEN_TENICHNUM：“一通三防”管理人员数量；GASGEB_BURSTWORKERNUM：防突施工人员数量
#T_AJ1_MJ_JCXX_WSZHFZGL = pd.read_sql('select CORP_ID,UPDATE_DATE,GASGEN_TENICHNUM,GASGEB_BURSTWORKERNUM,DEL_FLAG from mj.T_AJ1_MJ_JCXX_WSZHFZGL',conn)
query = "(select CORP_ID,UPDATE_DATE,GASGEN_TENICHNUM,GASGEB_BURSTWORKERNUM,DEL_FLAG from mj.T_AJ1_MJ_JCXX_WSZHFZGL) emp"
T_AJ1_MJ_JCXX_WSZHFZGL=spark.read.format("jdbc").option("url",url).option("dbtable",query).option("user", user).option("password", password).option("driver",driver).load().toPandas()

for ft in ['GASGEN_TENICHNUM','GASGEB_BURSTWORKERNUM']:
    T_AJ1_MJ_JCXX_WSZHFZGL[ft]=T_AJ1_MJ_JCXX_WSZHFZGL[ft].astype('float64')

#采煤工作面进度信息：CMGZM_JCL：机采率,CMGZM_PCGS：炮采个数,CMGZM_PC：普采个数,CMGZM_ZCGS：综采个数,CMGZM_HCL：回采率,CMGZM_ZDHGS：自动化开采个数
#T_AJ1_MJ_JCXX_CMGZMJD = pd.read_sql('select CORP_ID,REPORT_YEAR,CMGZM_JCL,CMGZM_PCGS,CMGZM_PC,CMGZM_ZCGS,CMGZM_HCL,CMGZM_ZDHGS,DEL_FLAG from mj.T_AJ1_MJ_JCXX_CMGZMJD',conn)
query = "(select CORP_ID,REPORT_YEAR,CMGZM_JCL,CMGZM_PCGS,CMGZM_PC,CMGZM_ZCGS,CMGZM_HCL,CMGZM_ZDHGS,DEL_FLAG from mj.T_AJ1_MJ_JCXX_CMGZMJD) emp"
T_AJ1_MJ_JCXX_CMGZMJD=spark.read.format("jdbc").option("url",url).option("dbtable",query).option("user", user).option("password", password).option("driver",driver).load().toPandas()

for ft in ['CMGZM_JCL','CMGZM_PCGS','CMGZM_PC','CMGZM_ZCGS','CMGZM_HCL','CMGZM_ZDHGS']:
    T_AJ1_MJ_JCXX_CMGZMJD[ft]=T_AJ1_MJ_JCXX_CMGZMJD[ft].astype('float64')

#瓦斯抽放利用信息指标项：GASUTIL_CONMAX：最大瓦斯抽放浓度,GASUTIL_CONMIN：最小瓦斯抽放浓度,GASUTIL_CONAVG：平均瓦斯抽放浓度,GASUTIL_PREDESIGN：设计瓦斯抽放负压,GASUTIL_PREMAX：最大瓦斯抽放负压,GASUTIL_PREMIN：最小瓦斯抽放负压,GASUTIL_PREAVG：平均瓦斯抽放负压
#T_AJ1_MJ_JCXX_WSCFLYGL = pd.read_sql('select CORP_ID,REPORT_YEAR,GASUTIL_CONMAX,GASUTIL_CONMIN,GASUTIL_CONAVG,GASUTIL_PREDESIGN,GASUTIL_PREMAX,GASUTIL_PREMIN,GASUTIL_PREAVG,DEL_FLAG from mj.T_AJ1_MJ_JCXX_WSCFLYGL',conn)
query = "(select CORP_ID,REPORT_YEAR,GASUTIL_CONMAX,GASUTIL_CONMIN,GASUTIL_CONAVG,GASUTIL_PREDESIGN,GASUTIL_PREMAX,GASUTIL_PREMIN,GASUTIL_PREAVG,DEL_FLAG from mj.T_AJ1_MJ_JCXX_WSCFLYGL) emp"
T_AJ1_MJ_JCXX_WSCFLYGL=spark.read.format("jdbc").option("url",url).option("dbtable",query).option("user", user).option("password", password).option("driver",driver).load().toPandas()

for ft in ['GASUTIL_CONMAX', 'GASUTIL_CONMIN','GASUTIL_CONAVG','GASUTIL_PREDESIGN','GASUTIL_PREMAX','GASUTIL_PREMIN','GASUTIL_PREAVG']:
    T_AJ1_MJ_JCXX_WSCFLYGL[ft]=T_AJ1_MJ_JCXX_WSCFLYGL[ft].astype('float64')

#事故上报_事故基本信息表：HYDROGEOLOGICAL_TYPE：水文地质类型,ACCIDENT_LEVEL：事故等级,ACCIDENT_CATEGORY：事故类别,HURT_PEOPLE_COLL_ID：事故受伤人员统计记录编号,MINE_WELL_TYPE：矿井井型,MONITOR_STATUS：监控设备,HIDDEN_CATEGORY：瞒报
#T_AJ1_MJ_SG_ACCIDENT_INFO = pd.read_sql('select ACCIDENT_MINE_ID,UPDATE_DATE,HYDROGEOLOGICAL_TYPE,ACCIDENT_LEVEL,ACCIDENT_CATEGORY,HURT_PEOPLE_COLL_ID,MINE_WELL_TYPE,MONITOR_STATUS,HIDDEN_CATEGORY,DEL_FLAG from mj.T_AJ1_MJ_SG_ACCIDENT_INFO',conn)
query = "(select ACCIDENT_MINE_ID,UPDATE_DATE,HYDROGEOLOGICAL_TYPE,ACCIDENT_LEVEL,ACCIDENT_CATEGORY,HURT_PEOPLE_COLL_ID,MINE_WELL_TYPE,MONITOR_STATUS,HIDDEN_CATEGORY,DEL_FLAG from mj.T_AJ1_MJ_SG_ACCIDENT_INFO) emp"
T_AJ1_MJ_SG_ACCIDENT_INFO=spark.read.format("jdbc").option("url",url).option("dbtable",query).option("user", user).option("password", password).option("driver",driver).load().toPandas()

#煤层信息：MINE_THICK_AVG：煤层平均厚度,MINE_THICK_MAX：煤层最大厚度,A_SFMCJJ：上覆煤层间距,A_XFMCJJ：下伏煤层间距,A_MCPJQJ：煤层平均倾角
#T_AJ1_MJ_JCXX_ZCMC = pd.read_sql('select CORP_ID,UPDATE_DATE,MINE_THICK_AVG,MINE_THICK_MAX,A_SFMCJJ,A_XFMCJJ,A_MCPJQJ,DEL_FLAG from mj.T_AJ1_MJ_JCXX_ZCMC',conn)
query = "(select CORP_ID,UPDATE_DATE,MINE_THICK_AVG,MINE_THICK_MAX,A_SFMCJJ,A_XFMCJJ,A_MCPJQJ,DEL_FLAG from mj.T_AJ1_MJ_JCXX_ZCMC) emp"
T_AJ1_MJ_JCXX_ZCMC=spark.read.format("jdbc").option("url",url).option("dbtable",query).option("user", user).option("password", password).option("driver",driver).load().toPandas()

for ft in ['MINE_THICK_AVG','MINE_THICK_MAX','A_SFMCJJ','A_XFMCJJ','A_MCPJQJ']:
    T_AJ1_MJ_JCXX_ZCMC[ft]=T_AJ1_MJ_JCXX_ZCMC[ft].astype('float64')

#事故上报_事故人员伤亡统计信息表：DIE_PEOPLE_NUM：事故死亡人数,WEIGHT_HURT_PEOPLE_NUM：事故重伤人数,LIGHT_HURT_PEOPLE_NUM：事故轻伤人数
#SG_HURT_PEOPLE_COLL_INFO = pd.read_sql('select ID,DIE_PEOPLE_NUM,WEIGHT_HURT_PEOPLE_NUM,LIGHT_HURT_PEOPLE_NUM,DEL_FLAG from mj.SG_HURT_PEOPLE_COLL_INFO',conn)
query = "(select ID,DIE_PEOPLE_NUM,WEIGHT_HURT_PEOPLE_NUM,LIGHT_HURT_PEOPLE_NUM,DEL_FLAG from mj.SG_HURT_PEOPLE_COLL_INFO) emp"
SG_HURT_PEOPLE_COLL_INFO=spark.read.format("jdbc").option("url",url).option("dbtable",query).option("user", user).option("password", password).option("driver",driver).load().toPandas()

for ft in ['DIE_PEOPLE_NUM', 'WEIGHT_HURT_PEOPLE_NUM','LIGHT_HURT_PEOPLE_NUM']:
    SG_HURT_PEOPLE_COLL_INFO[ft]=SG_HURT_PEOPLE_COLL_INFO[ft].astype('float64')

#例如在H_DIE_PEOPLE_NUM表新增，读取H_DIE_PEOPLE_NUM字段，格式规范：CORP_ID+字段名称，例如新增H_DIE_PEOPLE_NUM字段，SQL写法如下所示，或者处理成下面这种格式
#H_DIE_PEOPLE_NUM = pd.read_sql('select CORP_ID,H_DIE_PEOPLE_NUM from mj.H_DIE_PEOPLE_NUM',conn)

#文书基本信息：NAME：名称,P8_PENALTY：罚款总额,PAPER_TYPE：文书类型,CASE_ID：执法活动ID,PAPER_ID：文书ID
#jczf_paper = pd.read_sql('select CORP_ID,NAME,P8_PENALTY,PAPER_TYPE,CASE_ID,PAPER_ID,DEL_FLAG from mj.jczf_paper',conn)
query = "(select CORP_ID,NAME,P8_PENALTY,PAPER_TYPE,CASE_ID,PAPER_ID,DEL_FLAG from mj.jczf_paper) emp"
jczf_paper=spark.read.format("jdbc").option("url",url).option("dbtable",query).option("user", user).option("password", password).option("driver",driver).load().toPandas()

#执法基本信息
#jczf_case = pd.read_sql('select CASE_ID,DEL_FLAG from mj.jczf_case',conn)
query = "(select CASE_ID,DEL_FLAG from mj.jczf_case) emp"
jczf_case=spark.read.format("jdbc").option("url",url).option("dbtable",query).option("user", user).option("password", password).option("driver",driver).load().toPandas()

#隐患基本信息：ONSITE_TYPE：现场处理决定类型,PENALTY_TYPE：行政处罚类型,IS_HIGH：是否(引发)重大隐患（1表示是重大隐患，0表示不是）,COALING_FACE：采煤工作面个数,HEADING_FACE：掘进工作面个数
#jczf_danger = pd.read_sql('select PAPER_ID,ONSITE_TYPE,PENALTY_TYPE,IS_HIGH,COALING_FACE,HEADING_FACE,DEL_FLAG from mj.jczf_danger',conn)
query = "(select PAPER_ID,ONSITE_TYPE,PENALTY_TYPE,IS_HIGH,COALING_FACE,HEADING_FACE,DEL_FLAG from mj.jczf_danger) emp"
jczf_danger=spark.read.format("jdbc").option("url",url).option("dbtable",query).option("user", user).option("password", password).option("driver",driver).load().toPandas()

#隐患排查信息指标项：YHPCZ_ZDYHS：重大隐患数
#T_AJ1_MJ_JCXX_YHPCXX = pd.read_sql('select CORP_ID,YHPCZ_ZDYHS,DEL_FLAG from mj.T_AJ1_MJ_JCXX_YHPCXX',conn)
query = "(select CORP_ID,YHPCZ_ZDYHS,DEL_FLAG from mj.T_AJ1_MJ_JCXX_YHPCXX) emp"
T_AJ1_MJ_JCXX_YHPCXX=spark.read.format("jdbc").option("url",url).option("dbtable",query).option("user", user).option("password", password).option("driver",driver).load().toPandas()
T_AJ1_MJ_JCXX_YHPCXX['YHPCZ_ZDYHS']=T_AJ1_MJ_JCXX_YHPCXX['YHPCZ_ZDYHS'].astype('float64')

#NDCLX_CL：产量
#T_AJ1_MJ_JCXX_CLXX = pd.read_sql('select CORP_ID,UPDATE_DATE,NDCLX_CL,DEL_FLAG from mj.T_AJ1_MJ_JCXX_CLXX',conn)
query = "(select CORP_ID,UPDATE_DATE,NDCLX_CL,DEL_FLAG from mj.T_AJ1_MJ_JCXX_CLXX) emp"
T_AJ1_MJ_JCXX_CLXX=spark.read.format("jdbc").option("url",url).option("dbtable",query).option("user", user).option("password", password).option("driver",driver).load().toPandas()
T_AJ1_MJ_JCXX_CLXX['NDCLX_CL']=T_AJ1_MJ_JCXX_CLXX['NDCLX_CL'].astype('float64')

#井下安全避险系统信息：LDXTX_ZT：状态,LDXTX_XTLX：系统类型
#T_AJ1_MJ_JCXX_JXAQBXXTXX = pd.read_sql('select CORP_ID,UPDATE_DATE,LDXTX_ZT,LDXTX_XTLX,DEL_FLAG from mj.T_AJ1_MJ_JCXX_JXAQBXXTXX',conn)
query = "(select CORP_ID,UPDATE_DATE,LDXTX_ZT,LDXTX_XTLX,DEL_FLAG from mj.T_AJ1_MJ_JCXX_JXAQBXXTXX) emp"
T_AJ1_MJ_JCXX_JXAQBXXTXX=spark.read.format("jdbc").option("url",url).option("dbtable",query).option("user", user).option("password", password).option("driver",driver).load().toPandas()

#读取视图基础表
query = "(select * from mj.V_AJ1_MJ_JCXX_BASEINFO) emp"
V_AJ1_MJ_JCXX_BASEINFO=spark.read.format("jdbc").option("url",url).option("dbtable",query).option("user", user).option("password", password).option("driver",driver).load()
V_AJ1_MJ_JCXX_BASEINFO=pd.read_sql_query('select * from mj.V_AJ1_MJ_JCXX_BASEINFO',conn)
V_AJ1_MJ_JCXX_BASEINFO.columns=list(map(lambda x: x.upper(),list(V_AJ1_MJ_JCXX_BASEINFO.columns)))
V_AJ1_MJ_JCXX_BASEINFO['CORP_ID']=V_AJ1_MJ_JCXX_BASEINFO['CORP_ID'].astype('str')

#################################读取mysql需要的表##############################
#类案屡查屡犯表
#lclf=pd.read_sql_query('select CORP_ID,lclf from similar_data_behave_counties',engine2)
#lclf=lclf.rename(columns={'corp_id':'CORP_ID'})
query = "(select CORP_ID,lclf from similar_data_behave_counties) emp"
lclf=spark.read.format("jdbc").option("url",urls).option("dbtable",query).option("user", users).option("password", passwords).option("driver",drivers).load().toPandas()
lclf=lclf.rename(columns={'corp_id':'CORP_ID'})

#中英文对应及指标编码表
#indictor_config=pd.read_sql_query('select * from indictor_config',engine1)
query = "(select * from indictor_config) emp"
indictor_config=spark.read.format("jdbc").option("url",url_znzf_model).option("dbtable",query).option("user", user_znzf_model).option("password", password_znzf_model).option("driver",drivers).load().toPandas()

#model_detail：模型明细表
#model_detail=pd.read_sql_query('select * from model_detail',engine1)
query = "(select * from model_detail) emp"
model_detail=spark.read.format("jdbc").option("url",url_znzf_model).option("dbtable",query).option("user", user_znzf_model).option("password", password_znzf_model).option("driver",drivers).load().toPandas()

#得分解释原始表
#df_explain=pd.read_sql_query('select * from df_explain',engine1)
query = "(select * from df_explain) emp"
df_explain=spark.read.format("jdbc").option("url",url_znzf_model).option("dbtable",query).option("user", user_znzf_model).option("password", password_znzf_model).option("driver",drivers).load().toPandas()

#等级调整信息表：YDJ：原等级,XDJ：新等级
#t_bzh_jj = pd.read_sql('select CORP_ID,YDJ,XDJ,DEL_FLAG from t_bzh_jj',engine3)
query = "(select CORP_ID,YDJ,XDJ,DEL_FLAG from t_bzh_jj) emp"
t_bzh_jj=spark.read.format("jdbc").option("url",url_standardized_pro).option("dbtable",query).option("user", user_standardized_pro).option("password", password_standardized_pro).option("driver",drivers).load().toPandas()

#################################################数据过滤###################################
jczf_case = jczf_case[jczf_case['DEL_FLAG'] == '0']
jczf_danger = jczf_danger[jczf_danger['DEL_FLAG'] == '0']
jczf_paper = jczf_paper[jczf_paper['DEL_FLAG'] == '0']
SG_HURT_PEOPLE_COLL_INFO = SG_HURT_PEOPLE_COLL_INFO[SG_HURT_PEOPLE_COLL_INFO['DEL_FLAG'] == '0']
T_AJ1_MJ_JCXX_AQFYGL = T_AJ1_MJ_JCXX_AQFYGL[T_AJ1_MJ_JCXX_AQFYGL['DEL_FLAG'] == '0']
T_AJ1_MJ_JCXX_AQGLRYPB = T_AJ1_MJ_JCXX_AQGLRYPB[T_AJ1_MJ_JCXX_AQGLRYPB['DEL_FLAG'] == '0']
T_AJ1_MJ_JCXX_AQGLRYTJ = T_AJ1_MJ_JCXX_AQGLRYTJ[T_AJ1_MJ_JCXX_AQGLRYTJ['DEL_FLAG'] == '0']
T_AJ1_MJ_JCXX_CMGZMJD = T_AJ1_MJ_JCXX_CMGZMJD[T_AJ1_MJ_JCXX_CMGZMJD['DEL_FLAG'] == '0']
T_AJ1_MJ_JCXX_CYRYTJ = T_AJ1_MJ_JCXX_CYRYTJ[T_AJ1_MJ_JCXX_CYRYTJ['DEL_FLAG'] == '0']
T_AJ1_MJ_JCXX_WSCFLYGL = T_AJ1_MJ_JCXX_WSCFLYGL[T_AJ1_MJ_JCXX_WSCFLYGL['DEL_FLAG'] == '0']
T_AJ1_MJ_JCXX_WSZHFZGL = T_AJ1_MJ_JCXX_WSZHFZGL[T_AJ1_MJ_JCXX_WSZHFZGL['DEL_FLAG'] == '0']
T_AJ1_MJ_JCXX_YHPCXX = T_AJ1_MJ_JCXX_YHPCXX[T_AJ1_MJ_JCXX_YHPCXX['DEL_FLAG'] == '0']
T_AJ1_MJ_JCXX_ZCMC = T_AJ1_MJ_JCXX_ZCMC[T_AJ1_MJ_JCXX_ZCMC['DEL_FLAG'] == '0']
T_AJ1_MJ_JCXX_ZYJSRYTJ = T_AJ1_MJ_JCXX_ZYJSRYTJ[T_AJ1_MJ_JCXX_ZYJSRYTJ['DEL_FLAG'] == '0']
T_AJ1_MJ_SG_ACCIDENT_INFO = T_AJ1_MJ_SG_ACCIDENT_INFO[T_AJ1_MJ_SG_ACCIDENT_INFO['DEL_FLAG'] == '0']
T_AJ1_MJ_JCXX_JXAQBXXTXX = T_AJ1_MJ_JCXX_JXAQBXXTXX[T_AJ1_MJ_JCXX_JXAQBXXTXX['DEL_FLAG'] == '0']
T_AJ1_MJ_JCXX_CLXX = T_AJ1_MJ_JCXX_CLXX[T_AJ1_MJ_JCXX_CLXX['DEL_FLAG'] == '0']
t_bzh_jj = t_bzh_jj[t_bzh_jj['DEL_FLAG'] == '0']

####################################宽表加工及生成#####################################
#生成基础信息
total_frame=V_AJ1_MJ_JCXX_BASEINFO[['CORP_ID','CORP_NAME','MINE_MINETYPE_NAME','MINE_CLASS_NAME','PROVINCE_NAME','OUTPUT']]
total_frame=mine_type(total_frame)
total_frame.drop(columns=['OUTPUT'],inplace=True)
print(total_frame.shape)
###如果新增表字段，先在这里做新增格式拼接，例如将新增表命名对象名称为H_DIE_PEOPLE_NUM，要求新表的CORP_ID为str类型
##H_DIE_PEOPLE_NUM['CORP_ID']=H_DIE_PEOPLE_NUM['CORP_ID'].astype('str') #如果新表CORP_ID不是str类型，转换成str类型
#H_DIE_PEOPLE_NUM=pd.merge(total_frame,H_DIE_PEOPLE_NUM,on=['CORP_ID'],how='left')


#生成UE展示需要的原始数据
model_raw_values=compute_raw_ue(T_AJ1_MJ_JCXX_CYRYTJ,T_AJ1_MJ_JCXX_ZYJSRYTJ,T_AJ1_MJ_JCXX_AQGLRYTJ,T_AJ1_MJ_JCXX_AQGLRYPB,T_AJ1_MJ_JCXX_AQFYGL,T_AJ1_MJ_JCXX_WSZHFZGL,T_AJ1_MJ_JCXX_CMGZMJD,T_AJ1_MJ_JCXX_WSCFLYGL,T_AJ1_MJ_SG_ACCIDENT_INFO,T_AJ1_MJ_JCXX_ZCMC,SG_HURT_PEOPLE_COLL_INFO,jczf_paper,jczf_case,jczf_danger,T_AJ1_MJ_JCXX_YHPCXX,T_AJ1_MJ_JCXX_CLXX,T_AJ1_MJ_JCXX_JXAQBXXTXX,lclf,total_frame,indictor_config,V_AJ1_MJ_JCXX_BASEINFO)
model_raw_values.columns=['mine_id','mine_name','point_id','POINT_NAME','raw_score']
print('model_raw_values:',model_raw_values.shape)
#新增字段后，UE展示
# def new_transform(model_raw_values,H_DIE_PEOPLE_NUM,indictor_config)
    # ll=[]
    # df=H_DIE_PEOPLE_NUM.drop(columns=['MINE_MINETYPE_NAME','MINE_CLASS_NAME','PROVINCE_NAME'])
    # for ft in list(df.columns[2:]):
        # dt=df[list(df.columns[:2])+[ft]]
        # dt['INDEX_NAME']=ft
        # newcol=['mine_id','mine_name']+['point_scorp','INDEX_NAME']
        # dt.columns=newcol
        # ncol=['mine_id','mine_name']+['INDEX_NAME','point_scorp']
        # dt=dt[ncol]
        # ll.append(dt)
    # dfnew=pd.concat(ll,axis=0,join='inner')
    # #读取码表
    # indictor_config_all=indictor_config[['ID','POINT_CODE','POINT_NAME']]
    # indictor_config_all.columns=['point_id','INDEX_NAME','POINT_NAME']
    # #关联码表
    # dfend=pd.merge(dfnew,indictor_config_all,on=['INDEX_NAME'],how='inner')
    # dfend=dfend[['mine_id','mine_name','point_id','POINT_NAME','point_scorp']]    
    # #得到没有去掉百分号的结果
    # dfends=pd.concat([model_raw_values,dfend],axis=0,join='inner')
	# dfends.fillna('未填报',inplace=True)
    # return dfends
#调用函数再次执行得到model_raw_values
#model_raw_values=new_transform(model_raw_values,H_DIE_PEOPLE_NUM,indictor_config)


#生成基础宽表
dfall=compute_dfall(T_AJ1_MJ_JCXX_CYRYTJ,T_AJ1_MJ_JCXX_ZYJSRYTJ,T_AJ1_MJ_JCXX_AQGLRYTJ,T_AJ1_MJ_JCXX_AQGLRYPB,T_AJ1_MJ_JCXX_AQFYGL,T_AJ1_MJ_JCXX_WSZHFZGL,T_AJ1_MJ_JCXX_CMGZMJD,T_AJ1_MJ_JCXX_WSCFLYGL,T_AJ1_MJ_SG_ACCIDENT_INFO,T_AJ1_MJ_JCXX_ZCMC,SG_HURT_PEOPLE_COLL_INFO,jczf_paper,jczf_case,jczf_danger,T_AJ1_MJ_JCXX_YHPCXX,T_AJ1_MJ_JCXX_CLXX,T_AJ1_MJ_JCXX_JXAQBXXTXX,lclf,total_frame,indictor_config,V_AJ1_MJ_JCXX_BASEINFO)
print('dfall:',dfall.shape)
#如果新增字段，需要在dfall上面做join,新表输入要求：
#基础部分：'CORP_ID',新增字段名：'H_DIE_PEOPLE_NUM'
#生成新的dfall
#dfall=pd.merge(dfall,H_DIE_PEOPLE_NUM,on=['CORP_ID'],how='left')

#生成转换后的原始数据base_data_all
base_data_all=generate_base_data_all(dfall)
print('base_data_all:',base_data_all.shape)


###################################煤矿安全生产评估模型构建#####################
#生成三级指标得分和区间得分表
dfall_class = pd.merge(dfall, V_AJ1_MJ_JCXX_BASEINFO[['CORP_NAME', 'MINE_CLASSIFY_NAME']], on=['CORP_NAME'], how='left')
# 输出表interval_score_lx_lb
interval_score_lx_lb = generate_interval_score_lx_lb(dfall, indictor_config,dfall_class,'entropy')
#interval_score_lx_lb.loc[interval_score_lx_lb['LOWER']==-1,'LOWER']=0
print('interval_score_lx_lb:',interval_score_lx_lb.shape)
# 输出表 base_inf_all
base_inf_all = generate_base_inf_all(dfall,indictor_config,dfall_class,dt_binning='entropy')
print('base_inf_all:',base_inf_all.shape)
#生成缺失值填充表
missing_data_imputation = generate_missing_data_imputation(dfall, indictor_config)
print('missing_data_imputation:',missing_data_imputation.shape)
#生成权重表model_detail_all
#生成判断矩阵
comparison_matrices = generate_comparison_matrix(model_detail)
#print(comparison_matrices)
#生成三个模型权重表
model_detail_all=weight_models(indictor_config,model_detail,base_inf_all,df_all=dfall,df_all_class=dfall_class,use_supervision_method=True)
print('model_detail_all:',model_detail_all.shape)
#生成得分表model_score_all
model_score_all=cmopute_score_all(base_inf_all,model_detail_all,indictor_config)
print('model_score_all:',model_score_all.shape)

#围绕UE展示输出几个表的计算
#三级指标得分表
base_inf=base_inf_all[base_inf_all['MODEL_NAME']=='层次分析法'][['point_id','POINT_NAME','mine_id','mine_name','point_scorp']]
print('base_inf:',base_inf.shape)
#一二级和总分初始表
model_score_raw=model_score_all[model_score_all['MODEL_NAME']=='层次分析法'][['point_id','POINT_NAME','mine_id','mine_name','point_scorp']]
print('model_score_raw:',model_score_raw.shape)
#调用函数初步生成扣分数据
deduce=deduce_score(t_bzh_jj,dfall)
print('deduce:',deduce.shape)
#计算最终总分
#生成最终一二级和总分及文字描述初步表
model_score=pivot_table_data(deduce,model_score_raw)
print('model_score:',model_score.shape)
#文字解释和最终扣分表
#生成一级指标文字解释
yiji_explain,deduction=compute_explain(model_score,df_explain,deduce)
print('yiji_explain:',yiji_explain.shape)
print('deduction:',deduction.shape)
#UE展示原始值和得分合并一张表
base_inf_raw_values= base_raw(model_raw_values,base_inf,model_detail_all)
print('base_inf_raw_values:',base_inf_raw_values.shape)

#############################################################写入数据到评分系统数据库-spark方式写入#######################################################
#indictor_config_all
indictor_config=spark.createDataFrame(indictor_config)
indictor_config=indictor_config.replace('nan',None)
indictor_config.write.format("jdbc").mode("overwrite").option("dbtable","indictor_config_all").option("url",url_coal_mine_supervision).option("user",user_coal_mine_supervision).option("password",password_coal_mine_supervision).save()

#base_data_all
#存在混合类型列，需要先做处理再写入数据库
base_data_all['INDEX_VALUE']=base_data_all['INDEX_VALUE'].astype('str')
base_data_all=spark.createDataFrame(base_data_all)
base_data_all=base_data_all.replace('nan',None)
base_data_all.write.format("jdbc").mode("overwrite").option("dbtable","base_data_all").option("url",url_coal_mine_supervision).option("user",user_coal_mine_supervision).option("password",password_coal_mine_supervision).save()

#spark_df1=spark.read.format("jdbc").option("url",url_coal_mine_supervision).option("dbtable","spark_df").option("user", user_coal_mine_supervision).option("password", password_coal_mine_supervision).option("driver",drivers).load().toPandas()

#base_inf_all
base_inf_all=spark.createDataFrame(base_inf_all)
base_inf_all=base_inf_all.replace('nan',None)
base_inf_all.write.format("jdbc").mode("overwrite").option("dbtable","base_inf_all").option("url",url_coal_mine_supervision).option("user",user_coal_mine_supervision).option("password",password_coal_mine_supervision).save()

#model_detail_all
model_detail_all=spark.createDataFrame(model_detail_all)
model_detail_all=model_detail_all.replace('nan',None)
model_detail_all.write.format("jdbc").mode("overwrite").option("dbtable","model_detail_all").option("url",url_coal_mine_supervision).option("user",user_coal_mine_supervision).option("password",password_coal_mine_supervision).save()

#interval_score_lx_lb
for ft in interval_score_lx_lb.columns:
    interval_score_lx_lb[ft]=interval_score_lx_lb[ft].astype('str')
#interval_score_lx_lb['LOWER']=interval_score_lx_lb['LOWER'].astype('str')
#interval_score_lx_lb['UPPER']=interval_score_lx_lb['UPPER'].astype('str')
interval_score_lx_lb=interval_score_lx_lb.fillna('-111999.0')
interval_score_lx_lb=spark.createDataFrame(interval_score_lx_lb)
interval_score_lx_lb=interval_score_lx_lb.replace('nan',None)
#interval_score_lx_lb=interval_score_lx_lb.replace('-111999','')
#interval_score_lx_lb=interval_score_lx_lb.withColumn('UPPER',when(interval_score_lx_lb['UPPER']=='-111999',None).otherwise(interval_score_lx_lb['UPPER']))
for c in interval_score_lx_lb.columns:
    interval_score_lx_lb=interval_score_lx_lb.withColumn(c,when(interval_score_lx_lb[c]=='-111999.0',None).otherwise(interval_score_lx_lb[c]))
interval_score_lx_lb.write.format("jdbc").mode("overwrite").option("dbtable","interval_score_lx_lb").option("url",url_coal_mine_supervision).option("user",user_coal_mine_supervision).option("password",password_coal_mine_supervision).save()
interval_score_lx_lb=interval_score_lx_lb.replace('None','')
print('interval_score_lx_lb_success')

#missing_data_imputation
for ft in missing_data_imputation.columns:
    missing_data_imputation[ft]=missing_data_imputation[ft].astype('str')
for ft in missing_data_imputation.columns:
    if ft not in ['type1','type2','mode_z']:
        missing_data_imputation=missing_data_imputation.fillna('-111999.0')
missing_data_imputation=spark.createDataFrame(missing_data_imputation)
missing_data_imputation=missing_data_imputation.replace('nan',None)
#转化为空值
for c in missing_data_imputation.columns:
    missing_data_imputation=missing_data_imputation.withColumn(c,when(missing_data_imputation[c]=='-111999.0',None).otherwise(missing_data_imputation[c]))
#转换成spark double类型
ccol=['mean_z','min_z','lower_z','median_z','upper_z','max_z']
for c in ccol:
    missing_data_imputation=missing_data_imputation.withColumn(c,missing_data_imputation[c].cast(DoubleType()))
#转换成字符串类型
scol=['type1','type2','mode_z']
for c in scol:
    missing_data_imputation=missing_data_imputation.withColumn(c,when(missing_data_imputation[c]=='None',None).otherwise(missing_data_imputation[c]))
missing_data_imputation.write.format("jdbc").mode("overwrite").option("dbtable","missing_data_imputation").option("url",url_coal_mine_supervision).option("user",user_coal_mine_supervision).option("password",password_coal_mine_supervision).save()
print('missing_data_imputation_success')


#model_score_all
model_score_all=spark.createDataFrame(model_score_all)
model_score_all=model_score_all.replace('nan',None)
model_score_all.write.format("jdbc").mode("overwrite").option("dbtable","model_score_all").option("url",url_coal_mine_supervision).option("user",user_coal_mine_supervision).option("password",password_coal_mine_supervision).save()


#写入数据到评分系统数据库
# indictor_config.to_sql('indictor_config_all',engine4,if_exists='replace',index=False,
          # dtype={'ID':sqlalchemy.types.NVARCHAR(255),
                  # 'POINT_CODE':sqlalchemy.types.NVARCHAR(255),
                  # 'POINT_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'PARENT_ID':sqlalchemy.types.NVARCHAR(255)})

# base_data_all.to_sql('base_data_all',engine4,if_exists='replace',index=False,
          # dtype={'CORP_ID':sqlalchemy.types.NVARCHAR(255),
                  # 'CORP_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'MINE_MINETYPE_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'MINE_CLASS_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'PROVINCE_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'INDEX_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'INDEX_VALUE':sqlalchemy.types.NVARCHAR(255)})

# base_inf_all.to_sql('base_inf_all',engine4,if_exists='replace',index=False,
          # dtype={'point_id':sqlalchemy.types.NVARCHAR(255),
                  # 'POINT_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'mine_id':sqlalchemy.types.NVARCHAR(255),
                  # 'mine_name':sqlalchemy.types.NVARCHAR(255),
                  # 'MINE_MINETYPE_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'MINE_CLASS_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'PROVINCE_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'MODEL_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'point_scorp':sqlalchemy.types.Float})

# model_detail_all.to_sql('model_detail_all',engine4,if_exists='replace',index=False,
          # dtype={'ID':sqlalchemy.types.NVARCHAR(255),
                  # 'MODEL_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'ALGORITHM_TYPE':sqlalchemy.types.NVARCHAR(255),
                  # 'COMPUTED_RESULT':sqlalchemy.types.Float,
                  # 'YW_CONFORMITY':sqlalchemy.types.Float,
                  # 'ENABLE_TYPE':sqlalchemy.types.NVARCHAR(255),
                  # 'MODEL_REMARK':sqlalchemy.types.NVARCHAR(255),
                  # 'MODEL_PK':sqlalchemy.types.NVARCHAR(255),
                  # 'PROVINCE_NAME':sqlalchemy.types.NVARCHAR(255)})

# interval_score_lx_lb.to_sql('interval_score_lx_lb',engine4,if_exists='replace',index=False,
          # dtype={'PROVINCE_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'MINE_MINETYPE_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'MINE_CLASS_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'MODEL_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'POINT_CODE':sqlalchemy.types.NVARCHAR(255),
                  # 'POINT_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'LOWER':sqlalchemy.types.NVARCHAR(255),
                  # 'UPPER':sqlalchemy.types.NVARCHAR(255),
                  # 'SCORE':sqlalchemy.types.Float})

# missing_data_imputation.to_sql('missing_data_imputation',engine4,if_exists='replace',index=False,
          # dtype={'POINT_CODE':sqlalchemy.types.NVARCHAR(255),
                  # 'POINT_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'mean_z':sqlalchemy.types.Float,
                  # 'min_z':sqlalchemy.types.Float,
                  # 'lower_z':sqlalchemy.types.Float,
                  # 'median_z':sqlalchemy.types.Float,
                  # 'upper_z':sqlalchemy.types.Float,
                  # 'max_z':sqlalchemy.types.Float,
                  # 'type1':sqlalchemy.types.NVARCHAR(255),
                  # 'mode_z':sqlalchemy.types.NVARCHAR(255),
                  # 'type2':sqlalchemy.types.NVARCHAR(255),
                  # 'MINE_MINETYPE_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'MINE_CLASS_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'PROVINCE_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'MODEL_NAME':sqlalchemy.types.NVARCHAR(255)})

# model_score_all.to_sql('model_score_all',engine4,if_exists='replace',index=False,
          # dtype={'point_id':sqlalchemy.types.NVARCHAR(255),
                  # 'POINT_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'mine_id':sqlalchemy.types.NVARCHAR(255),
                  # 'mine_name':sqlalchemy.types.NVARCHAR(255),
                  # 'MINE_MINETYPE_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'MINE_CLASS_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'PROVINCE_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'MODEL_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'point_scorp':sqlalchemy.types.Float})


#############################################################写入UE展示数据库-spark方式写入#######################################################
#base_inf
base_inf=spark.createDataFrame(base_inf)
base_inf=base_inf.replace('nan',None)
base_inf.write.format("jdbc").mode("overwrite").option("dbtable","base_inf").option("url",urls).option("user",users).option("password",passwords).save()
#spark_df1=spark.read.format("jdbc").option("url",urls).option("dbtable","spark_df").option("user", users).option("password", passwords).option("driver",drivers).load().toPandas()

#model_score
model_score=spark.createDataFrame(model_score)
model_score=model_score.replace('nan',None)
model_score.write.format("jdbc").mode("overwrite").option("dbtable","model_score").option("url",urls).option("user",users).option("password",passwords).save()

#deduction
deduction['explain']=deduction['explain'].astype('str')
deduction=spark.createDataFrame(deduction)
deduction=deduction.replace('nan',None)
deduction.write.format("jdbc").mode("overwrite").option("dbtable","deduction").option("url",urls).option("user",users).option("password",passwords).save()

#yiji_explain
yiji_explain=spark.createDataFrame(yiji_explain)
yiji_explain=yiji_explain.replace('nan',None)
yiji_explain.write.format("jdbc").mode("overwrite").option("dbtable","yiji_explain").option("url",urls).option("user",users).option("password",passwords).save()

#model_raw_values
model_raw_values=spark.createDataFrame(model_raw_values)
model_raw_values=model_raw_values.replace('nan',None)
model_raw_values.write.format("jdbc").mode("overwrite").option("dbtable","model_raw_values").option("url",urls).option("user",users).option("password",passwords).save()

#base_inf_raw_values
base_inf_raw_values=spark.createDataFrame(base_inf_raw_values)
base_inf_raw_values=base_inf_raw_values.replace('nan',None)
base_inf_raw_values.write.format("jdbc").mode("overwrite").option("dbtable","base_inf_raw_values").option("url",urls).option("user",users).option("password",passwords).save()

#model_detail_all
model_detail_all.write.format("jdbc").mode("overwrite").option("dbtable","model_detail_all").option("url",urls).option("user",users).option("password",passwords).save()

########################################将UE展示数据写入oracle####################################################
#base_inf
base_inf.write.format("jdbc").mode("overwrite").option("dbtable","base_inf").option("url",url).option("user",user).option("password",password).save()
#model_score
ms_oracle=model_score.withColumn('create_time',substring(current_timestamp(),1,19))
#ms_oracle.write.format("jdbc").mode("overwrite").option("dbtable","model_score").option("url",url).option("user",user).option("password",password).save()
ms_oracle.write.format("jdbc").mode("append").option("dbtable","model_score").option("url",url).option("user",user).option("password",password).save()

#deduction
#deduction.write.format("jdbc").mode("overwrite").option("dbtable","deduction").option("url",url).option("user",user).option("password",password).save()
#yiji_explain
#yiji_explain.write.format("jdbc").mode("overwrite").option("dbtable","yiji_explain").option("url",url).option("user",user).option("password",password).save()
#model_raw_values
model_raw_values.write.format("jdbc").mode("overwrite").option("dbtable","model_raw_values").option("url",url).option("user",user).option("password",password).save()
#base_inf_raw_values
base_inf_raw_values.write.format("jdbc").mode("overwrite").option("dbtable","base_inf_raw_values").option("url",url).option("user",user).option("password",password).save()
#model_detail_all
model_detail_all.write.format("jdbc").mode("overwrite").option("dbtable","model_detail_all").option("url",url).option("user",user).option("password",password).save()


#写入UE展示数据库
# base_inf.to_sql('base_inf',engine2,if_exists='replace',index=False,
          # dtype={'point_id':sqlalchemy.types.NVARCHAR(255),
                  # 'POINT_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'mine_id':sqlalchemy.types.NVARCHAR(255),
                  # 'mine_name':sqlalchemy.types.NVARCHAR(255),
                  # 'point_scorp':sqlalchemy.types.INT})

# model_score.to_sql('model_score',engine2,if_exists='replace',index=False,
          # dtype={'point_id':sqlalchemy.types.NVARCHAR(255),
                  # 'POINT_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'mine_id':sqlalchemy.types.NVARCHAR(255),
                  # 'mine_name':sqlalchemy.types.NVARCHAR(255),
                  # 'point_scorp':sqlalchemy.types.Float})

# deduction.to_sql('deduction',engine2,if_exists='replace',index=False,
          # dtype={'item':sqlalchemy.types.NVARCHAR(255),
                  # 'mine_id':sqlalchemy.types.NVARCHAR(255),
                  # 'mine_name':sqlalchemy.types.NVARCHAR(255),
                  # 'explain':sqlalchemy.types.NVARCHAR(4000)})

# yiji_explain.to_sql('yiji_explain',engine2,if_exists='replace',index=False,
          # dtype={'mine_id':sqlalchemy.types.NVARCHAR(255),
                  # 'mine_name':sqlalchemy.types.NVARCHAR(255),
                  # 'item':sqlalchemy.types.NVARCHAR(255),
                  # 'explain':sqlalchemy.types.NVARCHAR(4000)})

# model_raw_values.to_sql('model_raw_values',engine2,if_exists='replace',index=False,
          # dtype={'mine_id':sqlalchemy.types.NVARCHAR(255),
                  # 'mine_name':sqlalchemy.types.NVARCHAR(255),
                  # 'point_id':sqlalchemy.types.NVARCHAR(255),
                  # 'POINT_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'point_scorp':sqlalchemy.types.NVARCHAR(255)})


# base_inf_raw_values.to_sql('base_inf_raw_values',engine2,if_exists='replace',index=False,
          # dtype={'mine_id':sqlalchemy.types.NVARCHAR(255),
                  # 'mine_name':sqlalchemy.types.NVARCHAR(255),
                  # 'point_id':sqlalchemy.types.NVARCHAR(255),
                  # 'POINT_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'raw_score':sqlalchemy.types.NVARCHAR(255),
                  # 'point_scorp':sqlalchemy.types.INT})

# model_detail_all.to_sql('model_detail_all',engine2,if_exists='replace',index=False,
          # dtype={'ID':sqlalchemy.types.NVARCHAR(255),
                  # 'MODEL_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'ALGORITHM_TYPE':sqlalchemy.types.NVARCHAR(255),
                  # 'COMPUTED_RESULT':sqlalchemy.types.Float,
                  # 'YW_CONFORMITY':sqlalchemy.types.Float,
                  # 'ENABLE_TYPE':sqlalchemy.types.NVARCHAR(255),
                  # 'MODEL_REMARK':sqlalchemy.types.NVARCHAR(255),
                  # 'MODEL_PK':sqlalchemy.types.NVARCHAR(255),
                  # 'PROVINCE_NAME':sqlalchemy.types.NVARCHAR(255)})



#############################################################写入东华数据库-spark方式写入#######################################################
#base_inf
base_inf.write.format("jdbc").mode("overwrite").option("dbtable","base_inf").option("url",url_znzf_model).option("user",user_znzf_model).option("password",password_znzf_model).save()

#model_score
model_score.write.format("jdbc").mode("overwrite").option("dbtable","model_score").option("url",url_znzf_model).option("user",user_znzf_model).option("password",password_znzf_model).save()

#deduction
deduction.write.format("jdbc").mode("overwrite").option("dbtable","deduction").option("url",url_znzf_model).option("user",user_znzf_model).option("password",password_znzf_model).save()

#yiji_explain
yiji_explain.write.format("jdbc").mode("overwrite").option("dbtable","yiji_explain").option("url",url_znzf_model).option("user",user_znzf_model).option("password",password_znzf_model).save()

#model_raw_values
model_raw_values.write.format("jdbc").mode("overwrite").option("dbtable","model_raw_values").option("url",url_znzf_model).option("user",user_znzf_model).option("password",password_znzf_model).save()

#base_inf_raw_values
base_inf_raw_values.write.format("jdbc").mode("overwrite").option("dbtable","base_inf_raw_values").option("url",url_znzf_model).option("user",user_znzf_model).option("password",password_znzf_model).save()

#model_detail_all
model_detail_all.write.format("jdbc").mode("overwrite").option("dbtable","model_detail_all").option("url",url_znzf_model).option("user",user_znzf_model).option("password",password_znzf_model).save()

# #测试效果
#df=pd.read_sql('select * from  missing_data_imputation',engine4)
#spark_df1=spark.read.format("jdbc").option("url",url_znzf_model).option("dbtable","base_inf").option("user", user_znzf_model).option("password", password_znzf_model).option("driver",drivers).load().toPandas()


#写入东华数据库
# base_inf.to_sql('base_inf',engine1,if_exists='replace',index=False,
          # dtype={'point_id':sqlalchemy.types.NVARCHAR(255),
                  # 'POINT_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'mine_id':sqlalchemy.types.NVARCHAR(255),
                  # 'mine_name':sqlalchemy.types.NVARCHAR(255),
                  # 'point_scorp':sqlalchemy.types.Float})

# model_score.to_sql('model_score',engine1,if_exists='replace',index=False,
          # dtype={'point_id':sqlalchemy.types.NVARCHAR(255),
                  # 'POINT_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'mine_id':sqlalchemy.types.NVARCHAR(255),
                  # 'mine_name':sqlalchemy.types.NVARCHAR(255),
                  # 'point_scorp':sqlalchemy.types.Float})

# deduction.to_sql('deduction',engine1,if_exists='replace',index=False,
          # dtype={'item':sqlalchemy.types.NVARCHAR(255),
                  # 'mine_id':sqlalchemy.types.NVARCHAR(255),
                  # 'mine_name':sqlalchemy.types.NVARCHAR(255),
                  # 'explain':sqlalchemy.types.NVARCHAR(4000)})

# yiji_explain.to_sql('yiji_explain',engine1,if_exists='replace',index=False,
          # dtype={'mine_id':sqlalchemy.types.NVARCHAR(255),
                  # 'mine_name':sqlalchemy.types.NVARCHAR(255),
                  # 'item':sqlalchemy.types.NVARCHAR(255),
                  # 'explain':sqlalchemy.types.NVARCHAR(4000)})

# model_raw_values.to_sql('model_raw_values',engine1,if_exists='replace',index=False,
          # dtype={'mine_id':sqlalchemy.types.NVARCHAR(255),
                  # 'mine_name':sqlalchemy.types.NVARCHAR(255),
                  # 'point_id':sqlalchemy.types.NVARCHAR(255),
                  # 'POINT_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'point_scorp':sqlalchemy.types.NVARCHAR(255)})


# base_inf_raw_values.to_sql('base_inf_raw_values',engine1,if_exists='replace',index=False,
          # dtype={'mine_id':sqlalchemy.types.NVARCHAR(255),
                  # 'mine_name':sqlalchemy.types.NVARCHAR(255),
                  # 'point_id':sqlalchemy.types.NVARCHAR(255),
                  # 'POINT_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'raw_score':sqlalchemy.types.NVARCHAR(255),
                  # 'point_scorp':sqlalchemy.types.INT})

# model_detail_all.to_sql('model_detail_all',engine1,if_exists='replace',index=False,
          # dtype={'ID':sqlalchemy.types.NVARCHAR(255),
                  # 'MODEL_NAME':sqlalchemy.types.NVARCHAR(255),
                  # 'ALGORITHM_TYPE':sqlalchemy.types.NVARCHAR(255),
                  # 'COMPUTED_RESULT':sqlalchemy.types.Float,
                  # 'YW_CONFORMITY':sqlalchemy.types.Float,
                  # 'ENABLE_TYPE':sqlalchemy.types.NVARCHAR(255),
                  # 'MODEL_REMARK':sqlalchemy.types.NVARCHAR(255),
                  # 'MODEL_PK':sqlalchemy.types.NVARCHAR(255),
                  # 'PROVINCE_NAME':sqlalchemy.types.NVARCHAR(255)})

#统计时长
end=timeit.default_timer()
all_time=round((end-start)/60,2)
print('Running minutes:',all_time)
