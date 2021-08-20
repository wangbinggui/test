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

#########################################################数据读取#########################################
#从业人员统计信息：REPORT_YEAR：报告年度；CYRYT_ZS：总数；CYRYT_ZZRS：中专及以上人数；CYRYT_GZ：高中；CYRYT_CZ：初中；CYRYT_CZYX：初中以下；DEL_FLAG：删除标记
T_AJ1_MJ_JCXX_CYRYTJ = pd.read_sql('select CORP_ID,REPORT_YEAR,CYRYT_ZS,CYRYT_ZZRS,CYRYT_GZ,CYRYT_CZ,CYRYT_CZYX,DEL_FLAG from mj.T_AJ1_MJ_JCXX_CYRYTJ',conn)

#专业技术人员统计信息：ZYJSR_SZRS：大专人数；ZYJSR_BKYS：本科及以上人数；A_ZYJSR_ZCGC：高级；A_ZYJSR_ZCZJ：中级；A_ZYJSR_ZCCJ：初级；A_ZYJSR_ZCJX：见习
T_AJ1_MJ_JCXX_ZYJSRYTJ = pd.read_sql('select CORP_ID,REPORT_YEAR,ZYJSR_SZRS,ZYJSR_BKYS,A_ZYJSR_ZCGC,A_ZYJSR_ZCZJ,A_ZYJSR_ZCCJ,A_ZYJSR_ZCJX,DEL_FLAG from mj.T_AJ1_MJ_JCXX_ZYJSRYTJ',conn)

#安全管理人员统计信息：AQGLR_FZRPX：负责人培训数；AQGLR_GLPX：安全生产管理人员培训数,AQGLR_ZYFZRS：负责人数,AQGLR_ZYFZR：负责人持证数（安全资格证）,AQGLR_TZZY：特种作业人员数,AQGLR_CZS：持证数（特种作业资格证）,AQGLR_AQSC：安全生产管理人员数,AQGLR_AQSCGL：安全生产管理人员持证数（管理人员安全资格证）,AQGLR_TZPX：特种作业人员培训数
T_AJ1_MJ_JCXX_AQGLRYTJ = pd.read_sql('select CORP_ID,REPORT_YEAR,AQGLR_FZRPX,AQGLR_GLPX,AQGLR_ZYFZRS,AQGLR_ZYFZR,AQGLR_TZZY,AQGLR_CZS,AQGLR_AQSC,AQGLR_AQSCGL,AQGLR_TZPX,DEL_FLAG from mj.T_AJ1_MJ_JCXX_AQGLRYTJ',conn)

#主要安全管理人员信息：A_PXKHQK：培训考核情况
T_AJ1_MJ_JCXX_AQGLRYPB = pd.read_sql('select CORP_ID,UPDATE_DATE,A_PXKHQK,DEL_FLAG from mj.T_AJ1_MJ_JCXX_AQGLRYPB',conn)

#安全费用管理信息：AQFYT_TQJE：,AQFYT_PJDM：,AQFYT_SYYE：,A_AQFYT_JYJE：
T_AJ1_MJ_JCXX_AQFYGL = pd.read_sql('select CORP_ID,REPORT_YEAR,AQFYT_TQJE,AQFYT_PJDM,AQFYT_SYYE,A_AQFYT_JYJE,DEL_FLAG from mj.T_AJ1_MJ_JCXX_AQFYGL',conn)

#瓦斯综合防治管理信息：GASGEN_TENICHNUM：“一通三防”管理人员数量；GASGEB_BURSTWORKERNUM：防突施工人员数量
T_AJ1_MJ_JCXX_WSZHFZGL = pd.read_sql('select CORP_ID,UPDATE_DATE,GASGEN_TENICHNUM,GASGEB_BURSTWORKERNUM,DEL_FLAG from mj.T_AJ1_MJ_JCXX_WSZHFZGL',conn)

#采煤工作面进度信息：CMGZM_JCL：机采率,CMGZM_PCGS：炮采个数,CMGZM_PC：普采个数,CMGZM_ZCGS：综采个数,CMGZM_HCL：回采率,CMGZM_ZDHGS：自动化开采个数
T_AJ1_MJ_JCXX_CMGZMJD = pd.read_sql('select CORP_ID,REPORT_YEAR,CMGZM_JCL,CMGZM_PCGS,CMGZM_PC,CMGZM_ZCGS,CMGZM_HCL,CMGZM_ZDHGS,DEL_FLAG from mj.T_AJ1_MJ_JCXX_CMGZMJD',conn)

#瓦斯抽放利用信息指标项：GASUTIL_CONMAX：最大瓦斯抽放浓度,GASUTIL_CONMIN：最小瓦斯抽放浓度,GASUTIL_CONAVG：平均瓦斯抽放浓度,GASUTIL_PREDESIGN：设计瓦斯抽放负压,GASUTIL_PREMAX：最大瓦斯抽放负压,GASUTIL_PREMIN：最小瓦斯抽放负压,GASUTIL_PREAVG：平均瓦斯抽放负压
T_AJ1_MJ_JCXX_WSCFLYGL = pd.read_sql('select CORP_ID,REPORT_YEAR,GASUTIL_CONMAX,GASUTIL_CONMIN,GASUTIL_CONAVG,GASUTIL_PREDESIGN,GASUTIL_PREMAX,GASUTIL_PREMIN,GASUTIL_PREAVG,DEL_FLAG from mj.T_AJ1_MJ_JCXX_WSCFLYGL',conn)

#T_AJ1_MJ_JCXX_WSCFLYGL.to_excel('./test/T_AJ1_MJ_JCXX_WSCFLYGL0626.xlsx',index=False,encoding='gbk')
#事故上报_事故基本信息表：HYDROGEOLOGICAL_TYPE：水文地质类型,ACCIDENT_LEVEL：事故等级,ACCIDENT_CATEGORY：事故类别,HURT_PEOPLE_COLL_ID：事故受伤人员统计记录编号,MINE_WELL_TYPE：矿井井型,MONITOR_STATUS：监控设备,HIDDEN_CATEGORY：瞒报
T_AJ1_MJ_SG_ACCIDENT_INFO = pd.read_sql('select ACCIDENT_MINE_ID,UPDATE_DATE,HYDROGEOLOGICAL_TYPE,ACCIDENT_LEVEL,ACCIDENT_CATEGORY,HURT_PEOPLE_COLL_ID,MINE_WELL_TYPE,MONITOR_STATUS,HIDDEN_CATEGORY,DEL_FLAG from mj.T_AJ1_MJ_SG_ACCIDENT_INFO',conn)

#煤层信息：MINE_THICK_AVG：煤层平均厚度,MINE_THICK_MAX：煤层最大厚度,A_SFMCJJ：上覆煤层间距,A_XFMCJJ：下伏煤层间距,A_MCPJQJ：煤层平均倾角
T_AJ1_MJ_JCXX_ZCMC = pd.read_sql('select CORP_ID,UPDATE_DATE,MINE_THICK_AVG,MINE_THICK_MAX,A_SFMCJJ,A_XFMCJJ,A_MCPJQJ,DEL_FLAG from mj.T_AJ1_MJ_JCXX_ZCMC',conn)

#事故上报_事故人员伤亡统计信息表：DIE_PEOPLE_NUM：事故死亡人数,WEIGHT_HURT_PEOPLE_NUM：事故重伤人数,LIGHT_HURT_PEOPLE_NUM：事故轻伤人数
SG_HURT_PEOPLE_COLL_INFO = pd.read_sql('select ID,DIE_PEOPLE_NUM,WEIGHT_HURT_PEOPLE_NUM,LIGHT_HURT_PEOPLE_NUM,DEL_FLAG from mj.SG_HURT_PEOPLE_COLL_INFO',conn)

#文书基本信息：NAME：名称,P8_PENALTY：罚款总额,PAPER_TYPE：文书类型,CASE_ID：执法活动ID,PAPER_ID：文书ID
jczf_paper = pd.read_sql('select CORP_ID,NAME,P8_PENALTY,PAPER_TYPE,CASE_ID,PAPER_ID,DEL_FLAG from mj.jczf_paper',conn)

#执法基本信息
jczf_case = pd.read_sql('select CASE_ID,DEL_FLAG from mj.jczf_case',conn)

#隐患基本信息：ONSITE_TYPE：现场处理决定类型,PENALTY_TYPE：行政处罚类型,IS_HIGH：是否(引发)重大隐患（1表示是重大隐患，0表示不是）,COALING_FACE：采煤工作面个数,HEADING_FACE：掘进工作面个数
jczf_danger = pd.read_sql('select PAPER_ID,ONSITE_TYPE,PENALTY_TYPE,IS_HIGH,COALING_FACE,HEADING_FACE,DEL_FLAG from mj.jczf_danger',conn)

#隐患排查信息指标项：YHPCZ_ZDYHS：重大隐患数
T_AJ1_MJ_JCXX_YHPCXX = pd.read_sql('select CORP_ID,YHPCZ_ZDYHS,DEL_FLAG from mj.T_AJ1_MJ_JCXX_YHPCXX',conn)

#NDCLX_CL：产量
T_AJ1_MJ_JCXX_CLXX = pd.read_sql('select CORP_ID,UPDATE_DATE,NDCLX_CL,DEL_FLAG from mj.T_AJ1_MJ_JCXX_CLXX',conn)

#井下安全避险系统信息：LDXTX_ZT：状态,LDXTX_XTLX：系统类型
T_AJ1_MJ_JCXX_JXAQBXXTXX = pd.read_sql('select CORP_ID,UPDATE_DATE,LDXTX_ZT,LDXTX_XTLX,DEL_FLAG from mj.T_AJ1_MJ_JCXX_JXAQBXXTXX',conn)

#读取视图基础表
V_AJ1_MJ_JCXX_BASEINFO=pd.read_sql_query('select * from mj.V_AJ1_MJ_JCXX_BASEINFO',conn)
V_AJ1_MJ_JCXX_BASEINFO.columns=list(map(lambda x: x.upper(),list(V_AJ1_MJ_JCXX_BASEINFO.columns)))
V_AJ1_MJ_JCXX_BASEINFO['CORP_ID']=V_AJ1_MJ_JCXX_BASEINFO['CORP_ID'].astype('str')

#################################读取mysql需要的表##############################
#类案屡查屡犯表
lclf=pd.read_sql_query('select CORP_ID,lclf from similar_data_behave_counties',engine2)
lclf=lclf.rename(columns={'corp_id':'CORP_ID'})
#中英文对应及指标编码表
indictor_config=pd.read_sql_query('select * from indictor_config',engine1)
#model_detail：模型明细表
model_detail=pd.read_sql_query('select * from model_detail',engine1)
#得分解释原始表
df_explain=pd.read_sql_query('select * from df_explain',engine1)
#等级调整信息表：YDJ：原等级,XDJ：新等级
t_bzh_jj = pd.read_sql('select CORP_ID,YDJ,XDJ,DEL_FLAG from t_bzh_jj',engine3)


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
#生成UE展示需要的原始数据
model_raw_values=compute_raw_ue(T_AJ1_MJ_JCXX_CYRYTJ,T_AJ1_MJ_JCXX_ZYJSRYTJ,T_AJ1_MJ_JCXX_AQGLRYTJ,T_AJ1_MJ_JCXX_AQGLRYPB,T_AJ1_MJ_JCXX_AQFYGL,T_AJ1_MJ_JCXX_WSZHFZGL,T_AJ1_MJ_JCXX_CMGZMJD,T_AJ1_MJ_JCXX_WSCFLYGL,T_AJ1_MJ_SG_ACCIDENT_INFO,T_AJ1_MJ_JCXX_ZCMC,SG_HURT_PEOPLE_COLL_INFO,jczf_paper,jczf_case,jczf_danger,T_AJ1_MJ_JCXX_YHPCXX,T_AJ1_MJ_JCXX_CLXX,T_AJ1_MJ_JCXX_JXAQBXXTXX,lclf,total_frame,indictor_config,V_AJ1_MJ_JCXX_BASEINFO)
model_raw_values.columns=['mine_id','mine_name','point_id','POINT_NAME','raw_score']
print('model_raw_values:',model_raw_values.shape)
#生成基础宽表
dfall=compute_dfall(T_AJ1_MJ_JCXX_CYRYTJ,T_AJ1_MJ_JCXX_ZYJSRYTJ,T_AJ1_MJ_JCXX_AQGLRYTJ,T_AJ1_MJ_JCXX_AQGLRYPB,T_AJ1_MJ_JCXX_AQFYGL,T_AJ1_MJ_JCXX_WSZHFZGL,T_AJ1_MJ_JCXX_CMGZMJD,T_AJ1_MJ_JCXX_WSCFLYGL,T_AJ1_MJ_SG_ACCIDENT_INFO,T_AJ1_MJ_JCXX_ZCMC,SG_HURT_PEOPLE_COLL_INFO,jczf_paper,jczf_case,jczf_danger,T_AJ1_MJ_JCXX_YHPCXX,T_AJ1_MJ_JCXX_CLXX,T_AJ1_MJ_JCXX_JXAQBXXTXX,lclf,total_frame,indictor_config,V_AJ1_MJ_JCXX_BASEINFO)
print('dfall:',dfall.shape)
dfall.to_excel('dfall.xlsx',index=False,encoding='gbk')
#生成转换后的原始数据base_data_all
base_data_all=generate_base_data_all(dfall)
print('base_data_all:',dfall.shape)

###########################################煤矿安全生产评估模型构建#####################
#生成三级指标得分和区间得分表
dfall_class = pd.merge(dfall, V_AJ1_MJ_JCXX_BASEINFO[['CORP_NAME', 'MINE_CLASSIFY_NAME']], on=['CORP_NAME'], how='left')
# 输出表interval_score_lx_lb
interval_score_lx_lb = generate_interval_score_lx_lb(dfall, indictor_config,dfall_class,'entropy')
#interval_score_lx_lb.loc[interval_score_lx_lb['LOWER']==-1,'LOWER']=0
#interval_score_lx_lb['LOWER']=interval_score_lx_lb['LOWER'].apply(lambda x:'0' if x=='-1' else x)
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

#写入数据到评分系统数据库
indictor_config.to_sql('indictor_config_all',engine4,if_exists='replace',index=False,
          dtype={'ID':sqlalchemy.types.NVARCHAR(255),
                  'POINT_CODE':sqlalchemy.types.NVARCHAR(255),
                  'POINT_NAME':sqlalchemy.types.NVARCHAR(255),
                  'PARENT_ID':sqlalchemy.types.NVARCHAR(255)})

base_data_all.to_sql('base_data_all',engine4,if_exists='replace',index=False,
          dtype={'CORP_ID':sqlalchemy.types.NVARCHAR(255),
                  'CORP_NAME':sqlalchemy.types.NVARCHAR(255),
                  'MINE_MINETYPE_NAME':sqlalchemy.types.NVARCHAR(255),
                  'MINE_CLASS_NAME':sqlalchemy.types.NVARCHAR(255),
                  'PROVINCE_NAME':sqlalchemy.types.NVARCHAR(255),
                  'INDEX_NAME':sqlalchemy.types.NVARCHAR(255),
                  'INDEX_VALUE':sqlalchemy.types.NVARCHAR(255)})

base_inf_all.to_sql('base_inf_all',engine4,if_exists='replace',index=False,
          dtype={'point_id':sqlalchemy.types.NVARCHAR(255),
                  'POINT_NAME':sqlalchemy.types.NVARCHAR(255),
                  'mine_id':sqlalchemy.types.NVARCHAR(255),
                  'mine_name':sqlalchemy.types.NVARCHAR(255),
                  'MINE_MINETYPE_NAME':sqlalchemy.types.NVARCHAR(255),
                  'MINE_CLASS_NAME':sqlalchemy.types.NVARCHAR(255),
                  'PROVINCE_NAME':sqlalchemy.types.NVARCHAR(255),
                  'MODEL_NAME':sqlalchemy.types.NVARCHAR(255),
                  'point_scorp':sqlalchemy.types.Float})

model_detail_all.to_sql('model_detail_all',engine4,if_exists='replace',index=False,
          dtype={'ID':sqlalchemy.types.NVARCHAR(255),
                  'MODEL_NAME':sqlalchemy.types.NVARCHAR(255),
                  'ALGORITHM_TYPE':sqlalchemy.types.NVARCHAR(255),
                  'COMPUTED_RESULT':sqlalchemy.types.Float,
                  'YW_CONFORMITY':sqlalchemy.types.Float,
                  'ENABLE_TYPE':sqlalchemy.types.NVARCHAR(255),
                  'MODEL_REMARK':sqlalchemy.types.NVARCHAR(255),
                  'MODEL_PK':sqlalchemy.types.NVARCHAR(255),
                  'PROVINCE_NAME':sqlalchemy.types.NVARCHAR(255)})

interval_score_lx_lb.to_sql('interval_score_lx_lb',engine4,if_exists='replace',index=False,
          dtype={'PROVINCE_NAME':sqlalchemy.types.NVARCHAR(255),
                  'MINE_MINETYPE_NAME':sqlalchemy.types.NVARCHAR(255),
                  'MINE_CLASS_NAME':sqlalchemy.types.NVARCHAR(255),
                  'MODEL_NAME':sqlalchemy.types.NVARCHAR(255),
                  'POINT_CODE':sqlalchemy.types.NVARCHAR(255),
                  'POINT_NAME':sqlalchemy.types.NVARCHAR(255),
                  'LOWER':sqlalchemy.types.NVARCHAR(255),
                  'UPPER':sqlalchemy.types.NVARCHAR(255),
                  'SCORE':sqlalchemy.types.Float})

missing_data_imputation.to_sql('missing_data_imputation',engine4,if_exists='replace',index=False,
          dtype={'POINT_CODE':sqlalchemy.types.NVARCHAR(255),
                  'POINT_NAME':sqlalchemy.types.NVARCHAR(255),
                  'mean_z':sqlalchemy.types.Float,
                  'min_z':sqlalchemy.types.Float,
                  'lower_z':sqlalchemy.types.Float,
                  'median_z':sqlalchemy.types.Float,
                  'upper_z':sqlalchemy.types.Float,
                  'max_z':sqlalchemy.types.Float,
                  'type1':sqlalchemy.types.NVARCHAR(255),
                  'mode_z':sqlalchemy.types.NVARCHAR(255),
                  'type2':sqlalchemy.types.NVARCHAR(255),
                  'MINE_MINETYPE_NAME':sqlalchemy.types.NVARCHAR(255),
                  'MINE_CLASS_NAME':sqlalchemy.types.NVARCHAR(255),
                  'PROVINCE_NAME':sqlalchemy.types.NVARCHAR(255),
                  'MODEL_NAME':sqlalchemy.types.NVARCHAR(255)})

model_score_all.to_sql('model_score_all',engine4,if_exists='replace',index=False,
          dtype={'point_id':sqlalchemy.types.NVARCHAR(255),
                  'POINT_NAME':sqlalchemy.types.NVARCHAR(255),
                  'mine_id':sqlalchemy.types.NVARCHAR(255),
                  'mine_name':sqlalchemy.types.NVARCHAR(255),
                  'MINE_MINETYPE_NAME':sqlalchemy.types.NVARCHAR(255),
                  'MINE_CLASS_NAME':sqlalchemy.types.NVARCHAR(255),
                  'PROVINCE_NAME':sqlalchemy.types.NVARCHAR(255),
                  'MODEL_NAME':sqlalchemy.types.NVARCHAR(255),
                  'point_scorp':sqlalchemy.types.Float})



#写入UE展示数据库
base_inf.to_sql('base_inf',engine2,if_exists='replace',index=False,
          dtype={'point_id':sqlalchemy.types.NVARCHAR(255),
                  'POINT_NAME':sqlalchemy.types.NVARCHAR(255),
                  'mine_id':sqlalchemy.types.NVARCHAR(255),
                  'mine_name':sqlalchemy.types.NVARCHAR(255),
                  'point_scorp':sqlalchemy.types.INT})

model_score.to_sql('model_score',engine2,if_exists='replace',index=False,
          dtype={'point_id':sqlalchemy.types.NVARCHAR(255),
                  'POINT_NAME':sqlalchemy.types.NVARCHAR(255),
                  'mine_id':sqlalchemy.types.NVARCHAR(255),
                  'mine_name':sqlalchemy.types.NVARCHAR(255),
                  'point_scorp':sqlalchemy.types.Float})

deduction.to_sql('deduction',engine2,if_exists='replace',index=False,
          dtype={'item':sqlalchemy.types.NVARCHAR(255),
                  'mine_id':sqlalchemy.types.NVARCHAR(255),
                  'mine_name':sqlalchemy.types.NVARCHAR(255),
                  'explain':sqlalchemy.types.NVARCHAR(4000)})

yiji_explain.to_sql('yiji_explain',engine2,if_exists='replace',index=False,
          dtype={'mine_id':sqlalchemy.types.NVARCHAR(255),
                  'mine_name':sqlalchemy.types.NVARCHAR(255),
                  'item':sqlalchemy.types.NVARCHAR(255),
                  'explain':sqlalchemy.types.NVARCHAR(4000)})

model_raw_values.to_sql('model_raw_values',engine2,if_exists='replace',index=False,
          dtype={'mine_id':sqlalchemy.types.NVARCHAR(255),
                  'mine_name':sqlalchemy.types.NVARCHAR(255),
                  'point_id':sqlalchemy.types.NVARCHAR(255),
                  'POINT_NAME':sqlalchemy.types.NVARCHAR(255),
                  'point_scorp':sqlalchemy.types.NVARCHAR(255)})


base_inf_raw_values.to_sql('base_inf_raw_values',engine2,if_exists='replace',index=False,
          dtype={'mine_id':sqlalchemy.types.NVARCHAR(255),
                  'mine_name':sqlalchemy.types.NVARCHAR(255),
                  'point_id':sqlalchemy.types.NVARCHAR(255),
                  'POINT_NAME':sqlalchemy.types.NVARCHAR(255),
                  'raw_score':sqlalchemy.types.NVARCHAR(255),
                  'point_scorp':sqlalchemy.types.INT})

model_detail_all.to_sql('model_detail_all',engine2,if_exists='replace',index=False,
          dtype={'ID':sqlalchemy.types.NVARCHAR(255),
                  'MODEL_NAME':sqlalchemy.types.NVARCHAR(255),
                  'ALGORITHM_TYPE':sqlalchemy.types.NVARCHAR(255),
                  'COMPUTED_RESULT':sqlalchemy.types.Float,
                  'YW_CONFORMITY':sqlalchemy.types.Float,
                  'ENABLE_TYPE':sqlalchemy.types.NVARCHAR(255),
                  'MODEL_REMARK':sqlalchemy.types.NVARCHAR(255),
                  'MODEL_PK':sqlalchemy.types.NVARCHAR(255),
                  'PROVINCE_NAME':sqlalchemy.types.NVARCHAR(255)})



#写入东华数据库
base_inf.to_sql('base_inf',engine1,if_exists='replace',index=False,
          dtype={'point_id':sqlalchemy.types.NVARCHAR(255),
                  'POINT_NAME':sqlalchemy.types.NVARCHAR(255),
                  'mine_id':sqlalchemy.types.NVARCHAR(255),
                  'mine_name':sqlalchemy.types.NVARCHAR(255),
                  'point_scorp':sqlalchemy.types.Float})

model_score.to_sql('model_score',engine1,if_exists='replace',index=False,
          dtype={'point_id':sqlalchemy.types.NVARCHAR(255),
                  'POINT_NAME':sqlalchemy.types.NVARCHAR(255),
                  'mine_id':sqlalchemy.types.NVARCHAR(255),
                  'mine_name':sqlalchemy.types.NVARCHAR(255),
                  'point_scorp':sqlalchemy.types.Float})

deduction.to_sql('deduction',engine1,if_exists='replace',index=False,
          dtype={'item':sqlalchemy.types.NVARCHAR(255),
                  'mine_id':sqlalchemy.types.NVARCHAR(255),
                  'mine_name':sqlalchemy.types.NVARCHAR(255),
                  'explain':sqlalchemy.types.NVARCHAR(4000)})

yiji_explain.to_sql('yiji_explain',engine1,if_exists='replace',index=False,
          dtype={'mine_id':sqlalchemy.types.NVARCHAR(255),
                  'mine_name':sqlalchemy.types.NVARCHAR(255),
                  'item':sqlalchemy.types.NVARCHAR(255),
                  'explain':sqlalchemy.types.NVARCHAR(4000)})

model_raw_values.to_sql('model_raw_values',engine1,if_exists='replace',index=False,
          dtype={'mine_id':sqlalchemy.types.NVARCHAR(255),
                  'mine_name':sqlalchemy.types.NVARCHAR(255),
                  'point_id':sqlalchemy.types.NVARCHAR(255),
                  'POINT_NAME':sqlalchemy.types.NVARCHAR(255),
                  'point_scorp':sqlalchemy.types.NVARCHAR(255)})


base_inf_raw_values.to_sql('base_inf_raw_values',engine1,if_exists='replace',index=False,
          dtype={'mine_id':sqlalchemy.types.NVARCHAR(255),
                  'mine_name':sqlalchemy.types.NVARCHAR(255),
                  'point_id':sqlalchemy.types.NVARCHAR(255),
                  'POINT_NAME':sqlalchemy.types.NVARCHAR(255),
                  'raw_score':sqlalchemy.types.NVARCHAR(255),
                  'point_scorp':sqlalchemy.types.INT})

model_detail_all.to_sql('model_detail_all',engine1,if_exists='replace',index=False,
          dtype={'ID':sqlalchemy.types.NVARCHAR(255),
                  'MODEL_NAME':sqlalchemy.types.NVARCHAR(255),
                  'ALGORITHM_TYPE':sqlalchemy.types.NVARCHAR(255),
                  'COMPUTED_RESULT':sqlalchemy.types.Float,
                  'YW_CONFORMITY':sqlalchemy.types.Float,
                  'ENABLE_TYPE':sqlalchemy.types.NVARCHAR(255),
                  'MODEL_REMARK':sqlalchemy.types.NVARCHAR(255),
                  'MODEL_PK':sqlalchemy.types.NVARCHAR(255),
                  'PROVINCE_NAME':sqlalchemy.types.NVARCHAR(255)})

#统计时长
end=timeit.default_timer()
all_time=round((end-start)/60,2)
print('Running minutes:',all_time)
