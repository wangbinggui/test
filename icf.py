# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import pymysql
import sqlalchemy
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
import cx_Oracle
import os
import timeit
import warnings
import datetime
#生成UE展示数据
def compute_raw_ue(T_AJ1_MJ_JCXX_CYRYTJ,T_AJ1_MJ_JCXX_ZYJSRYTJ,T_AJ1_MJ_JCXX_AQGLRYTJ,T_AJ1_MJ_JCXX_AQGLRYPB,T_AJ1_MJ_JCXX_AQFYGL,T_AJ1_MJ_JCXX_WSZHFZGL,T_AJ1_MJ_JCXX_CMGZMJD,T_AJ1_MJ_JCXX_WSCFLYGL,T_AJ1_MJ_SG_ACCIDENT_INFO,T_AJ1_MJ_JCXX_ZCMC,SG_HURT_PEOPLE_COLL_INFO,jczf_paper,jczf_case,jczf_danger,T_AJ1_MJ_JCXX_YHPCXX,T_AJ1_MJ_JCXX_CLXX,T_AJ1_MJ_JCXX_JXAQBXXTXX,lclf,total_frame,indictor_config,V_AJ1_MJ_JCXX_BASEINFO):
    ##########################################################################################################
    ###########################################################################################################
    #############################################人员状况######################################################
    ##  T_AJ1_MJ_JCXX_CYRYTJ 总数 、中专以上人数、高中人数、初中人数、初中以下人数
    ##  T_AJ1_MJ_JCXX_ZYJSRYTJ  大专人数、本科及以上人数、高级技术人数、中级技术人数、初级技术人数、见习技术人数
    ## 以ID 和字段 report_year这两个字段作为分组主键，report_year是为了选取最近的时间
    df1A = T_AJ1_MJ_JCXX_CYRYTJ[['CORP_ID','REPORT_YEAR']].groupby(['CORP_ID']).max().reset_index(drop=False)
    df1A['REPORT_YEAR'] = df1A['REPORT_YEAR'].astype(float)
    df1B = T_AJ1_MJ_JCXX_CYRYTJ[['CORP_ID','REPORT_YEAR','CYRYT_ZS','CYRYT_ZZRS','CYRYT_GZ','CYRYT_CZ','CYRYT_CZYX']]
    df1B['REPORT_YEAR'] = df1B['REPORT_YEAR'].astype(float)
    df1 = pd.merge(df1A,df1B,on=['CORP_ID','REPORT_YEAR'],how='inner')
    df1 = df1.drop(['REPORT_YEAR'],axis=1)
    df1 = df1.drop_duplicates(['CORP_ID'])
    #T_AJ1_MJ_JCXX_CYRYTJ['REPORT_YEAR']=T_AJ1_MJ_JCXX_CYRYTJ['REPORT_YEAR'].fillna(0).astype('int').astype('str')
    #df1=T_AJ1_MJ_JCXX_CYRYTJ[T_AJ1_MJ_JCXX_CYRYTJ['REPORT_YEAR']==str(datetime.datetime.now().year-1)].drop(columns=['REPORT_YEAR','DEL_FLAG']).drop_duplicates(['CORP_ID'])

    ##  T_AJ1_MJ_JCXX_ZYJSRYTJ  大专人数、本科及以上人数、高级技术人数、中级技术人数、初级技术人数、见习技术人数
    df2A = T_AJ1_MJ_JCXX_ZYJSRYTJ[['CORP_ID','REPORT_YEAR']].groupby(['CORP_ID']).max().reset_index(drop=False)
    df2A['REPORT_YEAR'] = df2A['REPORT_YEAR'].astype(float)
    df2B = T_AJ1_MJ_JCXX_ZYJSRYTJ[['CORP_ID','REPORT_YEAR','ZYJSR_SZRS','ZYJSR_BKYS','A_ZYJSR_ZCGC','A_ZYJSR_ZCZJ','A_ZYJSR_ZCCJ','A_ZYJSR_ZCJX']]
    df2B['REPORT_YEAR'] = df2B['REPORT_YEAR'].astype(float)
    df2 = pd.merge(df2A,df2B,on=['CORP_ID','REPORT_YEAR'],how='inner')
    df2 = df2.drop(['REPORT_YEAR'],axis=1)
    df2 = df2.drop_duplicates(['CORP_ID'])
    #T_AJ1_MJ_JCXX_ZYJSRYTJ['REPORT_YEAR']=T_AJ1_MJ_JCXX_ZYJSRYTJ['REPORT_YEAR'].fillna(0).astype('int').astype('str')
    #df2=T_AJ1_MJ_JCXX_ZYJSRYTJ[T_AJ1_MJ_JCXX_ZYJSRYTJ['REPORT_YEAR']==str(datetime.datetime.now().year-1)].drop(columns=['REPORT_YEAR','DEL_FLAG']).drop_duplicates(['CORP_ID'])

    ## T_AJ1_MJ_JCXX_AQGLRYTJ 负责人数、负责人持证数（安全资格证）、特种作业人数、特种作业资格证人数、安全生产管理人员数、安全生产管理人员持证数（管理人员安全资格证）
    # df3A = T_AJ1_MJ_JCXX_AQGLRYTJ[['CORP_ID','REPORT_YEAR']].groupby(['CORP_ID']).max().reset_index(drop=False)
    # df3B = T_AJ1_MJ_JCXX_AQGLRYTJ[['CORP_ID','REPORT_YEAR','AQGLR_ZYFZRS','AQGLR_ZYFZR','AQGLR_TZZY','AQGLR_CZS','AQGLR_AQSC','AQGLR_AQSCGL']]
    # df3 = pd.merge(df3A,df3B,on=['CORP_ID','REPORT_YEAR'],how='inner')
    # df3 = df3.drop(['REPORT_YEAR'],axis=1)
    # df3 = df3.drop_duplicates(['CORP_ID'])
    df3=T_AJ1_MJ_JCXX_AQGLRYTJ[T_AJ1_MJ_JCXX_AQGLRYTJ['REPORT_YEAR']==str(datetime.datetime.now().year-1)].drop_duplicates(['CORP_ID'])
    df3=df3[['CORP_ID','AQGLR_ZYFZRS','AQGLR_ZYFZR','AQGLR_TZZY','AQGLR_CZS','AQGLR_AQSC','AQGLR_AQSCGL']]

    ## T_AJ1_MJ_JCXX_AQGLRYTJ 负责人培训数、安全生产管理人员培训数、特种作业人员培训数、
    # df4A = T_AJ1_MJ_JCXX_AQGLRYTJ[['CORP_ID','REPORT_YEAR']].groupby(['CORP_ID']).max().reset_index(drop=False)
    # df4B = T_AJ1_MJ_JCXX_AQGLRYTJ[['CORP_ID','REPORT_YEAR','AQGLR_FZRPX','AQGLR_GLPX','AQGLR_TZPX']]
    # df4 = pd.merge(df4A,df4B,on=['CORP_ID','REPORT_YEAR'],how='inner')
    # df4 = df4.drop(['REPORT_YEAR'],axis=1)
    # df4 = df4.drop_duplicates(['CORP_ID'])
    df4=T_AJ1_MJ_JCXX_AQGLRYTJ[T_AJ1_MJ_JCXX_AQGLRYTJ['REPORT_YEAR']==str(datetime.datetime.now().year-1)].drop_duplicates(['CORP_ID'])
    df4=df4[['CORP_ID','AQGLR_FZRPX','AQGLR_GLPX','AQGLR_TZPX']]

    #T_AJ1_MJ_JCXX_AQGLRYPB 添加培训考核情况 A_PXKHQK
    df5A = T_AJ1_MJ_JCXX_AQGLRYPB[['CORP_ID','UPDATE_DATE']].groupby(['CORP_ID']).max().reset_index(drop=False)
    df5B = T_AJ1_MJ_JCXX_AQGLRYPB[['CORP_ID','UPDATE_DATE','A_PXKHQK']]
    df5 = pd.merge(df5A,df5B,on=['CORP_ID','UPDATE_DATE'],how='inner')
    df5 = df5.drop(['UPDATE_DATE'],axis=1)
    df5 = df5.drop_duplicates(['CORP_ID'])

    #生成2020人员总数
    zs=T_AJ1_MJ_JCXX_CYRYTJ[T_AJ1_MJ_JCXX_CYRYTJ['REPORT_YEAR'].fillna(0).astype('int').astype('str')==str(datetime.datetime.now().year-1)].drop_duplicates(['CORP_ID'])
    zs=zs[['CORP_ID','CYRYT_ZS']]
    zs.columns=['CORP_ID','CYRYT_ZS2020']

    #合并人员状况宽表基础字段
    people_1=pd.merge(total_frame, df1,on=['CORP_ID'],how='left' )
    people_2=pd.merge(people_1, df2,on=['CORP_ID'],how='left' )
    people_3=pd.merge(people_2, df3,on=['CORP_ID'],how='left' )
    people_4=pd.merge(people_3, df4,on=['CORP_ID'],how='left' )
    Personnel_situation=pd.merge(people_4,df5,on=['CORP_ID'],how='left' )
    Personnel_situation=pd.merge(Personnel_situation,zs,on=['CORP_ID'],how='left' )

    #计算占比
    #先填充离散变量缺失值，由于缺失值没参加培训，默认合格
    Personnel_situation['A_PXKHQK']=Personnel_situation['A_PXKHQK'].fillna('未填报')
    Personnel_situation['A_PXKHQK']=Personnel_situation['A_PXKHQK'].apply(lambda x:'合格' if x!='未填报' else x)
    #本科及以上人数占比
    Personnel_situation['BZ_BKJYS_CM_rate']=Personnel_situation['ZYJSR_BKYS']/Personnel_situation['CYRYT_ZS']

    #大专人数占比
    Personnel_situation['BZ_DZ_CM_rate']=Personnel_situation['ZYJSR_SZRS']/Personnel_situation['CYRYT_ZS']

    #初中人数总数占比
    Personnel_situation['CYRYT_CZ_rate']=Personnel_situation['CYRYT_CZ']/Personnel_situation['CYRYT_ZS']

    #初中以下人数占比数占比
    Personnel_situation['CYRYT_CZYX_rate']=Personnel_situation['CYRYT_CZYX']/Personnel_situation['CYRYT_ZS']

    #高级技术人数占比
    Personnel_situation['A_ZYJSR_ZCGC_rate']=Personnel_situation['A_ZYJSR_ZCGC']/Personnel_situation['CYRYT_ZS']

    #中级技术人数占比
    Personnel_situation['A_ZYJSR_ZCZJ_rate']=Personnel_situation['A_ZYJSR_ZCZJ']/Personnel_situation['CYRYT_ZS']

    #初级技术人数占比
    Personnel_situation['A_ZYJSR_ZCCJ_rate']=Personnel_situation['A_ZYJSR_ZCCJ']/Personnel_situation['CYRYT_ZS']

    #见习技术人数占比
    Personnel_situation['A_ZYJSR_ZCJX_rate']=Personnel_situation['A_ZYJSR_ZCJX']/Personnel_situation['CYRYT_ZS']

    #企业负责人持证数（安全资格证）占比
    Personnel_situation['AQGLR_ZYFZR_rate']=Personnel_situation['AQGLR_ZYFZR']/Personnel_situation['CYRYT_ZS2020']

    #安全生产管理人员数占比
    Personnel_situation['AQGLR_AQSC_rate']=Personnel_situation['AQGLR_AQSC']/Personnel_situation['CYRYT_ZS2020']

    #安全生产管理人员持证数（管理人员安全资格证）占比
    Personnel_situation['AQGLR_AQSCGL_rate']=Personnel_situation['AQGLR_AQSCGL']/Personnel_situation['CYRYT_ZS2020']

    #特种作业资格证人数占比
    Personnel_situation['AQGLR_CZS_rate']=Personnel_situation['AQGLR_CZS']/Personnel_situation['CYRYT_ZS2020']

    #特种作业人员培训数占比
    Personnel_situation['AQGLR_TZPX_rate']=Personnel_situation['AQGLR_TZPX']/Personnel_situation['CYRYT_ZS2020']

    #字段整理
    #人员基本情况
    Basic_Information_of_personnel=['BZ_BKJYS_CM_rate','BZ_DZ_CM_rate','CYRYT_CZ_rate','CYRYT_CZYX_rate','A_ZYJSR_ZCGC_rate','A_ZYJSR_ZCZJ_rate','A_ZYJSR_ZCCJ_rate','A_ZYJSR_ZCJX_rate']
    #资格证
    certification=['AQGLR_ZYFZR_rate','AQGLR_AQSC_rate','AQGLR_AQSCGL_rate','AQGLR_CZS_rate']
    #参加培训/培训考核
    attend_training=['AQGLR_TZPX_rate']
    training_check=['A_PXKHQK']
    #最终字段
    Personnel_allcol=list(total_frame.columns)+Basic_Information_of_personnel+certification+attend_training+training_check
    #根据字段名和排序生成数据
    Personnel_situation_zj=Personnel_situation[['CORP_ID','CYRYT_ZS']]
    Personnel_situation=Personnel_situation[Personnel_allcol]
    Personnel_situation.replace([np.inf, -np.inf], np.nan, inplace=True)
    ##################异常值处理##################
    #异常值处理，过滤掉部分大于1的字段
    lxcol=['BZ_BKJYS_CM_rate','BZ_DZ_CM_rate','CYRYT_CZ_rate','CYRYT_CZYX_rate','A_ZYJSR_ZCGC_rate','A_ZYJSR_ZCZJ_rate','A_ZYJSR_ZCCJ_rate','A_ZYJSR_ZCJX_rate','AQGLR_ZYFZR_rate','AQGLR_AQSC_rate','AQGLR_AQSCGL_rate','AQGLR_CZS_rate','AQGLR_TZPX_rate']
    for ft in lxcol:
        Personnel_situation[ft]=Personnel_situation[ft].apply(lambda x:0.5 if x>1 else x)*100
        Personnel_situation[ft]=Personnel_situation[ft].fillna(10000).round(2)
        Personnel_situation[ft]=Personnel_situation[ft].astype('str')
        Personnel_situation[ft]=Personnel_situation[ft]+'%'
    #缺失值处理
    Personnel_situation=Personnel_situation.applymap(lambda x:'未填报' if x=='10000.0%' else x)
    #############################################安全管理规制及执行############################################
    #管理制度
    #对没接入的默认赋值
    df6=total_frame[['CORP_ID']]
    df6['Reasonable_organization_structure']='暂未接入数据'
    df6['Sound_organizational_structure']='暂未接入数据'
    df6['Sound_safety_management']='暂未接入数据'
    df6['Sound_Discipline_establishment']='暂未接入数据'
    df6['Plan_to_perfect']='暂未接入数据'
    df6['Plan_reasonable']='暂未接入数据'
    #安全经费投入
    ## T_AJ1_MJ_JCXX_AQFYGL 提取金额、吨煤提取金额、使用金额、结余金额
    # df7A = T_AJ1_MJ_JCXX_AQFYGL[['CORP_ID','REPORT_YEAR']].groupby(['CORP_ID']).max().reset_index(drop=False)
    # df7B = T_AJ1_MJ_JCXX_AQFYGL[['CORP_ID','REPORT_YEAR','AQFYT_TQJE','AQFYT_PJDM','AQFYT_SYYE','A_AQFYT_JYJE']]
    # df7 = pd.merge(df7A,df7B,on=['CORP_ID','REPORT_YEAR'],how='inner')
    # df7 = df7.drop(['REPORT_YEAR'],axis=1)
    # df7 = df7.drop_duplicates(['CORP_ID'])
    df7=T_AJ1_MJ_JCXX_AQFYGL[T_AJ1_MJ_JCXX_AQFYGL['REPORT_YEAR']==str(datetime.datetime.now().year-1)].drop_duplicates(['CORP_ID'])
    df7=df7[['CORP_ID','AQFYT_TQJE','AQFYT_PJDM','AQFYT_SYYE','A_AQFYT_JYJE']]

    #安全制度执行
    ## T_AJ1_MJ_JCXX_WSZHFZGL “一通三防”管理人员数量、防突施工人员数量
    ## 这个表中，没有report_year 这个字段，所以选择update_date 的最新时间来分组
    df8A = T_AJ1_MJ_JCXX_WSZHFZGL[['CORP_ID','UPDATE_DATE']].groupby(['CORP_ID']).max().reset_index(drop=False)
    df8B = T_AJ1_MJ_JCXX_WSZHFZGL[['CORP_ID','UPDATE_DATE','GASGEN_TENICHNUM','GASGEB_BURSTWORKERNUM']]
    df8_1 = pd.merge(df8A,df8B,on=['CORP_ID','UPDATE_DATE'],how='inner')
    df8_1 = df8_1.drop(['UPDATE_DATE'],axis=1)
    df8_1 = df8_1.drop_duplicates(['CORP_ID'])

    # df8_1=T_AJ1_MJ_JCXX_WSZHFZGL[T_AJ1_MJ_JCXX_WSZHFZGL['UPDATE_DATE'].astype('str').str[:4]==str(datetime.datetime.now().year-1)].drop_duplicates(['CORP_ID'])
    # df8_1=df8_1[['CORP_ID','GASGEN_TENICHNUM','GASGEB_BURSTWORKERNUM']]

    #关联人数计算占比
    #以2020人员为总数
    # zs=T_AJ1_MJ_JCXX_CYRYTJ[T_AJ1_MJ_JCXX_CYRYTJ['REPORT_YEAR'].fillna(0).astype('int').astype('str')==str(datetime.datetime.now().year-1)].drop_duplicates(['CORP_ID'])
    # zs=zs[['CORP_ID','CYRYT_ZS']]

    df8=pd.merge(Personnel_situation_zj,df8_1,on=['CORP_ID'],how='left')
    #“一通三防”管理人员数量占比
    df8['GASGEN_TENICHNUM_rate']=df8['GASGEN_TENICHNUM']/df8['CYRYT_ZS']
    #防突施工人员数量占比
    df8['GASGEB_BURSTWORKERNUM_rate']=df8['GASGEB_BURSTWORKERNUM']/df8['CYRYT_ZS']
    df8=df8[['CORP_ID','GASGEN_TENICHNUM_rate','GASGEB_BURSTWORKERNUM_rate']]
    df8.replace([np.inf, -np.inf], np.nan, inplace=True)

    #矿井文化建设
    #默认赋值
    df9=total_frame[['CORP_ID']]
    df9['Number_of_propaganda']='暂未接入数据'
    #合并安全管理规制及执行字段
    regulation_implementation_6=pd.merge(total_frame,df6,on=['CORP_ID'],how='left')
    regulation_implementation_7=pd.merge(regulation_implementation_6,df7,on=['CORP_ID'],how='left')
    regulation_implementation_8=pd.merge(regulation_implementation_7,df8,on=['CORP_ID'],how='left')
    regulation_implementation=pd.merge(regulation_implementation_8,df9,on=['CORP_ID'],how='left')

    #字段重命名
    regulation_implementation=regulation_implementation.rename(columns={'AQFYT_TQJE':'FXDYJ_CCJE','AQFYT_PJDM':'A_FXDYJ_DMTQJE','AQFYT_SYYE':'FXDYJ_SYJE','A_AQFYT_JYJE':'A_FXDYJ_JYJE'})
    #列名校验
    regulation_bcol=['Reasonable_organization_structure',
    'Sound_organizational_structure',
    'Sound_safety_management',
    'Sound_Discipline_establishment',
    'Plan_to_perfect',
    'Plan_reasonable',
    'FXDYJ_CCJE',
    'A_FXDYJ_DMTQJE',
    'FXDYJ_SYJE',
    'A_FXDYJ_JYJE',
    'GASGEN_TENICHNUM_rate',
    'GASGEB_BURSTWORKERNUM_rate',
    'Number_of_propaganda']
    #所有列
    regulation_allcol=list(total_frame.columns)+regulation_bcol
    regulation_implementation=regulation_implementation[regulation_allcol]
    ######异常值处理
    regulation_implementation['GASGEN_TENICHNUM_rate']=regulation_implementation['GASGEN_TENICHNUM_rate'].apply(lambda x:0.5 if x>1 else x)
    regulation_implementation['GASGEB_BURSTWORKERNUM_rate']=regulation_implementation['GASGEB_BURSTWORKERNUM_rate'].apply(lambda x:0.5 if x>1 else x)
    rcol=['GASGEN_TENICHNUM_rate','GASGEB_BURSTWORKERNUM_rate']
    for ft in rcol:
        regulation_implementation[ft]=regulation_implementation[ft].apply(lambda x:0.5 if x>1 else x)*100
        regulation_implementation[ft]=regulation_implementation[ft].fillna(10000).round(2)
        regulation_implementation[ft]=regulation_implementation[ft].astype('str')
        regulation_implementation[ft]=regulation_implementation[ft]+'%'
        regulation_implementation[ft]=regulation_implementation[ft].apply(lambda x:'未填报' if x=='10000.0%' else x)
    #regulation_implementation=regulation_implementation.round(2)
    wancol=['FXDYJ_CCJE','A_FXDYJ_DMTQJE','FXDYJ_SYJE','A_FXDYJ_JYJE']
    for ft in wancol:
        regulation_implementation[ft]=regulation_implementation[ft].fillna(100000000).round(2)
        regulation_implementation[ft]=regulation_implementation[ft].astype('str')
        regulation_implementation[ft]=regulation_implementation[ft]+'万元'
        regulation_implementation[ft]=regulation_implementation[ft].apply(lambda x:'未填报' if x=='100000000.0万元' else x)
    #缺失值填充
    regulation_implementation=regulation_implementation.fillna('未填报')
    #############################################机运设备##################################################
    #机电设备
    ## T_AJ1_MJ_JCXX_CMGZMJD  机采率、炮采个数、普采个数、综采个数
    # df10A = T_AJ1_MJ_JCXX_CMGZMJD[['CORP_ID','REPORT_YEAR']].groupby(['CORP_ID']).max().reset_index(drop=False)
    # df10B = T_AJ1_MJ_JCXX_CMGZMJD[['CORP_ID','REPORT_YEAR','CMGZM_JCL','CMGZM_PCGS','CMGZM_PC','CMGZM_ZCGS']]
    # df10 = pd.merge(df10A,df10B,on=['CORP_ID','REPORT_YEAR'],how='inner')
    # df10 = df10.drop(['REPORT_YEAR'],axis=1)
    # df10 = df10.drop_duplicates(['CORP_ID'])
    df10=T_AJ1_MJ_JCXX_CMGZMJD[T_AJ1_MJ_JCXX_CMGZMJD['REPORT_YEAR']==str(datetime.datetime.now().year-1)].drop_duplicates(['CORP_ID'])
    df10=df10[['CORP_ID','CMGZM_JCL','CMGZM_PCGS','CMGZM_PC','CMGZM_ZCGS']]

    ##通风方式
    df10_1=V_AJ1_MJ_JCXX_BASEINFO[['CORP_ID','MINE_VENTILATESTYLE_NAME']]

    ##自动化开采个数
    # df10_2A = T_AJ1_MJ_JCXX_CMGZMJD[['CORP_ID','REPORT_YEAR']].groupby(['CORP_ID']).max().reset_index(drop=False)
    # df10_2B = T_AJ1_MJ_JCXX_CMGZMJD[['CORP_ID','REPORT_YEAR','CMGZM_ZDHGS']]
    # df10_2 = pd.merge(df10_2A,df10_2B,on=['CORP_ID','REPORT_YEAR'],how='inner')
    # df10_2 = df10_2.drop(['REPORT_YEAR'],axis=1)
    # df10_2 = df10_2.drop_duplicates(['CORP_ID'])
    df10_2=T_AJ1_MJ_JCXX_CMGZMJD[T_AJ1_MJ_JCXX_CMGZMJD['REPORT_YEAR']==str(datetime.datetime.now().year-1)].drop_duplicates(['CORP_ID'])
    df10_2=df10_2[['CORP_ID','CMGZM_ZDHGS']]

    ##默认赋值
    df11=total_frame[['CORP_ID']]
    df11['Equip_equipment_as_required']='暂未接入数据'
    df11['equipment_selection_reasonable']='暂未接入数据'
    df11['equipment_maintenance']='暂未接入数据'
    df11['equipment_protection']='暂未接入数据'
    df11['Full_warning_signs']='暂未接入数据'
    df11['Complete_protective_facilities']='暂未接入数据'

    # 运输设备
    ##运输方式
    df12=V_AJ1_MJ_JCXX_BASEINFO[['CORP_ID','MINE_TRANSMITSTYLE_NAME']]

    #合并机运设备数据
    Chance_equipment_10=pd.merge(total_frame,df10,on=['CORP_ID'],how='left')
    Chance_equipment_10_1=pd.merge(Chance_equipment_10,df10_1,on=['CORP_ID'],how='left')
    Chance_equipment_10_2=pd.merge(Chance_equipment_10_1,df10_2,on=['CORP_ID'],how='left')
    Chance_equipment_11=pd.merge(Chance_equipment_10_2,df11,on=['CORP_ID'],how='left')
    Chance_equipment=pd.merge(Chance_equipment_11,df12,on=['CORP_ID'],how='left')
    #重命名
    Chance_equipment=Chance_equipment.rename(columns={'MINE_VENTILATESTYLE_NAME':'MINE_VENTILATESTYLE','MINE_TRANSMITSTYLE_NAME':'MINE_TRANSMITSTYLE'})
    #列名校验
    Chance_equipment_allcol=list(total_frame.columns)+['CMGZM_JCL','CMGZM_PCGS','CMGZM_PC','CMGZM_ZCGS','MINE_VENTILATESTYLE','CMGZM_ZDHGS','Equip_equipment_as_required','equipment_selection_reasonable','equipment_maintenance','equipment_protection','Full_warning_signs','Complete_protective_facilities','MINE_TRANSMITSTYLE']
    Chance_equipment=Chance_equipment[Chance_equipment_allcol]
    #百分比处理
    jclcol=['CMGZM_JCL']
    for ft in jclcol:
        Chance_equipment[ft]=Chance_equipment[ft].apply(lambda x:0.5 if x>1 else x)*100
        Chance_equipment[ft]=Chance_equipment[ft].fillna(10000).round(2)
        Chance_equipment[ft]=Chance_equipment[ft].astype('str')
        Chance_equipment[ft]=Chance_equipment[ft]+'%'
        Chance_equipment[ft]=Chance_equipment[ft].apply(lambda x:'未填报' if x=='10000.0%' else x)
    #个数处理
    jygs=['CMGZM_PCGS','CMGZM_PC','CMGZM_ZCGS','CMGZM_ZDHGS']
    for ft in jygs:
        Chance_equipment[ft]=Chance_equipment[ft].fillna(100000000).astype('int')
        Chance_equipment[ft]=Chance_equipment[ft].astype('str')
        Chance_equipment[ft]=Chance_equipment[ft]+'个'
        Chance_equipment[ft]=Chance_equipment[ft].apply(lambda x:'未填报' if x=='100000000个' else x)
    #最后填充
    Chance_equipment=Chance_equipment.fillna('未填报')
    #############################################灾害状况##################################################
    #瓦斯爆炸和突出状况
    ## T_AJ1_MJ_JCXX_WSCFLYGL  最大瓦斯抽放浓度、最小瓦斯抽放浓度、平均瓦斯抽放浓度、设计瓦斯抽放浓度、最大瓦斯抽放负压、最小瓦斯抽放负压、平均瓦斯抽放负压
    T_AJ1_MJ_JCXX_WSCFLYGL=T_AJ1_MJ_JCXX_WSCFLYGL[T_AJ1_MJ_JCXX_WSCFLYGL['REPORT_YEAR'].notnull()]
    T_AJ1_MJ_JCXX_WSCFLYGL['REPORT_YEAR'] = T_AJ1_MJ_JCXX_WSCFLYGL['REPORT_YEAR'].astype('int64')
    T_AJ1_MJ_JCXX_WSCFLYGL=T_AJ1_MJ_JCXX_WSCFLYGL[T_AJ1_MJ_JCXX_WSCFLYGL['REPORT_YEAR']>0]
    df13A = T_AJ1_MJ_JCXX_WSCFLYGL[['CORP_ID','REPORT_YEAR']].groupby(['CORP_ID']).max().reset_index(drop=False)
    df13B = T_AJ1_MJ_JCXX_WSCFLYGL[['CORP_ID','REPORT_YEAR','GASUTIL_CONMAX','GASUTIL_CONMIN','GASUTIL_CONAVG','GASUTIL_PREDESIGN','GASUTIL_PREMAX','GASUTIL_PREMIN','GASUTIL_PREAVG']]
    df13A['REPORT_YEAR'] = df13A['REPORT_YEAR'].astype('str')
    df13B['REPORT_YEAR'] = df13B['REPORT_YEAR'].astype('str')
    df13 = pd.merge(df13A,df13B,on=['CORP_ID','REPORT_YEAR'],how='inner')
    df13 = df13.drop(['REPORT_YEAR'],axis=1)
    df13 = df13.drop_duplicates(['CORP_ID'])
    #T_AJ1_MJ_JCXX_WSCFLYGL['REPORT_YEAR']=T_AJ1_MJ_JCXX_WSCFLYGL['REPORT_YEAR'].fillna(0).astype('int').astype('str')
    # df13=T_AJ1_MJ_JCXX_WSCFLYGL[T_AJ1_MJ_JCXX_WSCFLYGL['REPORT_YEAR']==str(datetime.datetime.now().year-1)].drop_duplicates(['CORP_ID'])
    # df13=df13[['CORP_ID','GASUTIL_CONMAX','GASUTIL_CONMIN','GASUTIL_CONAVG','GASUTIL_PREDESIGN','GASUTIL_PREMAX','GASUTIL_PREMIN','GASUTIL_PREAVG']]


    ## 瓦斯等级	MINE_WS_GRADE
    df14=V_AJ1_MJ_JCXX_BASEINFO[['CORP_ID','MINE_WS_GRADE_NAME']]
    #冲击地压状况
    # T_AJ1_MJ_JCXX_BASEINFO 是否有冲击地压
    df15=V_AJ1_MJ_JCXX_BASEINFO[['CORP_ID','ROCKBURST_NAME']]
    #水文地址状况
    ## 水文地质类型
    df16=V_AJ1_MJ_JCXX_BASEINFO[['CORP_ID','HYDROGEOLOGICAL_TYPE_NAME']]
    ##平均涌水量、最大涌水量
    df17=V_AJ1_MJ_JCXX_BASEINFO[['CORP_ID','MINE_WATERBURST','MINE_WATERBURST_MAX']]
    #默认赋值-顶板管理
    df18=total_frame[['CORP_ID']]
    df18['roof_caving']='暂未接入数据'
    #煤层自然倾向性
    df19=V_AJ1_MJ_JCXX_BASEINFO[['CORP_ID','MINE_FIRE_NAME']]

    #合并灾害状况
    Disaster_situation_13=pd.merge(total_frame,df13,on=['CORP_ID'],how='left')
    Disaster_situation_14=pd.merge(Disaster_situation_13,df14,on=['CORP_ID'],how='left')
    Disaster_situation_15=pd.merge(Disaster_situation_14,df15,on=['CORP_ID'],how='left')
    Disaster_situation_16=pd.merge(Disaster_situation_15,df16,on=['CORP_ID'],how='left')
    Disaster_situation_17=pd.merge(Disaster_situation_16,df17,on=['CORP_ID'],how='left')
    Disaster_situation_18=pd.merge(Disaster_situation_17,df18,on=['CORP_ID'],how='left')
    Disaster_situation=pd.merge(Disaster_situation_18,df19,on=['CORP_ID'],how='left')
    #重命名
    dicol={'MINE_WS_GRADE_NAME':'MINE_WS_GRADE',
           'ROCKBURST_NAME':'ROCKBURST',
           'HYDROGEOLOGICAL_TYPE_NAME':'HYDROGEOLOGICAL_TYPE',
           'MINE_WATERBURST':'MINE_WATERBURST',
           'MINE_WATERBURST_MAX':'MINE_WATERBURST_MAX',
           'MINE_FIRE_NAME':'MINE_FIRE',
           }

    Disaster_situation=Disaster_situation.rename(columns=dicol)

    #瓦斯爆炸和突出状况
    Gas_explosion_outburst=['GASUTIL_CONMAX','GASUTIL_CONMIN','GASUTIL_CONAVG','GASUTIL_PREDESIGN',
                            'GASUTIL_PREMAX','GASUTIL_PREMIN','GASUTIL_PREAVG','MINE_WS_GRADE']

    #冲击地压状况
    Impact_pressure_condition=['ROCKBURST']
    #水文地质状况
    Hydrogeological_condition=['HYDROGEOLOGICAL_TYPE','MINE_WATERBURST','MINE_WATERBURST_MAX']
    #顶板管理
    roof_control=['roof_caving']
    #自燃倾向状况
    Spontaneous_combustion_tendency=['MINE_FIRE']
    #最终列名
    Spontaneous_combustion_allcol=list(total_frame.columns)+Gas_explosion_outburst+Impact_pressure_condition+Hydrogeological_condition+roof_control+Spontaneous_combustion_tendency
    #生成结果
    Disaster_situation=Disaster_situation[Spontaneous_combustion_allcol]
    #Disaster_situation=Disaster_situation.round(4)
    Disaster_situation['MINE_WS_GRADE']=Disaster_situation['MINE_WS_GRADE'].fillna('无需填报')
    #瓦斯抽放
    cfcol=['GASUTIL_CONMAX','GASUTIL_CONMIN','GASUTIL_CONAVG','GASUTIL_PREDESIGN','GASUTIL_PREMAX','GASUTIL_PREMIN','GASUTIL_PREAVG']
    #Disaster_situation.loc[Disaster_situation['MINE_WS_GRADE']=='低瓦斯',cfcol]='无需填报'
    Disaster_situation.loc[Disaster_situation['MINE_WS_GRADE']=='低瓦斯',cfcol]=10000
    #百分比
    for ft in cfcol[:4]:
        Disaster_situation[ft]=Disaster_situation[ft].apply(lambda x:50 if abs(x)>100 or x<0 else x)
        Disaster_situation[ft]=Disaster_situation[ft].fillna(100000000).round(2)
        Disaster_situation[ft]=Disaster_situation[ft].astype('str')
        Disaster_situation[ft]=Disaster_situation[ft]+'%'
        Disaster_situation[ft]=Disaster_situation[ft].apply(lambda x:'无需填报' if x=='10000.0%' else '未填报' if x=='100000000.0%' else x)
    #瓦斯抽放负压
    for ft in cfcol[-3:]:
        Disaster_situation[ft]=Disaster_situation[ft].fillna(100000000).round(2)
        Disaster_situation[ft]=Disaster_situation[ft].astype('str')
        Disaster_situation[ft]=Disaster_situation[ft]+'Kpa'
        Disaster_situation[ft]=Disaster_situation[ft].apply(lambda x:'未填报' if x=='100000000.0Kpa' else '无需填报' if x=='10000.0Kpa' else x)

    #涌水量
    watercol=['MINE_WATERBURST','MINE_WATERBURST_MAX']
    for ft in watercol:
        Disaster_situation[ft]=Disaster_situation[ft].fillna(100000000).round(2)
        Disaster_situation[ft]=Disaster_situation[ft].astype('str')
        Disaster_situation[ft]=Disaster_situation[ft]+'(* m³/h)'
        Disaster_situation[ft]=Disaster_situation[ft].apply(lambda x:'未填报' if x=='100000000.0(* m³/h)' else x)
    #最后填充缺失值
    Disaster_situation=Disaster_situation.fillna('未填报')
    ########################################自然地质条件#####################################
    #周边地质条件
    ## T_AJ1_MJ_JCXX_ZCMC 煤层平均厚度、煤层最大厚度、上覆煤层间距、下伏煤层间距、煤层平均倾角
    df20A = T_AJ1_MJ_JCXX_ZCMC[['CORP_ID','UPDATE_DATE']].groupby(['CORP_ID']).max().reset_index(drop=False)
    df20B = T_AJ1_MJ_JCXX_ZCMC[['CORP_ID','UPDATE_DATE','MINE_THICK_AVG','MINE_THICK_MAX','A_SFMCJJ','A_XFMCJJ','A_MCPJQJ']]
    df20 = pd.merge(df20A,df20B,on=['CORP_ID','UPDATE_DATE'],how='inner')
    df20 = df20.drop(['UPDATE_DATE'],axis=1)
    df20 = df20.drop_duplicates(['CORP_ID'])

    # df20=T_AJ1_MJ_JCXX_ZCMC[T_AJ1_MJ_JCXX_ZCMC['UPDATE_DATE'].astype('str').str[:4]==str(datetime.datetime.now().year-1)].drop_duplicates(['CORP_ID'])
    # df20=df20[['CORP_ID','MINE_THICK_AVG','MINE_THICK_MAX','A_SFMCJJ','A_XFMCJJ','A_MCPJQJ']]


    #默认赋值-气候
    df21=total_frame[['CORP_ID']]
    df21['mean_annual_precipitation']='暂未接入数据'
    df21['Average_hot_days']='暂未接入数据'
    df21['Mean_wind_speed']='暂未接入数据'
    #默认赋值-自然灾害
    df22=total_frame[['CORP_ID']]
    df22['Annual_landslide']='暂未接入数据'
    #合并自然地质条件数据
    Natural_geological_condition_20=pd.merge(total_frame,df20,on=['CORP_ID'],how='left')
    Natural_geological_condition_21=pd.merge(Natural_geological_condition_20,df21,on=['CORP_ID'],how='left')
    Natural_geological_condition=pd.merge(Natural_geological_condition_21,df22,on=['CORP_ID'],how='left')
    #调整字段名称和顺序
    Natural_geological_col=list(total_frame.columns)+['MINE_THICK_AVG','MINE_THICK_MAX','A_SFMCJJ','A_XFMCJJ',
                                                      'A_MCPJQJ','mean_annual_precipitation','Average_hot_days','Mean_wind_speed','Annual_landslide']
    Natural_geological_condition=Natural_geological_condition[Natural_geological_col]
    #加单位
    mcol=['MINE_THICK_AVG','MINE_THICK_MAX','A_SFMCJJ','A_XFMCJJ','A_MCPJQJ']
    for ft in mcol:
        if ft in mcol[:4]:
            Natural_geological_condition[ft]=Natural_geological_condition[ft].fillna(100000000).round(2)
            Natural_geological_condition[ft]=Natural_geological_condition[ft].astype('str')
            Natural_geological_condition[ft]=Natural_geological_condition[ft]+'m'
            Natural_geological_condition[ft]=Natural_geological_condition[ft].apply(lambda x:'未填报' if x=='100000000.0m' else x)
        else:
            Natural_geological_condition[ft]=Natural_geological_condition[ft].fillna(100000000).round(2)
            Natural_geological_condition[ft]=Natural_geological_condition[ft].astype('str')
            Natural_geological_condition[ft]=Natural_geological_condition[ft]+'°'
            Natural_geological_condition[ft]=Natural_geological_condition[ft].apply(lambda x:'未填报' if x=='100000000.0°' else x)
    #缺失值处理
    Natural_geological_condition=Natural_geological_condition.fillna('未填报')
    ##############################################历史事故#########################################
    #T_AJ1_MJ_SG_ACCIDENT_INFO 
    ##  ACCIDENT_MINE_ID   =  CORP_ID
    # 事故等级	ACCIDENT_LEVEL
    # 事故类别 ====   ACCIDENT_CATEGORY
    ##  HURT_PEOPLE_COLL_ID ===== 事故受伤人员统计记录编号
    df23A = T_AJ1_MJ_SG_ACCIDENT_INFO[['ACCIDENT_MINE_ID','UPDATE_DATE']].groupby(['ACCIDENT_MINE_ID']).max().reset_index(drop=False)
    df23B = T_AJ1_MJ_SG_ACCIDENT_INFO[['ACCIDENT_MINE_ID','UPDATE_DATE','ACCIDENT_LEVEL','ACCIDENT_CATEGORY','HURT_PEOPLE_COLL_ID']]
    df23 = pd.merge(df23A,df23B,on=['ACCIDENT_MINE_ID','UPDATE_DATE'],how='inner')
    df23 = df23.drop(['UPDATE_DATE'],axis=1)
    df23 = df23.drop_duplicates(['ACCIDENT_MINE_ID'])
    df23 = df23.rename(columns={'ACCIDENT_MINE_ID':'CORP_ID'})

    #SG_HURT_PEOPLE_COLL_INFO 事故死亡人数、事故重伤人数、事故轻伤人数
    df24 = pd.DataFrame()
    df24['HURT_PEOPLE_COLL_ID'] = SG_HURT_PEOPLE_COLL_INFO['ID']
    df24['DIE_PEOPLE_NUM'] = SG_HURT_PEOPLE_COLL_INFO['DIE_PEOPLE_NUM']
    df24['WEIGHT_HURT_PEOPLE_NUM'] = SG_HURT_PEOPLE_COLL_INFO['WEIGHT_HURT_PEOPLE_NUM']
    df24['LIGHT_HURT_PEOPLE_NUM'] = SG_HURT_PEOPLE_COLL_INFO['LIGHT_HURT_PEOPLE_NUM']
    #
    df24_1 = df24.groupby(['HURT_PEOPLE_COLL_ID'])['DIE_PEOPLE_NUM'].sum().to_frame().reset_index(drop=False)
    df24_2 = df24.groupby(['HURT_PEOPLE_COLL_ID'])['WEIGHT_HURT_PEOPLE_NUM'].sum().to_frame().reset_index(drop=False)
    df24_3 = df24.groupby(['HURT_PEOPLE_COLL_ID'])['LIGHT_HURT_PEOPLE_NUM'].sum().to_frame().reset_index(drop=False)

    df23 = pd.merge(df23, df24_1,on=['HURT_PEOPLE_COLL_ID'],how='left' )
    df23 = pd.merge(df23, df24_2,on=['HURT_PEOPLE_COLL_ID'],how='left' )
    df23 = pd.merge(df23, df24_3,on=['HURT_PEOPLE_COLL_ID'],how='left' )
    df23 = df23.drop(['HURT_PEOPLE_COLL_ID'],axis =1)
    df23['ACCIDENT_LEVEL']=df23['ACCIDENT_LEVEL'].astype('int').astype('str')
    df23['ACCIDENT_CATEGORY']=df23['ACCIDENT_CATEGORY'].astype('int').astype('str')
    #事故等级中文
    df23['ACCIDENT_LEVEL']=df23['ACCIDENT_LEVEL'].apply(lambda x:'一般事故' if x=='10' else '较大事故' if x=='20' else '重大事故' if x=='30' else '特别重大事故' if x=='40' else '无事故')
    #事故类别中文
    #事故类别中文转换函数
    def ACCIDENT_CATEGORY_T(x):
        if x=='10':
            return '顶板'
        if x=='15':
            return '煤尘'    
        if x=='20':
            return '瓦斯'
        if x=='25':
            return '冲击地压 '
        if x=='30':
            return '水害'    
        if x=='40':
            return '运输'
        if x=='50':
            return '机电'
        if x=='60':
            return '爆破'    
        if x=='70':
            return '火灾'
        if x=='90':
            return '其他'
        if x=='1010':
            return '冒顶'    
        if x=='1020':
            return '片帮'
        if x=='1030':
            return '顶板掉矸'
        if x=='1040':
            return '顶板支护垮倒'    
        if x=='2510':
            return '冲击地压'
        if x=='1060':
            return '露天煤矿边坡滑移垮塌'
        if x=='2010':
            return '瓦斯爆炸'    
        if x=='1510':
            return '煤尘爆炸'
        if x=='2030':
            return '煤与瓦斯（突出）'
        if x=='2051':
            return '二氧化碳（突出）'    
        if x=='2040':
            return '中毒窒息'
        if x=='4010':
            return '坠罐'
        if x=='4020':
            return '跑车'    
        if x=='4030':
            return '其他'
        if x=='7010':
            return '井下火灾-内因'
        if x=='7020':
            return '井下火灾-外因'    
        if x=='7030':
            return '地面火灾'
        if x=='9010':
            return '捅溜煤眼'
        if x=='9020':
            return '火药储运过程中的爆炸事故'    
        if x=='9030':
            return '其他'
        if x=='9040':
            return '坠落'
        else:
            return '无事故'
    #调用函数转换事故类别
    df23['ACCIDENT_CATEGORY']=df23['ACCIDENT_CATEGORY'].apply(ACCIDENT_CATEGORY_T)
    #历史事故合并数据:注意事故等级和类型缺失应该是无事故、无类型的情况！
    Historical_accident=pd.merge(total_frame,df23,on=['CORP_ID'],how='left')
    Historical_accident=Historical_accident[list(total_frame.columns)+['ACCIDENT_CATEGORY','DIE_PEOPLE_NUM','ACCIDENT_LEVEL','WEIGHT_HURT_PEOPLE_NUM','LIGHT_HURT_PEOPLE_NUM']]
    Historical_accident=Historical_accident.round(4)
    Historical_accident[['ACCIDENT_CATEGORY','ACCIDENT_LEVEL']]=Historical_accident[['ACCIDENT_CATEGORY','ACCIDENT_LEVEL']].fillna('无事故')
    Historical_accident=Historical_accident.fillna(0)
    #加上事故人数单位
    sgcol=['DIE_PEOPLE_NUM','WEIGHT_HURT_PEOPLE_NUM','LIGHT_HURT_PEOPLE_NUM']
    for ft in sgcol:
        Historical_accident[ft]=Historical_accident[ft].astype('int')
        Historical_accident[ft]=Historical_accident[ft].astype('str')
        Historical_accident[ft]=Historical_accident[ft]+'人'
    ############################################监管监察#########################################
    jczf_paper_copy = jczf_paper.copy()
    # inner 三张表，选取所需的字段
    jczf_paper_copy['PAPER_TYPE']=jczf_paper_copy['PAPER_TYPE'].astype('str')
    jczf_paper_copy = jczf_paper_copy[jczf_paper_copy['PAPER_TYPE']=='1']
    jczf_paper_1 = pd.DataFrame()
    jczf_paper_1['CASE_ID'] = jczf_paper_copy['CASE_ID']
    jczf_paper_1['PAPER_ID'] = jczf_paper_copy['PAPER_ID']
    jczf_paper_1['CORP_ID'] = jczf_paper_copy['CORP_ID']

    jczf_case_1 = pd.DataFrame()
    jczf_case_1['CASE_ID'] = jczf_case['CASE_ID']

    jczf_danger_1 = pd.DataFrame()
    jczf_danger_1['PAPER_ID'] = jczf_danger['PAPER_ID']
    jczf_danger_1['ONSITE_TYPE'] = jczf_danger['ONSITE_TYPE']
    jczf_danger_1['PENALTY_TYPE'] = jczf_danger['PENALTY_TYPE']
    jczf_danger_1['IS_HIGH'] = jczf_danger['IS_HIGH']
    jczf_danger_1['COALING_FACE'] = jczf_danger['COALING_FACE']
    jczf_danger_1['HEADING_FACE'] = jczf_danger['HEADING_FACE']

    # inner三张表，去重，删除了不需要的字段
    t_frame = pd.merge(jczf_paper_1, jczf_case_1,on=['CASE_ID'],how='inner')
    t_frame = pd.merge(t_frame, jczf_danger_1,on=['PAPER_ID'],how='inner')
    t_frame = t_frame.drop('CASE_ID',axis= 1)

    # 数据预处理，填补，去矿井中的空值
    t_frame['PENALTY_TYPE'] = t_frame['PENALTY_TYPE'].fillna(0)
    t_frame['ONSITE_TYPE'] = t_frame['ONSITE_TYPE'].fillna(0)
    t_frame['COALING_FACE'] = t_frame['COALING_FACE'].fillna(0)
    t_frame['HEADING_FACE'] = t_frame['HEADING_FACE'].fillna(0)
    t_frame = t_frame.dropna()

    #现场处理 
    ONSITE_TYPE=pd.DataFrame()
    ONSITE_TYPE=t_frame[['PAPER_ID','CORP_ID','ONSITE_TYPE']]
    # ONSITE_TYPE['CORP_ID'] = t_frame['CORP_ID']
    # ONSITE_TYPE['ONSITE_TYPE'] = t_frame['ONSITE_TYPE']

    # 停止采掘工作面 = COALING_FACE（采煤工作面）+ HEADING_FACE（掘进工作面）
    # Stop_mining_face['CORP_ID'] = Stop_mining_face['CORP_ID'].astype('int64')
    df_spface = pd.DataFrame()
    df_spface['PAPER_ID'] = t_frame['PAPER_ID']
    df_spface['CORP_ID'] = t_frame['CORP_ID']
    df_spface['Stop_mining_face'] = t_frame['COALING_FACE'].astype(float)+t_frame['HEADING_FACE'].astype(float)
    Stop_mining_face = df_spface.groupby('CORP_ID')['PAPER_ID'].nunique().reset_index(drop=False)
    Stop_mining_face = Stop_mining_face.rename(columns={'PAPER_ID':'Stop_mining_face'})
    #类型转换
    ONSITE_TYPE['ONSITE_TYPE'] = ONSITE_TYPE['ONSITE_TYPE'].astype(float)

    # 责令立即停止生产  编码 8
    # stop_production['CORP_ID'] = stop_production['CORP_ID'].astype('int64')
    df_8 = ONSITE_TYPE[ONSITE_TYPE['ONSITE_TYPE']==8.0]
    stop_production = df_8.groupby('CORP_ID')['PAPER_ID'].nunique().reset_index(drop=False)
    stop_production = stop_production.rename(columns={'PAPER_ID':'stop_production'})

    # 责令立即停止作业  编码 7
    # stop_work['CORP_ID'] = stop_work['CORP_ID'].astype('int64')
    df_7 = ONSITE_TYPE[ONSITE_TYPE['ONSITE_TYPE']==7.0]
    stop_work = df_7.groupby('CORP_ID')['PAPER_ID'].nunique().reset_index(drop=False)
    stop_work = stop_work.rename(columns={'PAPER_ID':'stop_work'})

    # 停止使用相关设备  编码 6
    # Stop_using_equipment['CORP_ID'] = Stop_using_equipment['CORP_ID'].astype('int64')
    df_6 = ONSITE_TYPE[ONSITE_TYPE['ONSITE_TYPE']==6.0]
    Stop_using_equipment = df_7.groupby('CORP_ID')['PAPER_ID'].nunique().reset_index(drop=False)
    Stop_using_equipment = Stop_using_equipment.rename(columns={'PAPER_ID':'Stop_using_equipment'})

    # 从危险区域撤出作业人员  编码 12
    # Evacuation['CORP_ID'] = Evacuation['CORP_ID'].astype('int64')
    df_12 = ONSITE_TYPE[ONSITE_TYPE['ONSITE_TYPE']==12.0]
    Evacuation = df_12.groupby('CORP_ID')['PAPER_ID'].nunique().reset_index(drop=False)
    Evacuation = Evacuation.rename(columns={'PAPER_ID':'Evacuation'})

    #行政处罚 
    PENALTY_TYPE = pd.DataFrame()
    PENALTY_TYPE = t_frame[['PAPER_ID','CORP_ID','PENALTY_TYPE']]
    #PENALTY_TYPE['CORP_ID'] = t_frame['CORP_ID']
    #PENALTY_TYPE['PENALTY_TYPE'] = t_frame['PENALTY_TYPE']
    PENALTY_TYPE['PENALTY_TYPE'] = PENALTY_TYPE['PENALTY_TYPE'].astype(float)

    # 责令停产整顿编码 3
    # rectification['CORP_ID'] = rectification['CORP_ID'].astype('int64')
    df_3 = ONSITE_TYPE[ONSITE_TYPE['ONSITE_TYPE']==3]
    rectification = df_3.groupby('CORP_ID')['PAPER_ID'].nunique().reset_index(drop=False)
    rectification = rectification.rename(columns={'PAPER_ID':'rectification'})

    # 暂扣安全生产许可证  编码 7
    # Temporary_suspension_license['CORP_ID'] = Temporary_suspension_license['CORP_ID'].astype('int64')
    df_7_1 = ONSITE_TYPE[ONSITE_TYPE['ONSITE_TYPE']==7]
    Temporary_suspension_license = df_7_1.groupby('CORP_ID')['PAPER_ID'].nunique().reset_index(drop=False)
    Temporary_suspension_license = Temporary_suspension_license.rename(columns={'PAPER_ID':'Temporary_suspension_license'})

    # 责令停止建设  编码 5
    # stop_construction['CORP_ID'] = stop_construction['CORP_ID'].astype('int64')
    df_5 = ONSITE_TYPE[ONSITE_TYPE['ONSITE_TYPE']==5]
    stop_construction = df_5.groupby('CORP_ID')['PAPER_ID'].nunique().reset_index(drop=False)
    stop_construction = stop_construction.rename(columns={'PAPER_ID':'stop_construction'})

    # 处罚金额
    P8_PENALTY = pd.DataFrame()
    P8_PENALTY = jczf_paper[['CORP_ID','NAME','P8_PENALTY']]
    # P8_PENALTY['NAME'] = jczf_paper['NAME']
    # P8_PENALTY['P8_PENALTY'] = jczf_paper['P8_PENALTY']

    P8_PENALTY['P8_PENALTY'] = P8_PENALTY['P8_PENALTY'].fillna(0)
    P8_PENALTY['NAME'] = P8_PENALTY['NAME'].fillna(0)
    P8_PENALTY = P8_PENALTY.dropna()
    P8_PENALTY['P8_PENALTY'] = P8_PENALTY['P8_PENALTY'].astype(float)

    PENALTY = P8_PENALTY[P8_PENALTY['NAME']=='行政处罚决定书'].groupby('CORP_ID')['P8_PENALTY'].sum().reset_index(drop=False).round(0)
    PENALTY = PENALTY.rename(columns={'P8_PENALTY':'PENALTY'})
    PENALTY['PENALTY'] = PENALTY['PENALTY'].astype(float)


    #处罚次数
    Number_of_penalties = pd.DataFrame()
    Number_of_penalties = jczf_paper[['CORP_ID','PAPER_ID','NAME']]
    # Number_of_penalties['PAPER_ID'] = jczf_paper['PAPER_ID']
    # Number_of_penalties['NAME'] = jczf_paper['NAME']

    penalties_times = Number_of_penalties[Number_of_penalties['NAME']=='行政处罚决定书'].groupby('CORP_ID')['PAPER_ID'].nunique().reset_index(drop=False)
    penalties_times = penalties_times.rename(columns={'PAPER_ID':'penalties_times'})
    penalties_times['penalties_times'] = penalties_times['penalties_times'].astype(float)

    #屡查屡犯
    lclf_1 = pd.DataFrame()
    lclf_1 = lclf[['CORP_ID','lclf']]
    #lclf_1['lclf'] = lclf['lclf']
    lclf_1 = lclf_1.dropna()
    lclf_1 = lclf_1.drop_duplicates()

    Repeated = lclf_1[lclf_1['lclf']!='需专家进一步鉴定'].groupby('CORP_ID')['lclf'].nunique().reset_index(drop=False)
    Repeated = Repeated.rename(columns={'lclf':'Repeated'})
    Repeated['Repeated'] = Repeated['Repeated'].astype(float)

    ####20200525计算重大隐患和一般隐患更新
    jczf_paper['DEL_FLAG']=jczf_paper['DEL_FLAG'].astype('str')
    jczf_paper['PAPER_TYPE']=jczf_paper['PAPER_TYPE'].astype('str')
    jczf_danger['DEL_FLAG']=jczf_danger['DEL_FLAG'].astype('str')
    jczf_paper0=jczf_paper[(jczf_paper['DEL_FLAG']=='0')&(jczf_paper['PAPER_TYPE']=='1')]
    jczf_paper0=jczf_paper0.drop(columns=['DEL_FLAG'])
    jczf_danger0=jczf_danger[(jczf_danger['DEL_FLAG']=='0')]
    yh=pd.merge(jczf_paper0,jczf_danger0,on=['PAPER_ID'],how='inner')
    #计算重大隐患数
    df25=yh[(yh['IS_HIGH']=='1')].groupby(['CORP_ID'])[['CORP_ID']].size().reset_index()
    df25.columns=['CORP_ID','YHPCZ_ZDYHS']
    YBYHS=yh[(yh['IS_HIGH']=='0')].groupby(['CORP_ID'])[['CORP_ID']].size().reset_index()
    YBYHS.columns=['CORP_ID','YBYHS']

    ##一般隐患 IS_HIGH jczf_danger
    IS_HIGH = pd.DataFrame()
    IS_HIGH = t_frame[['CORP_ID','PAPER_ID','IS_HIGH']]
    # IS_HIGH['PAPER_ID'] = t_frame['PAPER_ID']
    # IS_HIGH['IS_HIGH'] = t_frame['IS_HIGH']

    # YBYHS = IS_HIGH[IS_HIGH['IS_HIGH']==0].groupby('CORP_ID')['PAPER_ID'].nunique().reset_index(drop=False)
    # YBYHS = YBYHS.rename(columns={'PAPER_ID':'YBYHS'})
    # YBYHS['YBYHS'] = YBYHS['YBYHS'].astype(float)

    #合并监管监察数据
    ##现场处理
    # stop_production	责令立即停止生产次数
    # stop_work	责令立即停止作业次数
    # Stop_mining_face	停止采掘工作面次数
    # Stop_using_equipment	停止使用相关设备次数
    # Evacuation	从危险区域撤出作业人员次数
    Regulation_supervisi_1=pd.merge(total_frame,stop_production,on=['CORP_ID'],how='left')
    Regulation_supervisi_2=pd.merge(Regulation_supervisi_1,stop_work,on=['CORP_ID'],how='left')
    Regulation_supervisi_3=pd.merge(Regulation_supervisi_2,Stop_mining_face,on=['CORP_ID'],how='left')
    Regulation_supervisi_4=pd.merge(Regulation_supervisi_3,Stop_using_equipment,on=['CORP_ID'],how='left')
    Regulation_supervisi_5=pd.merge(Regulation_supervisi_4,Evacuation,on=['CORP_ID'],how='left')

    ##行政处罚
    # rectification	责令停产整顿次数
    # Temporary_suspension_license	暂扣安全生产许可证次数
    # stop_construction	责令停止建设次数
    # PENALTY	总处罚金额
    # penalties_times	处罚次数
    # Repeated	屡查屡犯
    Regulation_supervisi_6=pd.merge(Regulation_supervisi_5,rectification,on=['CORP_ID'],how='left')
    Regulation_supervisi_7=pd.merge(Regulation_supervisi_6,Temporary_suspension_license,on=['CORP_ID'],how='left')
    Regulation_supervisi_8=pd.merge(Regulation_supervisi_7,stop_construction,on=['CORP_ID'],how='left')
    Regulation_supervisi_9=pd.merge(Regulation_supervisi_8,PENALTY,on=['CORP_ID'],how='left')
    Regulation_supervisi_10=pd.merge(Regulation_supervisi_9,penalties_times,on=['CORP_ID'],how='left')
    Regulation_supervisi_11=pd.merge(Regulation_supervisi_10,Repeated,on=['CORP_ID'],how='left')

    #历史隐患发生情况
    # YHPCZ_ZDYHS	重大隐患数
    # YBYHS	一般隐患数
    Regulation_supervisi_12=pd.merge(Regulation_supervisi_11,df25,on=['CORP_ID'],how='left')
    Regulation_supervisi=pd.merge(Regulation_supervisi_12,YBYHS,on=['CORP_ID'],how='left')
    Regulation_supervisi=Regulation_supervisi.fillna(0)
    #字段名和顺序都没问题
    jgjcol=['stop_production','stop_work','Stop_mining_face','Stop_using_equipment','Evacuation','rectification','Temporary_suspension_license',
     'stop_construction','PENALTY','penalties_times','Repeated','YHPCZ_ZDYHS','YBYHS']
    cscol=[i for i in jgjcol if i not in ['YHPCZ_ZDYHS','YBYHS','PENALTY']]
    qcol=['YHPCZ_ZDYHS','YBYHS']
    fkcol=['PENALTY']
    for ft in jgjcol:
        if ft in cscol:
            Regulation_supervisi[ft]=Regulation_supervisi[ft].astype('int')
            Regulation_supervisi[ft]=Regulation_supervisi[ft].astype('str')
            Regulation_supervisi[ft]=Regulation_supervisi[ft]+'次'
        elif ft in qcol:
            Regulation_supervisi[ft]=Regulation_supervisi[ft].astype('int')
            Regulation_supervisi[ft]=Regulation_supervisi[ft].astype('str')
            Regulation_supervisi[ft]=Regulation_supervisi[ft]+'起'
        else:
            Regulation_supervisi[ft]=Regulation_supervisi[ft].round(2)
            Regulation_supervisi[ft]=Regulation_supervisi[ft].astype('str')
            Regulation_supervisi[ft]=Regulation_supervisi[ft]+'万元'
    #########################################基础情况##############################################
    #煤矿基本情况
    df26_28=V_AJ1_MJ_JCXX_BASEINFO[['CORP_ID','DESIGN_OUTPUT','PROVED_OUTPUT','MINE_MINESTYLE_NAME','PARENT_TYPE_NAME','MINE_STATUS_ZS_NAME','GRIME_EXPLOSIVE_NAME','ECONOMY_TYPE_NAME','XKZ_STATUS_NAME_ZS','STANDARD_CLASS_NAME']]
    #监控设备	MONITOR_STATUS 需要进一步明确编码含义，先用良好、一般、较差替换
    df29A = T_AJ1_MJ_SG_ACCIDENT_INFO[['ACCIDENT_MINE_ID','UPDATE_DATE']].groupby(['ACCIDENT_MINE_ID']).max().reset_index(drop=False)
    df29B = T_AJ1_MJ_SG_ACCIDENT_INFO[['ACCIDENT_MINE_ID','UPDATE_DATE','MONITOR_STATUS']]
    df29 = pd.merge(df29A,df29B,on=['ACCIDENT_MINE_ID','UPDATE_DATE'],how='inner')
    df29 = df29.drop(['UPDATE_DATE'],axis=1)
    df29 = df29.rename(columns={'ACCIDENT_MINE_ID':'CORP_ID'})
    df29['MONITOR_STATUS']=df29['MONITOR_STATUS'].astype('str')
    # 10    27  # 20     8  # 30     4
    df29['MONITOR_STATUS']=df29['MONITOR_STATUS'].apply(lambda x:'未建' if x=='10' else '在建' if x=='20' else '已建' if x=='30' else '未填报')
    # 产量
    df30A = T_AJ1_MJ_JCXX_CLXX[['CORP_ID','UPDATE_DATE']].groupby(['CORP_ID']).max().reset_index(drop=False)
    df30B = T_AJ1_MJ_JCXX_CLXX[['CORP_ID','UPDATE_DATE','NDCLX_CL']]
    df30 = pd.merge(df30A,df30B,on=['CORP_ID','UPDATE_DATE'],how='inner')
    df30 = df30.drop(['UPDATE_DATE'],axis=1)
    df30 = df30.drop_duplicates(['CORP_ID'])

    #井下安全避险系统信息	LDXTX_XTLX
    df31A = T_AJ1_MJ_JCXX_JXAQBXXTXX[['CORP_ID','UPDATE_DATE']].groupby(['CORP_ID']).max().reset_index(drop=False)
    df31B = T_AJ1_MJ_JCXX_JXAQBXXTXX[['CORP_ID','UPDATE_DATE','LDXTX_XTLX']]
    df31 = pd.merge(df31A,df31B,on=['CORP_ID','UPDATE_DATE'],how='inner')
    df31['LDXTX_XTLX']=df31['LDXTX_XTLX'].astype('str')
    df31 = df31.drop(['UPDATE_DATE'],axis=1)
    df31 = df31.drop_duplicates(['CORP_ID'])
    #中文转换
    def LDXTX_XTLX_T(x):
        if x=='10':
            return '监测监控系统'
        if x=='20':
            return '人员定位系统'
        if x=='30':
            return '通信联络系统'
        if x=='40':
            return '压风自救系统'
        if x=='50':
            return '供水施救系统'
        if x=='60':
            return '紧急避险系统'
        else:
            return '未填报'
    #调用函数转换中文
    df31['LDXTX_XTLX']=df31['LDXTX_XTLX'].apply(LDXTX_XTLX_T)

    # 回采率
    # df32_1A = T_AJ1_MJ_JCXX_CMGZMJD[['CORP_ID','REPORT_YEAR']].groupby(['CORP_ID']).max().reset_index(drop=False)
    # df32_1B = T_AJ1_MJ_JCXX_CMGZMJD[['CORP_ID','REPORT_YEAR','CMGZM_HCL']]
    # df32_1 = pd.merge(df32_1A,df32_1B,on=['CORP_ID','REPORT_YEAR'],how='inner')
    # df32_1 = df32_1.drop(['REPORT_YEAR'],axis=1)
    # df32_1 = df32_1.drop_duplicates(['CORP_ID'])
    df32_1=T_AJ1_MJ_JCXX_CMGZMJD[T_AJ1_MJ_JCXX_CMGZMJD['REPORT_YEAR']==str(datetime.datetime.now().year-1)].drop_duplicates(['CORP_ID'])
    df32_1=df32_1[['CORP_ID','CMGZM_HCL']]

    #T_AJ1_MJ_SG_ACCIDENT_INFO 
    ## ACCIDENT_MINE_ID   =  CORP_ID
    #瞒报	HIDDEN_CATEGORY
    df33A = T_AJ1_MJ_SG_ACCIDENT_INFO[['ACCIDENT_MINE_ID','UPDATE_DATE']].groupby(['ACCIDENT_MINE_ID']).max().reset_index(drop=False)
    df33B = T_AJ1_MJ_SG_ACCIDENT_INFO[['ACCIDENT_MINE_ID','UPDATE_DATE','HIDDEN_CATEGORY']]
    df33 = pd.merge(df33A,df33B,on=['ACCIDENT_MINE_ID','UPDATE_DATE'],how='inner')
    df33 = df33.drop(['UPDATE_DATE'],axis=1)
    df33 = df33.drop_duplicates(['ACCIDENT_MINE_ID'])
    df33 = df33.rename(columns={'ACCIDENT_MINE_ID':'CORP_ID'})
    df33['HIDDEN_CATEGORY']=df33['HIDDEN_CATEGORY'].astype('str')
    df33['HIDDEN_CATEGORY']=df33['HIDDEN_CATEGORY'].apply(lambda x:'瞒报' if x=='10' else '谎报' if x=='20' else '迟报' if x=='30' else '无')
    #df33['HIDDEN_CATEGORY']=df33['HIDDEN_CATEGORY'].apply(lambda x:'瞒报' if x==10 else '谎报' if x==20 else '迟报' if x==30 else '无')
    #证照是否齐全
    df34 = pd.DataFrame()
    df34['CORP_ID'] = total_frame['CORP_ID']
    df34['Full_certificates']='齐全'
    #基础情况合并
    Basic_information_1=pd.merge(total_frame,df26_28,on=['CORP_ID'],how='left')
    Basic_information_2=pd.merge(Basic_information_1,df29,on=['CORP_ID'],how='left')
    Basic_information_3=pd.merge(Basic_information_2,df30,on=['CORP_ID'],how='left')
    Basic_information_4=pd.merge(Basic_information_3,df31,on=['CORP_ID'],how='left')
    Basic_information_5=pd.merge(Basic_information_4,df32_1,on=['CORP_ID'],how='left')
    Basic_information_6=pd.merge(Basic_information_5,df33,on=['CORP_ID'],how='left')
    Basic_information=pd.merge(Basic_information_6,df34,on=['CORP_ID'],how='left')
    #缺失值处理
    Basic_information['MONITOR_STATUS']=Basic_information['MONITOR_STATUS'].fillna('未填报')
    Basic_information['HIDDEN_CATEGORY']=Basic_information['HIDDEN_CATEGORY'].fillna('无')

    #查看
    #Basic_information.head()
    #字段名称和顺序处理
    Basic_information['MINE_TYPE']=Basic_information['MINE_CLASS_NAME']
    Basic_information['MINE_MINETYPE']=Basic_information['MINE_MINETYPE_NAME']
    #重命名
    Basic_information=Basic_information.rename(columns={'MINE_MINESTYLE_NAME':'MINE_MINESTYLE','PARENT_TYPE_NAME':'PARENT_TYPE','MINE_STATUS_ZS_NAME':'MINE_STATUS_ZS','GRIME_EXPLOSIVE_NAME':'GRIME_EXPLOSIVE','ECONOMY_TYPE_NAME':'ECONOMIC_TYPE','XKZ_STATUS_NAME_ZS':'LICENSE_STATUS','STANDARD_CLASS_NAME':'STANDARD_CLASS'})
    #煤矿基本情况
    Basic_information_mine=['MINE_TYPE','DESIGN_OUTPUT','PROVED_OUTPUT','MINE_MINESTYLE','MONITOR_STATUS',
                            'PARENT_TYPE','MINE_STATUS_ZS','NDCLX_CL','LDXTX_XTLX',
                            'MINE_MINETYPE','CMGZM_HCL','GRIME_EXPLOSIVE','ECONOMIC_TYPE']
    #信用评价
    credit_rating=['HIDDEN_CATEGORY']
    #安全生产许可证情况
    Work_safety_license=['Full_certificates','LICENSE_STATUS']
    #安全生产标准化情况
    Standardization_production_safety=['STANDARD_CLASS']
    Basic_information_allcol=list(total_frame.columns)+Basic_information_mine+credit_rating+Work_safety_license+Standardization_production_safety
    Basic_information=Basic_information[Basic_information_allcol]
    #百分比处理
    hclcol=['CMGZM_HCL']
    for ft in hclcol:
        Basic_information[ft]=Basic_information[ft].apply(lambda x:99.75 if x>100 else x)
        Basic_information[ft]=Basic_information[ft].fillna(10000).round(2)
        Basic_information[ft]=Basic_information[ft].astype('str')
        Basic_information[ft]=Basic_information[ft]+'%'
        Basic_information[ft]=Basic_information[ft].apply(lambda x:'未填报' if x=='10000.0%' else x)
    #产能和产量处理
    clol=['DESIGN_OUTPUT','PROVED_OUTPUT','NDCLX_CL']
    for ft in clol:
        if ft in clol[:2]:
            Basic_information[ft]=Basic_information[ft].fillna(100000000).astype('int')
            Basic_information[ft]=Basic_information[ft].astype('str')
            Basic_information[ft]=Basic_information[ft]+'万吨/年'
            Basic_information[ft]=Basic_information[ft].apply(lambda x:'未填报' if x=='100000000万吨/年' else x)
        else:
            Basic_information[ft]=Basic_information[ft].fillna(100000000).round(2)
            Basic_information[ft]=Basic_information[ft].astype('str')
            Basic_information[ft]=Basic_information[ft]+'万吨'
            Basic_information[ft]=Basic_information[ft].apply(lambda x:'未填报' if x=='100000000.0万吨' else x)
    #最终缺失值处理
    Basic_information=Basic_information.fillna('未填报')
    ####################################合并原始指标宽表###########################
    oncol=['CORP_ID','CORP_NAME','MINE_MINETYPE_NAME','MINE_CLASS_NAME','PROVINCE_NAME']
    dfall_1=pd.merge(Personnel_situation,regulation_implementation,on=oncol,how='inner')
    dfall_2=pd.merge(dfall_1,Chance_equipment,on=oncol,how='inner')
    dfall_3=pd.merge(dfall_2,Disaster_situation,on=oncol,how='inner')
    dfall_4=pd.merge(dfall_3,Natural_geological_condition,on=oncol,how='inner')
    dfall_5=pd.merge(dfall_4,Historical_accident,on=oncol,how='inner')
    dfall_6=pd.merge(dfall_5,Regulation_supervisi,on=oncol,how='inner')
    dfall=pd.merge(dfall_6,Basic_information,on=oncol,how='inner')
    ###########################行转列之前的宽表###################################
    df=dfall.drop(columns=['MINE_MINETYPE_NAME','MINE_CLASS_NAME','PROVINCE_NAME'])
    #字段行转列
    ll=[]
    for ft in list(df.columns[2:]):
        dt=df[list(df.columns[:2])+[ft]]
        dt['INDEX_NAME']=ft
        newcol=['mine_id','mine_name']+['point_scorp','INDEX_NAME']
        dt.columns=newcol
        ncol=['mine_id','mine_name']+['INDEX_NAME','point_scorp']
        dt=dt[ncol]
        ll.append(dt)
    dfnew=pd.concat(ll,axis=0,join='inner')
    #读取码表
    indictor_config_all=indictor_config[['ID','POINT_CODE','POINT_NAME']]
    indictor_config_all.columns=['point_id','INDEX_NAME','POINT_NAME']
    #关联码表
    dfend=pd.merge(dfnew,indictor_config_all,on=['INDEX_NAME'],how='inner')
    #sx=[i for i in list(dfend['INDEX_NAME'].unique()) if i not in ['MINE_TYPE','MINE_MINETYPE']]
    #dfend=dfend[dfend['INDEX_NAME'].isin(sx)]
    #得到没有去掉百分号的结果
    dfends=dfend[['mine_id','mine_name','point_id','POINT_NAME','point_scorp']]
    #得到去掉去掉百分号的结果
#     def fun(x):
#         if isinstance(x, str):
#             if x.endswith("%"):
#                 x = x.replace("%", "")
#                 x = str(round(float(x) * 0.01,4))
#         return x
#     dfend1=dfend[dfend['point_scorp'].str.contains("%")]
#     dfend2=dfend[~dfend['point_scorp'].str.contains("%")]
#     dfend1['point_scorp']=dfend1['point_scorp'].apply(fun)
#     dfends=pd.concat([dfend1,dfend2],axis=0,join='inner')
    return dfends

#计算宽表数据
def compute_dfall(T_AJ1_MJ_JCXX_CYRYTJ,T_AJ1_MJ_JCXX_ZYJSRYTJ,T_AJ1_MJ_JCXX_AQGLRYTJ,T_AJ1_MJ_JCXX_AQGLRYPB,T_AJ1_MJ_JCXX_AQFYGL,T_AJ1_MJ_JCXX_WSZHFZGL,T_AJ1_MJ_JCXX_CMGZMJD,T_AJ1_MJ_JCXX_WSCFLYGL,T_AJ1_MJ_SG_ACCIDENT_INFO,T_AJ1_MJ_JCXX_ZCMC,SG_HURT_PEOPLE_COLL_INFO,jczf_paper,jczf_case,jczf_danger,T_AJ1_MJ_JCXX_YHPCXX,T_AJ1_MJ_JCXX_CLXX,T_AJ1_MJ_JCXX_JXAQBXXTXX,lclf,total_frame,indictor_config,V_AJ1_MJ_JCXX_BASEINFO):
    #############################################人员状况######################################################
    ##  T_AJ1_MJ_JCXX_CYRYTJ 总数 、中专以上人数、高中人数、初中人数、初中以下人数
    ##  T_AJ1_MJ_JCXX_ZYJSRYTJ  大专人数、本科及以上人数、高级技术人数、中级技术人数、初级技术人数、见习技术人数
    ## 以ID 和字段 report_year这两个字段作为分组主键，report_year是为了选取最近的时间
    df1A = T_AJ1_MJ_JCXX_CYRYTJ[['CORP_ID','REPORT_YEAR']].groupby(['CORP_ID']).max().reset_index(drop=False)
    df1A['REPORT_YEAR'] = df1A['REPORT_YEAR'].astype(float)
    df1B = T_AJ1_MJ_JCXX_CYRYTJ[['CORP_ID','REPORT_YEAR','CYRYT_ZS','CYRYT_ZZRS','CYRYT_GZ','CYRYT_CZ','CYRYT_CZYX']]
    df1B['REPORT_YEAR'] = df1B['REPORT_YEAR'].astype(float)
    df1 = pd.merge(df1A,df1B,on=['CORP_ID','REPORT_YEAR'],how='inner')
    df1 = df1.drop(['REPORT_YEAR'],axis=1)
    df1 = df1.drop_duplicates(['CORP_ID'])
    #T_AJ1_MJ_JCXX_CYRYTJ['REPORT_YEAR']=T_AJ1_MJ_JCXX_CYRYTJ['REPORT_YEAR'].fillna(0).astype('int').astype('str')
    #df1=T_AJ1_MJ_JCXX_CYRYTJ[T_AJ1_MJ_JCXX_CYRYTJ['REPORT_YEAR']==str(datetime.datetime.now().year-1)].drop(columns=['REPORT_YEAR','DEL_FLAG']).drop_duplicates(['CORP_ID'])

    ##  T_AJ1_MJ_JCXX_ZYJSRYTJ  大专人数、本科及以上人数、高级技术人数、中级技术人数、初级技术人数、见习技术人数
    df2A = T_AJ1_MJ_JCXX_ZYJSRYTJ[['CORP_ID','REPORT_YEAR']].groupby(['CORP_ID']).max().reset_index(drop=False)
    df2A['REPORT_YEAR'] = df2A['REPORT_YEAR'].astype(float)
    df2B = T_AJ1_MJ_JCXX_ZYJSRYTJ[['CORP_ID','REPORT_YEAR','ZYJSR_SZRS','ZYJSR_BKYS','A_ZYJSR_ZCGC','A_ZYJSR_ZCZJ','A_ZYJSR_ZCCJ','A_ZYJSR_ZCJX']]
    df2B['REPORT_YEAR'] = df2B['REPORT_YEAR'].astype(float)
    df2 = pd.merge(df2A,df2B,on=['CORP_ID','REPORT_YEAR'],how='inner')
    df2 = df2.drop(['REPORT_YEAR'],axis=1)
    df2 = df2.drop_duplicates(['CORP_ID'])
    #T_AJ1_MJ_JCXX_ZYJSRYTJ['REPORT_YEAR']=T_AJ1_MJ_JCXX_ZYJSRYTJ['REPORT_YEAR'].fillna(0).astype('int').astype('str')
    #df2=T_AJ1_MJ_JCXX_ZYJSRYTJ[T_AJ1_MJ_JCXX_ZYJSRYTJ['REPORT_YEAR']==str(datetime.datetime.now().year-1)].drop(columns=['REPORT_YEAR','DEL_FLAG']).drop_duplicates(['CORP_ID'])

    ## T_AJ1_MJ_JCXX_AQGLRYTJ 负责人数、负责人持证数（安全资格证）、特种作业人数、特种作业资格证人数、安全生产管理人员数、安全生产管理人员持证数（管理人员安全资格证）
    # df3A = T_AJ1_MJ_JCXX_AQGLRYTJ[['CORP_ID','REPORT_YEAR']].groupby(['CORP_ID']).max().reset_index(drop=False)
    # df3B = T_AJ1_MJ_JCXX_AQGLRYTJ[['CORP_ID','REPORT_YEAR','AQGLR_ZYFZRS','AQGLR_ZYFZR','AQGLR_TZZY','AQGLR_CZS','AQGLR_AQSC','AQGLR_AQSCGL']]
    # df3 = pd.merge(df3A,df3B,on=['CORP_ID','REPORT_YEAR'],how='inner')
    # df3 = df3.drop(['REPORT_YEAR'],axis=1)
    # df3 = df3.drop_duplicates(['CORP_ID'])
    df3=T_AJ1_MJ_JCXX_AQGLRYTJ[T_AJ1_MJ_JCXX_AQGLRYTJ['REPORT_YEAR']==str(datetime.datetime.now().year-1)].drop_duplicates(['CORP_ID'])
    df3=df3[['CORP_ID','AQGLR_ZYFZRS','AQGLR_ZYFZR','AQGLR_TZZY','AQGLR_CZS','AQGLR_AQSC','AQGLR_AQSCGL']]

    ## T_AJ1_MJ_JCXX_AQGLRYTJ 负责人培训数、安全生产管理人员培训数、特种作业人员培训数、
    # df4A = T_AJ1_MJ_JCXX_AQGLRYTJ[['CORP_ID','REPORT_YEAR']].groupby(['CORP_ID']).max().reset_index(drop=False)
    # df4B = T_AJ1_MJ_JCXX_AQGLRYTJ[['CORP_ID','REPORT_YEAR','AQGLR_FZRPX','AQGLR_GLPX','AQGLR_TZPX']]
    # df4 = pd.merge(df4A,df4B,on=['CORP_ID','REPORT_YEAR'],how='inner')
    # df4 = df4.drop(['REPORT_YEAR'],axis=1)
    # df4 = df4.drop_duplicates(['CORP_ID'])
    df4=T_AJ1_MJ_JCXX_AQGLRYTJ[T_AJ1_MJ_JCXX_AQGLRYTJ['REPORT_YEAR']==str(datetime.datetime.now().year-1)].drop_duplicates(['CORP_ID'])
    df4=df4[['CORP_ID','AQGLR_FZRPX','AQGLR_GLPX','AQGLR_TZPX']]

    #T_AJ1_MJ_JCXX_AQGLRYPB 添加培训考核情况 A_PXKHQK
    df5A = T_AJ1_MJ_JCXX_AQGLRYPB[['CORP_ID','UPDATE_DATE']].groupby(['CORP_ID']).max().reset_index(drop=False)
    df5B = T_AJ1_MJ_JCXX_AQGLRYPB[['CORP_ID','UPDATE_DATE','A_PXKHQK']]
    df5 = pd.merge(df5A,df5B,on=['CORP_ID','UPDATE_DATE'],how='inner')
    df5 = df5.drop(['UPDATE_DATE'],axis=1)
    df5 = df5.drop_duplicates(['CORP_ID'])

    #生成2020人员总数
    zs=T_AJ1_MJ_JCXX_CYRYTJ[T_AJ1_MJ_JCXX_CYRYTJ['REPORT_YEAR'].fillna(0).astype('int').astype('str')==str(datetime.datetime.now().year-1)].drop_duplicates(['CORP_ID'])
    zs=zs[['CORP_ID','CYRYT_ZS']]
    zs.columns=['CORP_ID','CYRYT_ZS2020']

    #合并人员状况宽表基础字段
    people_1=pd.merge(total_frame, df1,on=['CORP_ID'],how='left' )
    people_2=pd.merge(people_1, df2,on=['CORP_ID'],how='left' )
    people_3=pd.merge(people_2, df3,on=['CORP_ID'],how='left' )
    people_4=pd.merge(people_3, df4,on=['CORP_ID'],how='left' )
    Personnel_situation=pd.merge(people_4,df5,on=['CORP_ID'],how='left' )
    Personnel_situation=pd.merge(Personnel_situation,zs,on=['CORP_ID'],how='left' )

    #计算占比
    #先填充离散变量缺失值，由于缺失值没参加培训，默认合格
    Personnel_situation['A_PXKHQK']=Personnel_situation['A_PXKHQK'].fillna('未填报')
    Personnel_situation['A_PXKHQK']=Personnel_situation['A_PXKHQK'].apply(lambda x:'合格' if x!='未填报' else x)
    #本科及以上人数占比
    Personnel_situation['BZ_BKJYS_CM_rate']=Personnel_situation['ZYJSR_BKYS']/Personnel_situation['CYRYT_ZS']

    #大专人数占比
    Personnel_situation['BZ_DZ_CM_rate']=Personnel_situation['ZYJSR_SZRS']/Personnel_situation['CYRYT_ZS']

    #初中人数总数占比
    Personnel_situation['CYRYT_CZ_rate']=Personnel_situation['CYRYT_CZ']/Personnel_situation['CYRYT_ZS']

    #初中以下人数占比数占比
    Personnel_situation['CYRYT_CZYX_rate']=Personnel_situation['CYRYT_CZYX']/Personnel_situation['CYRYT_ZS']

    #高级技术人数占比
    Personnel_situation['A_ZYJSR_ZCGC_rate']=Personnel_situation['A_ZYJSR_ZCGC']/Personnel_situation['CYRYT_ZS']

    #中级技术人数占比
    Personnel_situation['A_ZYJSR_ZCZJ_rate']=Personnel_situation['A_ZYJSR_ZCZJ']/Personnel_situation['CYRYT_ZS']

    #初级技术人数占比
    Personnel_situation['A_ZYJSR_ZCCJ_rate']=Personnel_situation['A_ZYJSR_ZCCJ']/Personnel_situation['CYRYT_ZS']

    #见习技术人数占比
    Personnel_situation['A_ZYJSR_ZCJX_rate']=Personnel_situation['A_ZYJSR_ZCJX']/Personnel_situation['CYRYT_ZS']

    #企业负责人持证数（安全资格证）占比
    Personnel_situation['AQGLR_ZYFZR_rate']=Personnel_situation['AQGLR_ZYFZR']/Personnel_situation['CYRYT_ZS2020']

    #安全生产管理人员数占比
    Personnel_situation['AQGLR_AQSC_rate']=Personnel_situation['AQGLR_AQSC']/Personnel_situation['CYRYT_ZS2020']

    #安全生产管理人员持证数（管理人员安全资格证）占比
    Personnel_situation['AQGLR_AQSCGL_rate']=Personnel_situation['AQGLR_AQSCGL']/Personnel_situation['CYRYT_ZS2020']

    #特种作业资格证人数占比
    Personnel_situation['AQGLR_CZS_rate']=Personnel_situation['AQGLR_CZS']/Personnel_situation['CYRYT_ZS2020']

    #特种作业人员培训数占比
    Personnel_situation['AQGLR_TZPX_rate']=Personnel_situation['AQGLR_TZPX']/Personnel_situation['CYRYT_ZS2020']

    #字段整理
    #人员基本情况
    Basic_Information_of_personnel=['BZ_BKJYS_CM_rate','BZ_DZ_CM_rate','CYRYT_CZ_rate','CYRYT_CZYX_rate','A_ZYJSR_ZCGC_rate','A_ZYJSR_ZCZJ_rate','A_ZYJSR_ZCCJ_rate','A_ZYJSR_ZCJX_rate']
    #资格证
    certification=['AQGLR_ZYFZR_rate','AQGLR_AQSC_rate','AQGLR_AQSCGL_rate','AQGLR_CZS_rate']
    #参加培训/培训考核
    attend_training=['AQGLR_TZPX_rate']
    training_check=['A_PXKHQK']
    #最终字段
    Personnel_allcol=list(total_frame.columns)+Basic_Information_of_personnel+certification+attend_training+training_check
    #根据字段名和排序生成数据
    Personnel_situation_zj=Personnel_situation[['CORP_ID','CYRYT_ZS']]
    Personnel_situation=Personnel_situation[Personnel_allcol]
    Personnel_situation.replace([np.inf, -np.inf], np.nan, inplace=True)
    ##################异常值处理##################
    #异常值处理，过滤掉部分大于1的字段
    lxcol=['BZ_BKJYS_CM_rate','BZ_DZ_CM_rate','CYRYT_CZ_rate','CYRYT_CZYX_rate','A_ZYJSR_ZCGC_rate','A_ZYJSR_ZCZJ_rate','A_ZYJSR_ZCCJ_rate','A_ZYJSR_ZCJX_rate','AQGLR_ZYFZR_rate','AQGLR_AQSC_rate','AQGLR_AQSCGL_rate','AQGLR_CZS_rate','AQGLR_TZPX_rate']
    for ft in lxcol:
        Personnel_situation[ft]=Personnel_situation[ft].apply(lambda x:0.5 if x>1 else x)
    Personnel_situation=Personnel_situation.round(4)
    #############################################安全管理规制及执行############################################
    #管理制度
    #对没接入的默认赋值
    df6=total_frame[['CORP_ID']]
    df6['Reasonable_organization_structure']='暂未接入数据'
    df6['Sound_organizational_structure']='暂未接入数据'
    df6['Sound_safety_management']='暂未接入数据'
    df6['Sound_Discipline_establishment']='暂未接入数据'
    df6['Plan_to_perfect']='暂未接入数据'
    df6['Plan_reasonable']='暂未接入数据'
    #安全经费投入
    ## T_AJ1_MJ_JCXX_AQFYGL 提取金额、吨煤提取金额、使用金额、结余金额
    # df7A = T_AJ1_MJ_JCXX_AQFYGL[['CORP_ID','REPORT_YEAR']].groupby(['CORP_ID']).max().reset_index(drop=False)
    # df7B = T_AJ1_MJ_JCXX_AQFYGL[['CORP_ID','REPORT_YEAR','AQFYT_TQJE','AQFYT_PJDM','AQFYT_SYYE','A_AQFYT_JYJE']]
    # df7 = pd.merge(df7A,df7B,on=['CORP_ID','REPORT_YEAR'],how='inner')
    # df7 = df7.drop(['REPORT_YEAR'],axis=1)
    # df7 = df7.drop_duplicates(['CORP_ID'])
    df7=T_AJ1_MJ_JCXX_AQFYGL[T_AJ1_MJ_JCXX_AQFYGL['REPORT_YEAR']==str(datetime.datetime.now().year-1)].drop_duplicates(['CORP_ID'])
    df7=df7[['CORP_ID','AQFYT_TQJE','AQFYT_PJDM','AQFYT_SYYE','A_AQFYT_JYJE']]

    #安全制度执行
    ## T_AJ1_MJ_JCXX_WSZHFZGL “一通三防”管理人员数量、防突施工人员数量
    ## 这个表中，没有report_year 这个字段，所以选择update_date 的最新时间来分组
    df8A = T_AJ1_MJ_JCXX_WSZHFZGL[['CORP_ID','UPDATE_DATE']].groupby(['CORP_ID']).max().reset_index(drop=False)
    df8B = T_AJ1_MJ_JCXX_WSZHFZGL[['CORP_ID','UPDATE_DATE','GASGEN_TENICHNUM','GASGEB_BURSTWORKERNUM']]
    df8_1 = pd.merge(df8A,df8B,on=['CORP_ID','UPDATE_DATE'],how='inner')
    df8_1 = df8_1.drop(['UPDATE_DATE'],axis=1)
    df8_1 = df8_1.drop_duplicates(['CORP_ID'])

    # df8_1=T_AJ1_MJ_JCXX_WSZHFZGL[T_AJ1_MJ_JCXX_WSZHFZGL['UPDATE_DATE'].astype('str').str[:4]==str(datetime.datetime.now().year-1)].drop_duplicates(['CORP_ID'])
    # df8_1=df8_1[['CORP_ID','GASGEN_TENICHNUM','GASGEB_BURSTWORKERNUM']]

    #关联人数计算占比
    #以2020人员为总数
    # zs=T_AJ1_MJ_JCXX_CYRYTJ[T_AJ1_MJ_JCXX_CYRYTJ['REPORT_YEAR'].fillna(0).astype('int').astype('str')==str(datetime.datetime.now().year-1)].drop_duplicates(['CORP_ID'])
    # zs=zs[['CORP_ID','CYRYT_ZS']]

    df8=pd.merge(Personnel_situation_zj,df8_1,on=['CORP_ID'],how='left')
    #“一通三防”管理人员数量占比
    df8['GASGEN_TENICHNUM_rate']=df8['GASGEN_TENICHNUM']/df8['CYRYT_ZS']
    #防突施工人员数量占比
    df8['GASGEB_BURSTWORKERNUM_rate']=df8['GASGEB_BURSTWORKERNUM']/df8['CYRYT_ZS']
    df8=df8[['CORP_ID','GASGEN_TENICHNUM_rate','GASGEB_BURSTWORKERNUM_rate']]
    df8.replace([np.inf, -np.inf], np.nan, inplace=True)

    #矿井文化建设
    #默认赋值
    df9=total_frame[['CORP_ID']]
    df9['Number_of_propaganda']='暂未接入数据'
    #合并安全管理规制及执行字段
    regulation_implementation_6=pd.merge(total_frame,df6,on=['CORP_ID'],how='left')
    regulation_implementation_7=pd.merge(regulation_implementation_6,df7,on=['CORP_ID'],how='left')
    regulation_implementation_8=pd.merge(regulation_implementation_7,df8,on=['CORP_ID'],how='left')
    regulation_implementation=pd.merge(regulation_implementation_8,df9,on=['CORP_ID'],how='left')

    #字段重命名
    regulation_implementation=regulation_implementation.rename(columns={'AQFYT_TQJE':'FXDYJ_CCJE','AQFYT_PJDM':'A_FXDYJ_DMTQJE','AQFYT_SYYE':'FXDYJ_SYJE','A_AQFYT_JYJE':'A_FXDYJ_JYJE'})
    #列名校验
    regulation_bcol=['Reasonable_organization_structure',
    'Sound_organizational_structure',
    'Sound_safety_management',
    'Sound_Discipline_establishment',
    'Plan_to_perfect',
    'Plan_reasonable',
    'FXDYJ_CCJE',
    'A_FXDYJ_DMTQJE',
    'FXDYJ_SYJE',
    'A_FXDYJ_JYJE',
    'GASGEN_TENICHNUM_rate',
    'GASGEB_BURSTWORKERNUM_rate',
    'Number_of_propaganda']
    #所有列
    regulation_allcol=list(total_frame.columns)+regulation_bcol
    regulation_implementation=regulation_implementation[regulation_allcol]
    ######异常值处理
    regulation_implementation['GASGEN_TENICHNUM_rate']=regulation_implementation['GASGEN_TENICHNUM_rate'].apply(lambda x:0.5 if x>1 else x)
    regulation_implementation['GASGEB_BURSTWORKERNUM_rate']=regulation_implementation['GASGEB_BURSTWORKERNUM_rate'].apply(lambda x:0.5 if x>1 else x)
    regulation_implementation=regulation_implementation.round(4)
    #############################################机运设备##################################################
    #机电设备
    ## T_AJ1_MJ_JCXX_CMGZMJD  机采率、炮采个数、普采个数、综采个数
    # df10A = T_AJ1_MJ_JCXX_CMGZMJD[['CORP_ID','REPORT_YEAR']].groupby(['CORP_ID']).max().reset_index(drop=False)
    # df10B = T_AJ1_MJ_JCXX_CMGZMJD[['CORP_ID','REPORT_YEAR','CMGZM_JCL','CMGZM_PCGS','CMGZM_PC','CMGZM_ZCGS']]
    # df10 = pd.merge(df10A,df10B,on=['CORP_ID','REPORT_YEAR'],how='inner')
    # df10 = df10.drop(['REPORT_YEAR'],axis=1)
    # df10 = df10.drop_duplicates(['CORP_ID'])
    df10=T_AJ1_MJ_JCXX_CMGZMJD[T_AJ1_MJ_JCXX_CMGZMJD['REPORT_YEAR']==str(datetime.datetime.now().year-1)].drop_duplicates(['CORP_ID'])
    df10=df10[['CORP_ID','CMGZM_JCL','CMGZM_PCGS','CMGZM_PC','CMGZM_ZCGS']]

    ##通风方式
    df10_1=V_AJ1_MJ_JCXX_BASEINFO[['CORP_ID','MINE_VENTILATESTYLE_NAME']]

    ##自动化开采个数
    # df10_2A = T_AJ1_MJ_JCXX_CMGZMJD[['CORP_ID','REPORT_YEAR']].groupby(['CORP_ID']).max().reset_index(drop=False)
    # df10_2B = T_AJ1_MJ_JCXX_CMGZMJD[['CORP_ID','REPORT_YEAR','CMGZM_ZDHGS']]
    # df10_2 = pd.merge(df10_2A,df10_2B,on=['CORP_ID','REPORT_YEAR'],how='inner')
    # df10_2 = df10_2.drop(['REPORT_YEAR'],axis=1)
    # df10_2 = df10_2.drop_duplicates(['CORP_ID'])
    df10_2=T_AJ1_MJ_JCXX_CMGZMJD[T_AJ1_MJ_JCXX_CMGZMJD['REPORT_YEAR']==str(datetime.datetime.now().year-1)].drop_duplicates(['CORP_ID'])
    df10_2=df10_2[['CORP_ID','CMGZM_ZDHGS']]

    ##默认赋值
    df11=total_frame[['CORP_ID']]
    df11['Equip_equipment_as_required']='暂未接入数据'
    df11['equipment_selection_reasonable']='暂未接入数据'
    df11['equipment_maintenance']='暂未接入数据'
    df11['equipment_protection']='暂未接入数据'
    df11['Full_warning_signs']='暂未接入数据'
    df11['Complete_protective_facilities']='暂未接入数据'

    # 运输设备
    ##运输方式
    df12=V_AJ1_MJ_JCXX_BASEINFO[['CORP_ID','MINE_TRANSMITSTYLE_NAME']]

    #合并机运设备数据
    Chance_equipment_10=pd.merge(total_frame,df10,on=['CORP_ID'],how='left')
    Chance_equipment_10_1=pd.merge(Chance_equipment_10,df10_1,on=['CORP_ID'],how='left')
    Chance_equipment_10_2=pd.merge(Chance_equipment_10_1,df10_2,on=['CORP_ID'],how='left')
    Chance_equipment_11=pd.merge(Chance_equipment_10_2,df11,on=['CORP_ID'],how='left')
    Chance_equipment=pd.merge(Chance_equipment_11,df12,on=['CORP_ID'],how='left')
    #重命名
    Chance_equipment=Chance_equipment.rename(columns={'MINE_VENTILATESTYLE_NAME':'MINE_VENTILATESTYLE','MINE_TRANSMITSTYLE_NAME':'MINE_TRANSMITSTYLE'})
    #列名校验
    Chance_equipment_allcol=list(total_frame.columns)+['CMGZM_JCL','CMGZM_PCGS','CMGZM_PC','CMGZM_ZCGS','MINE_VENTILATESTYLE','CMGZM_ZDHGS','Equip_equipment_as_required','equipment_selection_reasonable','equipment_maintenance','equipment_protection','Full_warning_signs','Complete_protective_facilities','MINE_TRANSMITSTYLE']
    #百分比处理
    jclcol=['CMGZM_JCL']
    for ft in jclcol:
        Chance_equipment[ft]=Chance_equipment[ft].apply(lambda x:x/100 if x>100 else x)
    #生成
    Chance_equipment=Chance_equipment[Chance_equipment_allcol]
    Chance_equipment=Chance_equipment.round(4)
    #############################################灾害状况##################################################
    #瓦斯爆炸和突出状况
    ## T_AJ1_MJ_JCXX_WSCFLYGL  最大瓦斯抽放浓度、最小瓦斯抽放浓度、平均瓦斯抽放浓度、设计瓦斯抽放浓度、最大瓦斯抽放负压、最小瓦斯抽放负压、平均瓦斯抽放负压
    T_AJ1_MJ_JCXX_WSCFLYGL=T_AJ1_MJ_JCXX_WSCFLYGL[T_AJ1_MJ_JCXX_WSCFLYGL['REPORT_YEAR'].notnull()]
    T_AJ1_MJ_JCXX_WSCFLYGL['REPORT_YEAR'] = T_AJ1_MJ_JCXX_WSCFLYGL['REPORT_YEAR'].astype('int64')
    T_AJ1_MJ_JCXX_WSCFLYGL=T_AJ1_MJ_JCXX_WSCFLYGL[T_AJ1_MJ_JCXX_WSCFLYGL['REPORT_YEAR']>0]
    df13A = T_AJ1_MJ_JCXX_WSCFLYGL[['CORP_ID','REPORT_YEAR']].groupby(['CORP_ID']).max().reset_index(drop=False)
    df13B = T_AJ1_MJ_JCXX_WSCFLYGL[['CORP_ID','REPORT_YEAR','GASUTIL_CONMAX','GASUTIL_CONMIN','GASUTIL_CONAVG','GASUTIL_PREDESIGN','GASUTIL_PREMAX','GASUTIL_PREMIN','GASUTIL_PREAVG']]
    df13A['REPORT_YEAR'] = df13A['REPORT_YEAR'].astype('str')
    df13B['REPORT_YEAR'] = df13B['REPORT_YEAR'].astype('str')
    df13 = pd.merge(df13A,df13B,on=['CORP_ID','REPORT_YEAR'],how='inner')
    df13 = df13.drop(['REPORT_YEAR'],axis=1)
    df13 = df13.drop_duplicates(['CORP_ID'])
    #T_AJ1_MJ_JCXX_WSCFLYGL['REPORT_YEAR']=T_AJ1_MJ_JCXX_WSCFLYGL['REPORT_YEAR'].fillna(0).astype('int').astype('str')
    # df13=T_AJ1_MJ_JCXX_WSCFLYGL[T_AJ1_MJ_JCXX_WSCFLYGL['REPORT_YEAR']==str(datetime.datetime.now().year-1)].drop_duplicates(['CORP_ID'])
    # df13=df13[['CORP_ID','GASUTIL_CONMAX','GASUTIL_CONMIN','GASUTIL_CONAVG','GASUTIL_PREDESIGN','GASUTIL_PREMAX','GASUTIL_PREMIN','GASUTIL_PREAVG']]


    ## 瓦斯等级	MINE_WS_GRADE
    df14=V_AJ1_MJ_JCXX_BASEINFO[['CORP_ID','MINE_WS_GRADE_NAME']]
    #冲击地压状况
    # T_AJ1_MJ_JCXX_BASEINFO 是否有冲击地压
    df15=V_AJ1_MJ_JCXX_BASEINFO[['CORP_ID','ROCKBURST_NAME']]
    #水文地址状况
    ## 水文地质类型
    df16=V_AJ1_MJ_JCXX_BASEINFO[['CORP_ID','HYDROGEOLOGICAL_TYPE_NAME']]
    ##平均涌水量、最大涌水量
    df17=V_AJ1_MJ_JCXX_BASEINFO[['CORP_ID','MINE_WATERBURST','MINE_WATERBURST_MAX']]
    #默认赋值-顶板管理
    df18=total_frame[['CORP_ID']]
    df18['roof_caving']='暂未接入数据'
    #煤层自然倾向性
    df19=V_AJ1_MJ_JCXX_BASEINFO[['CORP_ID','MINE_FIRE_NAME']]

    #合并灾害状况
    Disaster_situation_13=pd.merge(total_frame,df13,on=['CORP_ID'],how='left')
    Disaster_situation_14=pd.merge(Disaster_situation_13,df14,on=['CORP_ID'],how='left')
    Disaster_situation_15=pd.merge(Disaster_situation_14,df15,on=['CORP_ID'],how='left')
    Disaster_situation_16=pd.merge(Disaster_situation_15,df16,on=['CORP_ID'],how='left')
    Disaster_situation_17=pd.merge(Disaster_situation_16,df17,on=['CORP_ID'],how='left')
    Disaster_situation_18=pd.merge(Disaster_situation_17,df18,on=['CORP_ID'],how='left')
    Disaster_situation=pd.merge(Disaster_situation_18,df19,on=['CORP_ID'],how='left')
    #重命名
    dicol={'MINE_WS_GRADE_NAME':'MINE_WS_GRADE',
           'ROCKBURST_NAME':'ROCKBURST',
           'HYDROGEOLOGICAL_TYPE_NAME':'HYDROGEOLOGICAL_TYPE',
           'MINE_WATERBURST':'MINE_WATERBURST',
           'MINE_WATERBURST_MAX':'MINE_WATERBURST_MAX',
           'MINE_FIRE_NAME':'MINE_FIRE',
           }

    Disaster_situation=Disaster_situation.rename(columns=dicol)

    #瓦斯爆炸和突出状况
    Gas_explosion_outburst=['GASUTIL_CONMAX','GASUTIL_CONMIN','GASUTIL_CONAVG','GASUTIL_PREDESIGN',
                            'GASUTIL_PREMAX','GASUTIL_PREMIN','GASUTIL_PREAVG','MINE_WS_GRADE']

    #冲击地压状况
    Impact_pressure_condition=['ROCKBURST']
    #水文地质状况
    Hydrogeological_condition=['HYDROGEOLOGICAL_TYPE','MINE_WATERBURST','MINE_WATERBURST_MAX']
    #顶板管理
    roof_control=['roof_caving']
    #自燃倾向状况
    Spontaneous_combustion_tendency=['MINE_FIRE']
    #最终列名
    Spontaneous_combustion_allcol=list(total_frame.columns)+Gas_explosion_outburst+Impact_pressure_condition+Hydrogeological_condition+roof_control+Spontaneous_combustion_tendency
    #生成结果
    Disaster_situation=Disaster_situation[Spontaneous_combustion_allcol]
    #Disaster_situation=Disaster_situation.round(4)
    Disaster_situation['MINE_WS_GRADE']=Disaster_situation['MINE_WS_GRADE'].fillna('无需填报')
    #瓦斯抽放
    cfcol=['GASUTIL_CONMAX','GASUTIL_CONMIN','GASUTIL_CONAVG','GASUTIL_PREDESIGN','GASUTIL_PREMAX','GASUTIL_PREMIN','GASUTIL_PREAVG']
    #Disaster_situation.loc[Disaster_situation['MINE_WS_GRADE']=='低瓦斯',cfcol]='无需填报'
    Disaster_situation.loc[Disaster_situation['MINE_WS_GRADE']=='低瓦斯',cfcol[:4]]=1
    Disaster_situation.loc[Disaster_situation['MINE_WS_GRADE']=='低瓦斯','GASUTIL_PREMAX']=Disaster_situation['GASUTIL_PREMAX'].max()
    Disaster_situation.loc[Disaster_situation['MINE_WS_GRADE']=='低瓦斯','GASUTIL_PREMIN']=Disaster_situation['GASUTIL_PREMIN'].max()
    Disaster_situation.loc[Disaster_situation['MINE_WS_GRADE']=='低瓦斯','GASUTIL_PREAVG']=Disaster_situation['GASUTIL_PREAVG'].max()
    #百分比
    for ft in cfcol[:4]:
        Disaster_situation[ft]=Disaster_situation[ft].apply(lambda x:1 if abs(x)>100 or x<0 else x)/100
    Disaster_situation=Disaster_situation.round(4)

    ########################################自然地质条件#####################################
    #周边地质条件
    ## T_AJ1_MJ_JCXX_ZCMC 煤层平均厚度、煤层最大厚度、上覆煤层间距、下伏煤层间距、煤层平均倾角
    df20A = T_AJ1_MJ_JCXX_ZCMC[['CORP_ID','UPDATE_DATE']].groupby(['CORP_ID']).max().reset_index(drop=False)
    df20B = T_AJ1_MJ_JCXX_ZCMC[['CORP_ID','UPDATE_DATE','MINE_THICK_AVG','MINE_THICK_MAX','A_SFMCJJ','A_XFMCJJ','A_MCPJQJ']]
    df20 = pd.merge(df20A,df20B,on=['CORP_ID','UPDATE_DATE'],how='inner')
    df20 = df20.drop(['UPDATE_DATE'],axis=1)
    df20 = df20.drop_duplicates(['CORP_ID'])

    # df20=T_AJ1_MJ_JCXX_ZCMC[T_AJ1_MJ_JCXX_ZCMC['UPDATE_DATE'].astype('str').str[:4]==str(datetime.datetime.now().year-1)].drop_duplicates(['CORP_ID'])
    # df20=df20[['CORP_ID','MINE_THICK_AVG','MINE_THICK_MAX','A_SFMCJJ','A_XFMCJJ','A_MCPJQJ']]


    #默认赋值-气候
    df21=total_frame[['CORP_ID']]
    df21['mean_annual_precipitation']='暂未接入数据'
    df21['Average_hot_days']='暂未接入数据'
    df21['Mean_wind_speed']='暂未接入数据'
    #默认赋值-自然灾害
    df22=total_frame[['CORP_ID']]
    df22['Annual_landslide']='暂未接入数据'
    #合并自然地质条件数据
    Natural_geological_condition_20=pd.merge(total_frame,df20,on=['CORP_ID'],how='left')
    Natural_geological_condition_21=pd.merge(Natural_geological_condition_20,df21,on=['CORP_ID'],how='left')
    Natural_geological_condition=pd.merge(Natural_geological_condition_21,df22,on=['CORP_ID'],how='left')
    #调整字段名称和顺序
    Natural_geological_col=list(total_frame.columns)+['MINE_THICK_AVG','MINE_THICK_MAX','A_SFMCJJ','A_XFMCJJ',
                                                      'A_MCPJQJ','mean_annual_precipitation','Average_hot_days','Mean_wind_speed','Annual_landslide']
    Natural_geological_condition=Natural_geological_condition[Natural_geological_col]
    Natural_geological_condition=Natural_geological_condition.round(4)

    ##############################################历史事故#########################################
    #T_AJ1_MJ_SG_ACCIDENT_INFO 
    ##  ACCIDENT_MINE_ID   =  CORP_ID
    # 事故等级	ACCIDENT_LEVEL
    # 事故类别 ====   ACCIDENT_CATEGORY
    ##  HURT_PEOPLE_COLL_ID ===== 事故受伤人员统计记录编号
    df23A = T_AJ1_MJ_SG_ACCIDENT_INFO[['ACCIDENT_MINE_ID','UPDATE_DATE']].groupby(['ACCIDENT_MINE_ID']).max().reset_index(drop=False)
    df23B = T_AJ1_MJ_SG_ACCIDENT_INFO[['ACCIDENT_MINE_ID','UPDATE_DATE','ACCIDENT_LEVEL','ACCIDENT_CATEGORY','HURT_PEOPLE_COLL_ID']]
    df23 = pd.merge(df23A,df23B,on=['ACCIDENT_MINE_ID','UPDATE_DATE'],how='inner')
    df23 = df23.drop(['UPDATE_DATE'],axis=1)
    df23 = df23.drop_duplicates(['ACCIDENT_MINE_ID'])
    df23 = df23.rename(columns={'ACCIDENT_MINE_ID':'CORP_ID'})

    #SG_HURT_PEOPLE_COLL_INFO 事故死亡人数、事故重伤人数、事故轻伤人数
    df24 = pd.DataFrame()
    df24['HURT_PEOPLE_COLL_ID'] = SG_HURT_PEOPLE_COLL_INFO['ID']
    df24['DIE_PEOPLE_NUM'] = SG_HURT_PEOPLE_COLL_INFO['DIE_PEOPLE_NUM']
    df24['WEIGHT_HURT_PEOPLE_NUM'] = SG_HURT_PEOPLE_COLL_INFO['WEIGHT_HURT_PEOPLE_NUM']
    df24['LIGHT_HURT_PEOPLE_NUM'] = SG_HURT_PEOPLE_COLL_INFO['LIGHT_HURT_PEOPLE_NUM']
    #
    df24_1 = df24.groupby(['HURT_PEOPLE_COLL_ID'])['DIE_PEOPLE_NUM'].sum().to_frame().reset_index(drop=False)
    df24_2 = df24.groupby(['HURT_PEOPLE_COLL_ID'])['WEIGHT_HURT_PEOPLE_NUM'].sum().to_frame().reset_index(drop=False)
    df24_3 = df24.groupby(['HURT_PEOPLE_COLL_ID'])['LIGHT_HURT_PEOPLE_NUM'].sum().to_frame().reset_index(drop=False)

    df23 = pd.merge(df23, df24_1,on=['HURT_PEOPLE_COLL_ID'],how='left' )
    df23 = pd.merge(df23, df24_2,on=['HURT_PEOPLE_COLL_ID'],how='left' )
    df23 = pd.merge(df23, df24_3,on=['HURT_PEOPLE_COLL_ID'],how='left' )
    df23 = df23.drop(['HURT_PEOPLE_COLL_ID'],axis =1)
    df23['ACCIDENT_LEVEL']=df23['ACCIDENT_LEVEL'].astype('int').astype('str')
    df23['ACCIDENT_CATEGORY']=df23['ACCIDENT_CATEGORY'].astype('int').astype('str')
    #事故等级中文
    df23['ACCIDENT_LEVEL']=df23['ACCIDENT_LEVEL'].apply(lambda x:'一般事故' if x=='10' else '较大事故' if x=='20' else '重大事故' if x=='30' else '特别重大事故' if x=='40' else '无事故')
    #事故类别中文
    #事故类别中文转换函数
    def ACCIDENT_CATEGORY_T(x):
        if x=='10':
            return '顶板'
        if x=='15':
            return '煤尘'    
        if x=='20':
            return '瓦斯'
        if x=='25':
            return '冲击地压 '
        if x=='30':
            return '水害'    
        if x=='40':
            return '运输'
        if x=='50':
            return '机电'
        if x=='60':
            return '爆破'    
        if x=='70':
            return '火灾'
        if x=='90':
            return '其他'
        if x=='1010':
            return '冒顶'    
        if x=='1020':
            return '片帮'
        if x=='1030':
            return '顶板掉矸'
        if x=='1040':
            return '顶板支护垮倒'    
        if x=='2510':
            return '冲击地压'
        if x=='1060':
            return '露天煤矿边坡滑移垮塌'
        if x=='2010':
            return '瓦斯爆炸'    
        if x=='1510':
            return '煤尘爆炸'
        if x=='2030':
            return '煤与瓦斯（突出）'
        if x=='2051':
            return '二氧化碳（突出）'    
        if x=='2040':
            return '中毒窒息'
        if x=='4010':
            return '坠罐'
        if x=='4020':
            return '跑车'    
        if x=='4030':
            return '其他'
        if x=='7010':
            return '井下火灾-内因'
        if x=='7020':
            return '井下火灾-外因'    
        if x=='7030':
            return '地面火灾'
        if x=='9010':
            return '捅溜煤眼'
        if x=='9020':
            return '火药储运过程中的爆炸事故'    
        if x=='9030':
            return '其他'
        if x=='9040':
            return '坠落'
        else:
            return '无事故'
    #调用函数转换事故类别
    df23['ACCIDENT_CATEGORY']=df23['ACCIDENT_CATEGORY'].apply(ACCIDENT_CATEGORY_T)
    #历史事故合并数据:注意事故等级和类型缺失应该是无事故、无类型的情况！
    Historical_accident=pd.merge(total_frame,df23,on=['CORP_ID'],how='left')
    Historical_accident=Historical_accident[list(total_frame.columns)+['ACCIDENT_CATEGORY','DIE_PEOPLE_NUM','ACCIDENT_LEVEL','WEIGHT_HURT_PEOPLE_NUM','LIGHT_HURT_PEOPLE_NUM']]
    Historical_accident[['ACCIDENT_CATEGORY','ACCIDENT_LEVEL']]=Historical_accident[['ACCIDENT_CATEGORY','ACCIDENT_LEVEL']].fillna('无事故')
    Historical_accident=Historical_accident.fillna(0)
    sgcol=['DIE_PEOPLE_NUM','WEIGHT_HURT_PEOPLE_NUM','LIGHT_HURT_PEOPLE_NUM']
    for ft in sgcol:
        Historical_accident[ft]=Historical_accident[ft].astype('int')
    #Historical_accident=Historical_accident.round(4)

    ############################################监管监察#########################################
    jczf_paper_copy = jczf_paper.copy()
    # inner 三张表，选取所需的字段
    jczf_paper_copy['PAPER_TYPE']=jczf_paper_copy['PAPER_TYPE'].astype('str')
    jczf_paper_copy = jczf_paper_copy[jczf_paper_copy['PAPER_TYPE']=='1']
    jczf_paper_1 = pd.DataFrame()
    jczf_paper_1['CASE_ID'] = jczf_paper_copy['CASE_ID']
    jczf_paper_1['PAPER_ID'] = jczf_paper_copy['PAPER_ID']
    jczf_paper_1['CORP_ID'] = jczf_paper_copy['CORP_ID']

    jczf_case_1 = pd.DataFrame()
    jczf_case_1['CASE_ID'] = jczf_case['CASE_ID']

    jczf_danger_1 = pd.DataFrame()
    jczf_danger_1['PAPER_ID'] = jczf_danger['PAPER_ID']
    jczf_danger_1['ONSITE_TYPE'] = jczf_danger['ONSITE_TYPE']
    jczf_danger_1['PENALTY_TYPE'] = jczf_danger['PENALTY_TYPE']
    jczf_danger_1['IS_HIGH'] = jczf_danger['IS_HIGH']
    jczf_danger_1['COALING_FACE'] = jczf_danger['COALING_FACE']
    jczf_danger_1['HEADING_FACE'] = jczf_danger['HEADING_FACE']

    # inner三张表，去重，删除了不需要的字段
    t_frame = pd.merge(jczf_paper_1, jczf_case_1,on=['CASE_ID'],how='inner')
    t_frame = pd.merge(t_frame, jczf_danger_1,on=['PAPER_ID'],how='inner')
    t_frame = t_frame.drop('CASE_ID',axis= 1)

    # 数据预处理，填补，去矿井中的空值
    t_frame['PENALTY_TYPE'] = t_frame['PENALTY_TYPE'].fillna(0)
    t_frame['ONSITE_TYPE'] = t_frame['ONSITE_TYPE'].fillna(0)
    t_frame['COALING_FACE'] = t_frame['COALING_FACE'].fillna(0)
    t_frame['HEADING_FACE'] = t_frame['HEADING_FACE'].fillna(0)
    t_frame = t_frame.dropna()

    #现场处理 
    ONSITE_TYPE=pd.DataFrame()
    ONSITE_TYPE=t_frame[['PAPER_ID','CORP_ID','ONSITE_TYPE']]
    # ONSITE_TYPE['CORP_ID'] = t_frame['CORP_ID']
    # ONSITE_TYPE['ONSITE_TYPE'] = t_frame['ONSITE_TYPE']

    # 停止采掘工作面 = COALING_FACE（采煤工作面）+ HEADING_FACE（掘进工作面）
    # Stop_mining_face['CORP_ID'] = Stop_mining_face['CORP_ID'].astype('int64')
    df_spface = pd.DataFrame()
    df_spface['PAPER_ID'] = t_frame['PAPER_ID']
    df_spface['CORP_ID'] = t_frame['CORP_ID']
    df_spface['Stop_mining_face'] = t_frame['COALING_FACE'].astype(float)+t_frame['HEADING_FACE'].astype(float)
    Stop_mining_face = df_spface.groupby('CORP_ID')['PAPER_ID'].nunique().reset_index(drop=False)
    Stop_mining_face = Stop_mining_face.rename(columns={'PAPER_ID':'Stop_mining_face'})
    #类型转换
    ONSITE_TYPE['ONSITE_TYPE'] = ONSITE_TYPE['ONSITE_TYPE'].astype(float)

    # 责令立即停止生产  编码 8
    # stop_production['CORP_ID'] = stop_production['CORP_ID'].astype('int64')
    df_8 = ONSITE_TYPE[ONSITE_TYPE['ONSITE_TYPE']==8.0]
    stop_production = df_8.groupby('CORP_ID')['PAPER_ID'].nunique().reset_index(drop=False)
    stop_production = stop_production.rename(columns={'PAPER_ID':'stop_production'})

    # 责令立即停止作业  编码 7
    # stop_work['CORP_ID'] = stop_work['CORP_ID'].astype('int64')
    df_7 = ONSITE_TYPE[ONSITE_TYPE['ONSITE_TYPE']==7.0]
    stop_work = df_7.groupby('CORP_ID')['PAPER_ID'].nunique().reset_index(drop=False)
    stop_work = stop_work.rename(columns={'PAPER_ID':'stop_work'})

    # 停止使用相关设备  编码 6
    # Stop_using_equipment['CORP_ID'] = Stop_using_equipment['CORP_ID'].astype('int64')
    df_6 = ONSITE_TYPE[ONSITE_TYPE['ONSITE_TYPE']==6.0]
    Stop_using_equipment = df_7.groupby('CORP_ID')['PAPER_ID'].nunique().reset_index(drop=False)
    Stop_using_equipment = Stop_using_equipment.rename(columns={'PAPER_ID':'Stop_using_equipment'})

    # 从危险区域撤出作业人员  编码 12
    # Evacuation['CORP_ID'] = Evacuation['CORP_ID'].astype('int64')
    df_12 = ONSITE_TYPE[ONSITE_TYPE['ONSITE_TYPE']==12.0]
    Evacuation = df_12.groupby('CORP_ID')['PAPER_ID'].nunique().reset_index(drop=False)
    Evacuation = Evacuation.rename(columns={'PAPER_ID':'Evacuation'})

    #行政处罚 
    PENALTY_TYPE = pd.DataFrame()
    PENALTY_TYPE = t_frame[['PAPER_ID','CORP_ID','PENALTY_TYPE']]
    #PENALTY_TYPE['CORP_ID'] = t_frame['CORP_ID']
    #PENALTY_TYPE['PENALTY_TYPE'] = t_frame['PENALTY_TYPE']
    PENALTY_TYPE['PENALTY_TYPE'] = PENALTY_TYPE['PENALTY_TYPE'].astype(float)

    # 责令停产整顿编码 3
    # rectification['CORP_ID'] = rectification['CORP_ID'].astype('int64')
    df_3 = ONSITE_TYPE[ONSITE_TYPE['ONSITE_TYPE']==3]
    rectification = df_3.groupby('CORP_ID')['PAPER_ID'].nunique().reset_index(drop=False)
    rectification = rectification.rename(columns={'PAPER_ID':'rectification'})

    # 暂扣安全生产许可证  编码 7
    # Temporary_suspension_license['CORP_ID'] = Temporary_suspension_license['CORP_ID'].astype('int64')
    df_7_1 = ONSITE_TYPE[ONSITE_TYPE['ONSITE_TYPE']==7]
    Temporary_suspension_license = df_7_1.groupby('CORP_ID')['PAPER_ID'].nunique().reset_index(drop=False)
    Temporary_suspension_license = Temporary_suspension_license.rename(columns={'PAPER_ID':'Temporary_suspension_license'})

    # 责令停止建设  编码 5
    # stop_construction['CORP_ID'] = stop_construction['CORP_ID'].astype('int64')
    df_5 = ONSITE_TYPE[ONSITE_TYPE['ONSITE_TYPE']==5]
    stop_construction = df_5.groupby('CORP_ID')['PAPER_ID'].nunique().reset_index(drop=False)
    stop_construction = stop_construction.rename(columns={'PAPER_ID':'stop_construction'})

    # 处罚金额
    P8_PENALTY = pd.DataFrame()
    P8_PENALTY = jczf_paper[['CORP_ID','NAME','P8_PENALTY']]
    # P8_PENALTY['NAME'] = jczf_paper['NAME']
    # P8_PENALTY['P8_PENALTY'] = jczf_paper['P8_PENALTY']

    P8_PENALTY['P8_PENALTY'] = P8_PENALTY['P8_PENALTY'].fillna(0)
    P8_PENALTY['NAME'] = P8_PENALTY['NAME'].fillna(0)
    P8_PENALTY = P8_PENALTY.dropna()
    P8_PENALTY['P8_PENALTY'] = P8_PENALTY['P8_PENALTY'].astype(float)

    PENALTY = P8_PENALTY[P8_PENALTY['NAME']=='行政处罚决定书'].groupby('CORP_ID')['P8_PENALTY'].sum().reset_index(drop=False).round(0)
    PENALTY = PENALTY.rename(columns={'P8_PENALTY':'PENALTY'})
    PENALTY['PENALTY'] = PENALTY['PENALTY'].astype(float)


    #处罚次数
    Number_of_penalties = pd.DataFrame()
    Number_of_penalties = jczf_paper[['CORP_ID','PAPER_ID','NAME']]
    # Number_of_penalties['PAPER_ID'] = jczf_paper['PAPER_ID']
    # Number_of_penalties['NAME'] = jczf_paper['NAME']

    penalties_times = Number_of_penalties[Number_of_penalties['NAME']=='行政处罚决定书'].groupby('CORP_ID')['PAPER_ID'].nunique().reset_index(drop=False)
    penalties_times = penalties_times.rename(columns={'PAPER_ID':'penalties_times'})
    penalties_times['penalties_times'] = penalties_times['penalties_times'].astype(float)

    #屡查屡犯
    lclf_1 = pd.DataFrame()
    lclf_1 = lclf[['CORP_ID','lclf']]
    #lclf_1['lclf'] = lclf['lclf']
    lclf_1 = lclf_1.dropna()
    lclf_1 = lclf_1.drop_duplicates()

    Repeated = lclf_1[lclf_1['lclf']!='需专家进一步鉴定'].groupby('CORP_ID')['lclf'].nunique().reset_index(drop=False)
    Repeated = Repeated.rename(columns={'lclf':'Repeated'})
    Repeated['Repeated'] = Repeated['Repeated'].astype(float)


    ####20200525计算重大隐患和一般隐患更新
    jczf_paper['DEL_FLAG']=jczf_paper['DEL_FLAG'].astype('str')
    jczf_paper['PAPER_TYPE']=jczf_paper['PAPER_TYPE'].astype('str')
    jczf_danger['DEL_FLAG']=jczf_danger['DEL_FLAG'].astype('str')
    jczf_paper0=jczf_paper[(jczf_paper['DEL_FLAG']=='0')&(jczf_paper['PAPER_TYPE']=='1')]
    jczf_paper0=jczf_paper0.drop(columns=['DEL_FLAG'])
    jczf_danger0=jczf_danger[(jczf_danger['DEL_FLAG']=='0')]
    yh=pd.merge(jczf_paper0,jczf_danger0,on=['PAPER_ID'],how='inner')
    #计算重大隐患数
    df25=yh[(yh['IS_HIGH']=='1')].groupby(['CORP_ID'])[['CORP_ID']].size().reset_index()
    df25.columns=['CORP_ID','YHPCZ_ZDYHS']
    YBYHS=yh[(yh['IS_HIGH']=='0')].groupby(['CORP_ID'])[['CORP_ID']].size().reset_index()
    YBYHS.columns=['CORP_ID','YBYHS']

    # T_AJ1_MJ_JCXX_YHPCXX 重大隐患数
    #df25 = T_AJ1_MJ_JCXX_YHPCXX.groupby(['CORP_ID'])['YHPCZ_ZDYHS'].sum().to_frame().reset_index(drop=False)

    ##一般隐患 IS_HIGH jczf_danger
    IS_HIGH = pd.DataFrame()
    IS_HIGH = t_frame[['CORP_ID','PAPER_ID','IS_HIGH']]
    # IS_HIGH['PAPER_ID'] = t_frame['PAPER_ID']
    # IS_HIGH['IS_HIGH'] = t_frame['IS_HIGH']

    #YBYHS = IS_HIGH[IS_HIGH['IS_HIGH']==0].groupby('CORP_ID')['PAPER_ID'].nunique().reset_index(drop=False)
    #YBYHS = YBYHS.rename(columns={'PAPER_ID':'YBYHS'})
    #YBYHS['YBYHS'] = YBYHS['YBYHS'].astype(float)

    #合并监管监察数据
    ##现场处理
    # stop_production	责令立即停止生产次数
    # stop_work	责令立即停止作业次数
    # Stop_mining_face	停止采掘工作面次数
    # Stop_using_equipment	停止使用相关设备次数
    # Evacuation	从危险区域撤出作业人员次数
    Regulation_supervisi_1=pd.merge(total_frame,stop_production,on=['CORP_ID'],how='left')
    Regulation_supervisi_2=pd.merge(Regulation_supervisi_1,stop_work,on=['CORP_ID'],how='left')
    Regulation_supervisi_3=pd.merge(Regulation_supervisi_2,Stop_mining_face,on=['CORP_ID'],how='left')
    Regulation_supervisi_4=pd.merge(Regulation_supervisi_3,Stop_using_equipment,on=['CORP_ID'],how='left')
    Regulation_supervisi_5=pd.merge(Regulation_supervisi_4,Evacuation,on=['CORP_ID'],how='left')

    ##行政处罚
    # rectification	责令停产整顿次数
    # Temporary_suspension_license	暂扣安全生产许可证次数
    # stop_construction	责令停止建设次数
    # PENALTY	总处罚金额
    # penalties_times	处罚次数
    # Repeated	屡查屡犯
    Regulation_supervisi_6=pd.merge(Regulation_supervisi_5,rectification,on=['CORP_ID'],how='left')
    Regulation_supervisi_7=pd.merge(Regulation_supervisi_6,Temporary_suspension_license,on=['CORP_ID'],how='left')
    Regulation_supervisi_8=pd.merge(Regulation_supervisi_7,stop_construction,on=['CORP_ID'],how='left')
    Regulation_supervisi_9=pd.merge(Regulation_supervisi_8,PENALTY,on=['CORP_ID'],how='left')
    Regulation_supervisi_10=pd.merge(Regulation_supervisi_9,penalties_times,on=['CORP_ID'],how='left')
    Regulation_supervisi_11=pd.merge(Regulation_supervisi_10,Repeated,on=['CORP_ID'],how='left')

    #历史隐患发生情况
    # YHPCZ_ZDYHS	重大隐患数
    # YBYHS	一般隐患数
    Regulation_supervisi_12=pd.merge(Regulation_supervisi_11,df25,on=['CORP_ID'],how='left')
    Regulation_supervisi=pd.merge(Regulation_supervisi_12,YBYHS,on=['CORP_ID'],how='left')
    #字段名和顺序都没问题
    Regulation_supervisi=Regulation_supervisi.fillna(0)
    jgjcol=['stop_production','stop_work','Stop_mining_face','Stop_using_equipment','Evacuation','rectification','Temporary_suspension_license',
     'stop_construction','PENALTY','penalties_times','Repeated','YHPCZ_ZDYHS','YBYHS']
    cscol=[i for i in jgjcol if i not in ['YHPCZ_ZDYHS','YBYHS','PENALTY']]
    qcol=['YHPCZ_ZDYHS','YBYHS']
    fkcol=['PENALTY']
    for ft in jgjcol:
        if ft in cscol+qcol:
            Regulation_supervisi[ft]=Regulation_supervisi[ft].astype('int')
    #保留4位小数
    #Regulation_supervisi=Regulation_supervisi.round(4)

    #########################################基础情况##############################################
    #煤矿基本情况
    df26_28=V_AJ1_MJ_JCXX_BASEINFO[['CORP_ID','DESIGN_OUTPUT','PROVED_OUTPUT','MINE_MINESTYLE_NAME','PARENT_TYPE_NAME','MINE_STATUS_ZS_NAME','GRIME_EXPLOSIVE_NAME','ECONOMY_TYPE_NAME','XKZ_STATUS_NAME_ZS','STANDARD_CLASS_NAME']]
    #监控设备	MONITOR_STATUS 需要进一步明确编码含义，先用良好、一般、较差替换
    df29A = T_AJ1_MJ_SG_ACCIDENT_INFO[['ACCIDENT_MINE_ID','UPDATE_DATE']].groupby(['ACCIDENT_MINE_ID']).max().reset_index(drop=False)
    df29B = T_AJ1_MJ_SG_ACCIDENT_INFO[['ACCIDENT_MINE_ID','UPDATE_DATE','MONITOR_STATUS']]
    df29 = pd.merge(df29A,df29B,on=['ACCIDENT_MINE_ID','UPDATE_DATE'],how='inner')
    df29 = df29.drop(['UPDATE_DATE'],axis=1)
    df29 = df29.rename(columns={'ACCIDENT_MINE_ID':'CORP_ID'})
    df29['MONITOR_STATUS']=df29['MONITOR_STATUS'].astype('str')
    # 10    27  # 20     8  # 30     4
    df29['MONITOR_STATUS']=df29['MONITOR_STATUS'].apply(lambda x:'未建' if x=='10' else '在建' if x=='20' else '已建' if x=='30' else '未填报')
    # 产量
    df30A = T_AJ1_MJ_JCXX_CLXX[['CORP_ID','UPDATE_DATE']].groupby(['CORP_ID']).max().reset_index(drop=False)
    df30B = T_AJ1_MJ_JCXX_CLXX[['CORP_ID','UPDATE_DATE','NDCLX_CL']]
    df30 = pd.merge(df30A,df30B,on=['CORP_ID','UPDATE_DATE'],how='inner')
    df30 = df30.drop(['UPDATE_DATE'],axis=1)
    df30 = df30.drop_duplicates(['CORP_ID'])

    #井下安全避险系统信息	LDXTX_XTLX
    df31A = T_AJ1_MJ_JCXX_JXAQBXXTXX[['CORP_ID','UPDATE_DATE']].groupby(['CORP_ID']).max().reset_index(drop=False)
    df31B = T_AJ1_MJ_JCXX_JXAQBXXTXX[['CORP_ID','UPDATE_DATE','LDXTX_XTLX']]
    df31 = pd.merge(df31A,df31B,on=['CORP_ID','UPDATE_DATE'],how='inner')
    df31['LDXTX_XTLX']=df31['LDXTX_XTLX'].astype('str')
    df31 = df31.drop(['UPDATE_DATE'],axis=1)
    df31 = df31.drop_duplicates(['CORP_ID'])
    #中文转换
    def LDXTX_XTLX_T(x):
        if x=='10':
            return '监测监控系统'
        if x=='20':
            return '人员定位系统'
        if x=='30':
            return '通信联络系统'
        if x=='40':
            return '压风自救系统'
        if x=='50':
            return '供水施救系统'
        if x=='60':
            return '紧急避险系统'
        else:
            return '未填报'
    #调用函数转换中文
    df31['LDXTX_XTLX']=df31['LDXTX_XTLX'].apply(LDXTX_XTLX_T)

    # 回采率
    # df32_1A = T_AJ1_MJ_JCXX_CMGZMJD[['CORP_ID','REPORT_YEAR']].groupby(['CORP_ID']).max().reset_index(drop=False)
    # df32_1B = T_AJ1_MJ_JCXX_CMGZMJD[['CORP_ID','REPORT_YEAR','CMGZM_HCL']]
    # df32_1 = pd.merge(df32_1A,df32_1B,on=['CORP_ID','REPORT_YEAR'],how='inner')
    # df32_1 = df32_1.drop(['REPORT_YEAR'],axis=1)
    # df32_1 = df32_1.drop_duplicates(['CORP_ID'])
    df32_1=T_AJ1_MJ_JCXX_CMGZMJD[T_AJ1_MJ_JCXX_CMGZMJD['REPORT_YEAR']==str(datetime.datetime.now().year-1)].drop_duplicates(['CORP_ID'])
    df32_1=df32_1[['CORP_ID','CMGZM_HCL']]

    #T_AJ1_MJ_SG_ACCIDENT_INFO 
    ## ACCIDENT_MINE_ID   =  CORP_ID
    #瞒报	HIDDEN_CATEGORY
    df33A = T_AJ1_MJ_SG_ACCIDENT_INFO[['ACCIDENT_MINE_ID','UPDATE_DATE']].groupby(['ACCIDENT_MINE_ID']).max().reset_index(drop=False)
    df33B = T_AJ1_MJ_SG_ACCIDENT_INFO[['ACCIDENT_MINE_ID','UPDATE_DATE','HIDDEN_CATEGORY']]
    df33 = pd.merge(df33A,df33B,on=['ACCIDENT_MINE_ID','UPDATE_DATE'],how='inner')
    df33 = df33.drop(['UPDATE_DATE'],axis=1)
    df33 = df33.drop_duplicates(['ACCIDENT_MINE_ID'])
    df33 = df33.rename(columns={'ACCIDENT_MINE_ID':'CORP_ID'})
    df33['HIDDEN_CATEGORY']=df33['HIDDEN_CATEGORY'].astype('str')
    df33['HIDDEN_CATEGORY']=df33['HIDDEN_CATEGORY'].apply(lambda x:'瞒报' if x=='10' else '谎报' if x=='20' else '迟报' if x=='30' else '无')
    #df33['HIDDEN_CATEGORY']=df33['HIDDEN_CATEGORY'].apply(lambda x:'瞒报' if x==10 else '谎报' if x==20 else '迟报' if x==30 else '无')
    #证照是否齐全
    df34 = pd.DataFrame()
    df34['CORP_ID'] = total_frame['CORP_ID']
    df34['Full_certificates']='齐全'
    #基础情况合并
    Basic_information_1=pd.merge(total_frame,df26_28,on=['CORP_ID'],how='left')
    Basic_information_2=pd.merge(Basic_information_1,df29,on=['CORP_ID'],how='left')
    Basic_information_3=pd.merge(Basic_information_2,df30,on=['CORP_ID'],how='left')
    Basic_information_4=pd.merge(Basic_information_3,df31,on=['CORP_ID'],how='left')
    Basic_information_5=pd.merge(Basic_information_4,df32_1,on=['CORP_ID'],how='left')
    Basic_information_6=pd.merge(Basic_information_5,df33,on=['CORP_ID'],how='left')
    Basic_information=pd.merge(Basic_information_6,df34,on=['CORP_ID'],how='left')
    #缺失值处理
    Basic_information['MONITOR_STATUS']=Basic_information['MONITOR_STATUS'].fillna('未填报')
    Basic_information['HIDDEN_CATEGORY']=Basic_information['HIDDEN_CATEGORY'].fillna('无')

    #查看
    #Basic_information.head()
    #字段名称和顺序处理
    Basic_information['MINE_TYPE']=Basic_information['MINE_CLASS_NAME']
    Basic_information['MINE_MINETYPE']=Basic_information['MINE_MINETYPE_NAME']
    #重命名
    Basic_information=Basic_information.rename(columns={'MINE_MINESTYLE_NAME':'MINE_MINESTYLE','PARENT_TYPE_NAME':'PARENT_TYPE','MINE_STATUS_ZS_NAME':'MINE_STATUS_ZS','GRIME_EXPLOSIVE_NAME':'GRIME_EXPLOSIVE','ECONOMY_TYPE_NAME':'ECONOMIC_TYPE','XKZ_STATUS_NAME_ZS':'LICENSE_STATUS','STANDARD_CLASS_NAME':'STANDARD_CLASS'})
    #煤矿基本情况
    Basic_information_mine=['MINE_TYPE','DESIGN_OUTPUT','PROVED_OUTPUT','MINE_MINESTYLE','MONITOR_STATUS',
                            'PARENT_TYPE','MINE_STATUS_ZS','NDCLX_CL','LDXTX_XTLX',
                            'MINE_MINETYPE','CMGZM_HCL','GRIME_EXPLOSIVE','ECONOMIC_TYPE']
    #信用评价
    credit_rating=['HIDDEN_CATEGORY']
    #安全生产许可证情况
    Work_safety_license=['Full_certificates','LICENSE_STATUS']
    #安全生产标准化情况
    Standardization_production_safety=['STANDARD_CLASS']
    Basic_information_allcol=list(total_frame.columns)+Basic_information_mine+credit_rating+Work_safety_license+Standardization_production_safety
    Basic_information=Basic_information[Basic_information_allcol]
    #异常值处理
    Basic_information['NDCLX_CL']= Basic_information['NDCLX_CL'].replace(470006.00,47)
    Basic_information=Basic_information.round(4)
    #处理百分号
    Basic_information['CMGZM_HCL']=Basic_information['CMGZM_HCL']/100
    Basic_information['CMGZM_HCL']=Basic_information['CMGZM_HCL'].apply(lambda x:0.9975 if x>1 else x)
    ####################################合并原始指标宽表###########################
    oncol=['CORP_ID','CORP_NAME','MINE_MINETYPE_NAME','MINE_CLASS_NAME','PROVINCE_NAME']
    dfall_1=pd.merge(Personnel_situation,regulation_implementation,on=oncol,how='inner')
    dfall_2=pd.merge(dfall_1,Chance_equipment,on=oncol,how='inner')
    dfall_3=pd.merge(dfall_2,Disaster_situation,on=oncol,how='inner')
    dfall_4=pd.merge(dfall_3,Natural_geological_condition,on=oncol,how='inner')
    dfall_5=pd.merge(dfall_4,Historical_accident,on=oncol,how='inner')
    dfall_6=pd.merge(dfall_5,Regulation_supervisi,on=oncol,how='inner')
    dfall=pd.merge(dfall_6,Basic_information,on=oncol,how='inner')
    return dfall


#煤矿类型函数
def mine_type(df):
    df1=df[df['MINE_MINETYPE_NAME']=='井工']
    df1['MINE_CLASS_NAME']=df1['OUTPUT'].apply(lambda x:'小型矿井' if x<=30 else '中型矿井' if x>30 and x<120 else '大型矿井')
    df2=df[df['MINE_MINETYPE_NAME']=='露天']
    df2['MINE_CLASS_NAME']=df2['OUTPUT'].apply(lambda x:'小型矿井' if x<=100 else '中型矿井' if x>100 and x<400 else '大型矿井')
    dall=pd.concat([df1,df2],axis=0,join='inner')
    return dall

#生成基础数据行转列
def generate_base_data_all(dfall):
    ll=[]
    for ft in list(dfall.iloc[:,5:].columns):
        data=dfall[list(dfall.iloc[:,:5].columns)+[ft]]
        data['INDEX_NAME']=ft
        data.columns=list(dfall.iloc[:,:5].columns)+['INDEX_VALUE','INDEX_NAME']
        data=data[list(dfall.iloc[:,:5].columns)+['INDEX_NAME','INDEX_VALUE']]
        ll.append(data)
    base_data_all=pd.concat(ll,axis=0,join='inner')
    print(base_data_all.shape)
    return base_data_all



















