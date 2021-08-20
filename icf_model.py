# -*i- coding: utf-8 -*-
import pandas as pd
import numpy as np
import random
from sklearn.tree import DecisionTreeClassifier
from itertools import product
import pymysql
import sqlalchemy
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
#SparkContext.setSystemProperty('spark.driver.maxResultSize', '16g')
#from pyspark.sql.functions import *
from pyspark.sql.types import *
sc= SparkContext.getOrCreate()
spark=SparkSession(sc)



#离散型指标取值-得分对应字典
table_obj_cols = {
    'A_PXKHQK': {'合格': 100, '未填报': 0},
    'MINE_VENTILATESTYLE': {'中央并列抽出': 85, '中央分列抽出': 85, '对角抽出': 85, '其他': 60, '中央并列压入': 75, '中央分列压入': 75, '对角压入': 75},
    'MINE_TRANSMITSTYLE': {'皮带运输': 95, '其他': 35, '电机车牵引': 100, '调度绞车': 55, '人力手推矿车': 25, '人力绞车': 25},
    'MINE_WS_GRADE': {'高瓦斯': 50, '低瓦斯': 100, '突出': 40, '按突出煤层管理': 100, '未鉴定': 10, '露天煤矿无需填写': 100, '无需填报': 100},
    'ROCKBURST': {'冲击地压矿井->弱': 80, '无冲击地压矿井->无冲击地压矿井': 100, '无冲击地压矿井': 100, '无冲击地压矿井->其中煤岩有冲击倾向性': 50, '冲击地压矿井->强': 10,
                  '冲击地压矿井->中等': 60},
    'HYDROGEOLOGICAL_TYPE': {'复杂': 40, '中等': 70, '极复杂': 20, '简单': 100, '未检测': 0},
    'MINE_FIRE': {'自燃': 10, '容易自燃': 30, '不易自燃': 80, '未检测': 0},
    'ACCIDENT_CATEGORY': {'冲击地压': 45, '机电': 40, '其他': 50, '无事故': 100, '水害': 30, '冒顶': 40, '片帮': 40, '捅溜煤眼': 40,
                          '中毒窒息': 0, '坠落': 30, '跑车': 40, '顶板': 40, '瓦斯爆炸': 0, '地面火灾': 35, '顶板支护垮倒': 40, '顶板掉矸': 40,
                          '井下火灾-外因': 30, '煤与瓦斯（突出）': 30, '运输': 40, '煤尘爆炸': 10, '爆破': 20, '露天煤矿边坡滑移垮塌': 30, '火灾': 0,
                          '井下火灾-内因': 0},
    'ACCIDENT_LEVEL': {'较大事故': 20, '一般事故': 60, '无事故': 100, '重大事故': 0},
    'MINE_TYPE': {'大型矿井': 100, '中型矿井': 100, '小型矿井': 100},
    'MINE_MINESTYLE': {'立井': 100, '斜井': 90, '立井斜井混合': 90, '平硐斜井混合': 90, '平硐': 90, '片盘斜井混合': 90, '其他': 80, '公路运输': 90,
                       '固定坑线': 90, '未填报': 40},
    'MONITOR_STATUS': {'未填报': 50, '未建': 0, '已建': 100, '在建': 60},
    'PARENT_TYPE': {'国有重点': 100, '地方国有': 80, '乡镇煤矿': 60, '未填报': 50},
    'MINE_STATUS_ZS': {'生产矿井->正常生产矿井': 100, '建设矿井->改建矿井->停建矿井->长期停建矿井': 0, '建设矿井->扩建矿井->停建矿井->长期停建矿井': 0,
                       '建设矿井->扩建矿井->停建矿井->长期停建无法联系矿井': 0, '建设矿井->新建矿井->停建矿井->自行停建': 0,
                       '建设矿井->扩建矿井->正常建设矿井': 100, '建设矿井->新建矿井->正常建设矿井': 100, '建设矿井->新建矿井->停建矿井->长期停建矿井': 0,
                       '建设矿井->改建矿井->正常建设矿井': 100, '建设矿井->新建矿井->停建矿井->长期停建无法联系矿井': 0, '生产矿井->停产矿井->长期停产矿井': 0,
                       '生产矿井->停产矿井->停产整改矿井': 0, '生产矿井->停产矿井->自行停产矿井': 0, '建设矿井->改建矿井->停建矿井->长期停建无法联系矿井': 0,
                       '建设矿井->新建矿井->拟建矿井': 80, '建设矿井->新建矿井->停建矿井->停建整改': 0, '关闭矿井->正在实施关闭矿井': 40,
                       '建设矿井->扩建矿井->停建矿井->自行停建': 0, '建设矿井->改建矿井->停建矿井->自行停建': 0, '建设矿井->扩建矿井->拟建矿井': 80,
                       '建设矿井->扩建矿井->停建矿井->停建整改': 0, '建设矿井->改建矿井->停建矿井->停建整改': 0, '建设矿井->改建矿井->拟建矿井': 80},
    'LDXTX_XTLX': {'监测监控系统': 100, '紧急避险系统': 90, '人员定位系统': 80, '通信联络系统': 80, '供水施救系统': 80, '压风自救系统': 80, '未填报': 0},
    'MINE_MINETYPE': {'井工': 100, '露天': 100},
    'GRIME_EXPLOSIVE': {'有爆炸性': 40, '无爆炸性': 100, '未检测': 20},
    'ECONOMIC_TYPE': {'国有经济': 100, '有限责任公司(自然人投资或控股)': 80, '私有经济': 80, '非公有经济': 80, '有限责任公司（非自然投资和控股的法人独资）': 80,
                      '股份制': 80, '集体经济': 80, '其他经济': 80, '外商经济': 80, '公有经济': 90, '港澳台经济': 80, '集体分支机构(非法人)': 70,
                      '联营经济': 70, '未填报': 50},
    'HIDDEN_CATEGORY': {'无': 100, '迟报': 50, '瞒报': 0, '谎报': 0},
    'Full_certificates': {'齐全': 100},
    'LICENSE_STATUS': {'持证（正常）': 100, '未办证': 0, '持证（暂扣）': 50, '正常注销': 70, '吊销': 0},
    'STANDARD_CLASS': {'三级': 80, '一级': 100, '二级': 90, '未评定': 0, '未达标': 30, '撤销等级': 0},
}

#根据标签产品处理生成离散型指标取值
#spark连接mysql  coal_mine_supervision配置信息
url='jdbc:mysql://10.18.33.227:3306/coal_mine_supervision'
user='root'
password='cdsf@119'
driver="com.mysql.cj.jdbc.Driver"
query = "(select POINT_CODE,VALUE,SCORE from label_score where VALUE!='暂未接入数据') emp"
df=spark.read.format("jdbc").option("url",url).option("dbtable",query).option("user", user).option("password", password).option("driver",driver).load().toPandas()
df['SCORE']=df['SCORE'].astype('int')
df.loc[df['VALUE']=='有限责任公司（非自然投资和控股的法人独资）','VALUE']='有限责任公司（非自然人投资和控股的法人独资）'
#新增离散变量取值类型的处理
df1=pd.DataFrame([['ECONOMIC_TYPE','其他有限责任公司',75],
                  ['ECONOMIC_TYPE','有限责任公司分公司',70],
                  ['ECONOMIC_TYPE','一人有限责任公司',65]])
df1.columns=['POINT_CODE','VALUE','SCORE']
df1['SCORE']=df1['SCORE'].astype('int')
df=df.append(df1)

#定义转换函数
def calALL(df_all,GROUP):
    ll_orign = dict()
    for name, df_g in df_all.groupby(GROUP):
        ll_orign[name]=df_g.iloc[:,1:].set_index('VALUE')['SCORE'].to_dict()
    return ll_orign
table_obj_cols=calALL(df,['POINT_CODE'])

# 连续型指标各区间得分
table_num_cols_describe = {
    'CYRYT_CZYX_rate': [100, 75, 50, 0],  # 初中以下人数占比
    'A_ZYJSR_ZCGC_rate': [50, 70, 90, 100],  # 高级技术人数占比
    'A_ZYJSR_ZCZJ_rate': [60, 70, 90, 100],  # 中级技术人数占比
    'A_ZYJSR_ZCCJ_rate': [60, 75, 85, 100],  # 初级技术人数占比
    'A_ZYJSR_ZCJX_rate': [100, 85, 70, 50],  # 见习技术人数占比
    'AQGLR_ZYFZR_rate': [50, 65, 80, 100],  # 企业负责人持证数（安全资格证）占比
    'AQGLR_TZPX_rate': [50, 65, 85, 100],  # 特种作业人员培训数占比
    'GASGEB_BURSTWORKERNUM_rate': [50, 65, 80, 100],  # 防突施工人员数量占比
    'CMGZM_JCL': [50, 80, 90, 100],  # 机采率
    'GASUTIL_CONMAX': [50, 70, 90, 100],  # 最大瓦斯抽放浓度
    'GASUTIL_CONMIN': [25, 50, 75, 100],  # 最小瓦斯抽放浓度
    'GASUTIL_CONAVG': [25, 50, 75, 100],  # 平均瓦斯抽放浓度
    'GASUTIL_PREDESIGN': [35, 70, 90, 100],  # 设计瓦斯抽放浓度
    'CMGZM_HCL': [50, 70, 80, 100],  # 回采率
    'GASUTIL_PREMAX': [25, 50, 75, 100],  # 最大瓦斯抽放负压
    'GASUTIL_PREMIN': [50, 70, 90, 100],  # 最小瓦斯抽放负压
    'GASUTIL_PREAVG': [50, 65, 80, 100],  # 平均瓦斯抽放负压
    'A_XFMCJJ': [60, 75, 85, 100],  # 下伏煤层间距

    'PENALTY': [100, 80, 60, 25],  # 总处罚金额
    'penalties_times': [90, 60, 30, 0],  # 处罚次数
    'DESIGN_OUTPUT': [50, 70, 90, 100],  # 设计生产能力
    'PROVED_OUTPUT': [60, 80, 90, 100],  # 核定生产能力
    'A_FXDYJ_DMTQJE': [35, 35, 100, 100],  # 吨煤提取金额
    'A_FXDYJ_JYJE': [25, 50, 75, 100],  # 结余金额

    'CYRYT_CZ_rate': [100, 75, 50, 25],  # 初中人数总数占比
    'BZ_DZ_CM_rate': [25, 50, 75, 100],  # 大专人数占比
    'BZ_BKJYS_CM_rate': [20, 50, 75, 100],  # 本科及以上人数占比
    'AQGLR_AQSC_rate': [25, 50, 75, 100],  # 安全生产管理人员数占比
    'AQGLR_AQSCGL_rate': [25, 50, 75, 100],  # 安全生产管理人员持证数（管理人员安全资格证）占比
    'AQGLR_CZS_rate': [25, 50, 75, 100],  # 特种作业资格证人数占比
    'GASGEN_TENICHNUM_rate': [25, 50, 75, 100],  # “一通三防”管理人员数量占比
    'MINE_WATERBURST': [25, 50, 75, 100],  # 平均涌水量
    'MINE_WATERBURST_MAX': [25, 50, 75, 100],  # 最大涌水量
    'MINE_THICK_AVG': [60, 80, 90, 100],  # 煤层平均厚度
    'MINE_THICK_MAX': [25, 50, 75, 100],  # 煤层最大厚度  # 井工 A类小 C类大   露天 A类大 C类小 ……
    'A_SFMCJJ': [60, 80, 90, 100],  # 上覆煤层间距
    # 'A_MCPJQJ': [25, 50, 75, 100],  # 煤层平均倾角
    'A_MCPJQJ': [100, 75, 50, 25],  # 煤层平均倾角
    'Stop_mining_face': [95, 50, 25, 5],  # 停止采掘工作面次数
    'YBYHS': [95, 50, 25, 5],  # 一般隐患数
    'FXDYJ_CCJE': [25, 50, 75, 100],  # 提取金额
    'FXDYJ_SYJE': [25, 50, 75, 100],  # 使用金额
    'NDCLX_CL': [60, 100, 100, 100],  # 产量
    'rectification': [100, 75, 55, 20],  # 责令停产整顿次数
    'Temporary_suspension_license': [100, 60, 30, 0],  # 暂扣安全生产许可证次数
    'stop_construction': [100, 20, 10, 0],  # 责令停止建设次数
    'CMGZM_PCGS': [55, 75, 90, 100],  # 炮采个数
    'CMGZM_PC': [55, 75, 90, 100],  # 普采个数
    'CMGZM_ZCGS': [55, 75, 90, 100],  # 综采个数
    'stop_production': [100, 50, 30, 0],  # 责令立即停止生产次数
    'stop_work': [100, 40, 20, 1],  # 责令立即停止作业次数
}


# 读取连续指标区间打分
query = "(select POINT_CODE,score1,score2,score3,score4 from label_score_interval) emp"
label_score_interval = spark.read.format("jdbc").option("url", url).option("dbtable", query).option("user", user).option("password", password).option("driver", driver).load().toPandas()
label_score_interval[['score1', 'score2', 'score3', 'score4']] = label_score_interval[
    ['score1', 'score2', 'score3', 'score4']].astype(int)
label_score_interval.set_index('POINT_CODE', inplace=True)

def calALL_2(df):
    res_dict = dict()
    for col in df.index:
        res_dict[col] = df.loc[col, :].tolist()
    return res_dict

table_num_cols_describe = calALL_2(label_score_interval)
# 断开spark连接
spark.stop()


# 部分连续型指标需要手动给出分箱区间值
table_num_cols_manual_segmentation = {
    'DIE_PEOPLE_NUM': ([0, 1, 5, 10, 30], [100, 0, 0, 0]),  # 事故死亡人数
    'WEIGHT_HURT_PEOPLE_NUM': ([0, 1, 5, 10, 30], [100, 0, 0, 0]),  # 事故重伤人数
    'LIGHT_HURT_PEOPLE_NUM': ([0, 1, 5, 10, 30], [100, 40, 20, 0]),  # 事故轻伤人数
    'Stop_using_equipment': ([0, 1, 4, 6, 20], [100, 60, 40, 20]),  # 停止使用相关设备次数
    'Evacuation': ([0, 1, 2, 3, 4], [100, 50, 50, 50]),  # 从危险区域撤出作业人员次数
    'Repeated': ([0, 1, 2, 3, 10], [100, 0, 0, 0]),  # 屡查屡犯
    'YHPCZ_ZDYHS': ([0, 1, 2, 3, 12], [100, 0, 0, 0]),  # 重大隐患数
    'CMGZM_ZDHGS': ([0, 1, 2, 3, 11], [25, 50, 75, 100]),  # 自动化开采个数
}


def get_score_tables(table_dic, random_state=0):
    table_dic_new = dict()
    for feature in table_dic.keys():
        scores = table_dic[feature]
        scores_new = scores.copy()
        for k, s in enumerate(scores) if isinstance(scores, list) else scores.items():
            random.seed(str(random_state) + feature + str(k))
            r = random.randrange(100) / 100
            scores_new[k] = s + (0 if (s == 0 or s == 100 or r < 0.4) else -5 if r < 0.7 else 5)
        table_dic_new[feature] = scores_new
    return table_dic_new


table_num_cols_describes = {'层次分析法': table_num_cols_describe,
                            '熵权法': table_num_cols_describe,
                            '组合评价模型': table_num_cols_describe}
table_num_cols_manual_segmentations = {'层次分析法': table_num_cols_manual_segmentation,
                                       '熵权法': table_num_cols_manual_segmentation,
                                       '组合评价模型': table_num_cols_manual_segmentation}
table_obj_colss = {'层次分析法': table_obj_cols,
                   '熵权法': table_obj_cols,
                   '组合评价模型': table_obj_cols}


def fill_na(df):
    """
    根据不同字段的不同业务属性，分别进行缺失值填充
    :param df: 包含缺失值的数据集，类型为 pd.DataFrame
    :return: 返回对df中缺失值进行填充后的数据集，类型为 pd.DataFrame
    """
    df = df.copy()
    data_value = df.iloc[:, 5:]  # 去除前5列煤矿固有属性
    num = data_value.select_dtypes(exclude=['object'])  # 筛选连续型指标
    obj = data_value.select_dtypes(include=['object'])  # 筛选离散型指标
    numcol = list(num.columns)
    objcol = list(obj.columns)

    # 需要采用最小值填充的列
    mincol = ['MINE_THICK_AVG', 'MINE_THICK_MAX', 'A_SFMCJJ', 'A_XFMCJJ', 'GASUTIL_CONMAX',
              'GASUTIL_CONMIN', 'GASUTIL_CONAVG',
              'GASUTIL_PREMAX', 'GASUTIL_PREMIN', 'GASUTIL_PREAVG', 'MINE_WATERBURST',
              'MINE_WATERBURST_MAX']
    # 逆向指标，采用最大值填充
    maxcol = ['CYRYT_CZYX_rate', 'CYRYT_CZ_rate', 'A_ZYJSR_ZCJX_rate', 'PENALTY', 'penalties_times', 'YBYHS',
              'rectification', 'Temporary_suspension_license', 'stop_construction', 'stop_production', 'stop_work',
              'DIE_PEOPLE_NUM', 'WEIGHT_HURT_PEOPLE_NUM', 'LIGHT_HURT_PEOPLE_NUM', 'Stop_using_equipment',
              'Evacuation', 'Repeated', 'YHPCZ_ZDYHS', 'A_MCPJQJ']
    # 其余数值型指标列采用0值填充
    zerocol = [i for i in numcol if i not in (mincol + maxcol)]

    # 离散型变量，用分值最小对应的值填充缺失值
    for col in objcol:
        if col not in table_obj_cols.keys():
            continue
        dic = table_obj_cols[col]
        na_value = min(dic.keys(), key=lambda k: dic[k])
        df[col] = df[col].fillna(na_value)
    # 对于不在得分字典中的离散型变量，用众数填充缺失值
    df[objcol] = df[objcol].fillna(obj.describe().loc['top'])

    # 0值填充
    df[zerocol] = df[zerocol].fillna(0)

    # 其余最小、最大值填充，先按MINE_MINETYPE、MINE_CLASS两个维度填充
    mine_minetype = list(df['MINE_MINETYPE_NAME'].unique())
    mine_class = list(df['MINE_CLASS_NAME'].unique())
    for i in mine_minetype:
        for j in mine_class:
            # 数值变量mincol用最小值填充、maxcol用最大值填充
            idx = (df['MINE_MINETYPE_NAME'] == i) & (df['MINE_CLASS_NAME'] == j)
            df.loc[idx, mincol] = df.loc[idx, mincol].fillna(df.loc[idx, mincol].min())
            df.loc[idx, maxcol] = df.loc[idx, maxcol].fillna(df.loc[idx, maxcol].max())
    # 对于部分空数据集，按MINE_CLASS一个条件填充
    for ft in mine_class:
        idx = (df['MINE_CLASS_NAME'] == ft)
        df.loc[idx, mincol] = df.loc[idx, mincol].fillna(df.loc[idx, mincol].min())
        df.loc[idx, maxcol] = df.loc[idx, maxcol].fillna(df.loc[idx, maxcol].max())

    # 剩下的全为空的列，用0填充
    df.fillna(0, inplace=True)

    # 缺失值个数计算，检查是否还有缺失值
    num_null = df.isnull().sum().sum()
    if num_null > 0:
        print(f'函数fill_na填充缺失值后，数据集df中还有{num_null}个缺失值...')
    return df


def bins_to_int(bins):
    """
    函数 adjust 的子功能函数。
    部分字段取值全为整数，因此需要将分箱结果处理为整数
    :param bins: 分箱列表
    :return: 返回处理后的bins
    """
    # 四舍五入为整数
    bins = [round(_) for _ in bins]
    # 如果有相同的，则后边的等于前边的加1
    for i in range(1, 5):
        if bins[i] <= bins[i - 1]:
            bins[i] = bins[i - 1] + 1
    return bins


def interpolation_bins(bins):
    """
    函数 adjust 的子功能函数。
    由于数据因素，四分位分箱后可能遇到分箱值相等的情况，例如：[0, 0, 0.1, 0.1, 0.2]，
    这时需要进行处理，对于两个相同的值，使后者等于其两边值的平均，即使其变为 [0, 0.05, 0.1, 0.15, 2]，
    :param bins: 分箱列表
    :return: 返回处理后的bins
    """
    bins_old = bins
    bins = bins_old.copy()

    # 在bins后面添加一个略大于bins[-1]的辅助值，用于当原始bins最后两个值相等时对最后一个值算平均
    bins.append(round(bins[-1] + (bins[-1] - bins[0]) / 10, 6))

    # 寻找相等值的开始位置和结束位置
    a, b = 0, 1
    while True:
        if bins[b] == bins[a]:
            b += 1
        else:
            if b - a == 1:
                a += 1
                b += 1
            else:
                break

    # 对相等的值进行线性插值
    for i in range(a + 1, b):
        bins[i] = round(bins[i] + (i - a) * (bins[b] - bins[a]) / (b - a), 6)
    # 去除上面在bins后添加的一个辅助值
    bins = bins[:-1]
    # 使新的bins范围等比缩放为原始bins范围
    if bins[-1] != bins_old[-1]:
        bins = [round(i * bins_old[-1] / bins[-1], 6) for i in bins]
    return bins


def copy_des(df, max_ls=1):
    """
    函数 adjust 的子功能函数
    用于当 ['min', '25%', '50%', '75%', 'max'] 中重复个数过多时，直接使用其他类别煤矿的分箱值来替代
    :param df: 分箱结果数据，pd.DataFrame
    :param max_ls: 当 ['min', '25%', '50%', '75%', 'max'] 中不重复的数值的个数小于这个值时，函数起作用
    :return: 无返回值，直接更改df
    """
    copy_cols = ['25%', '50%', '75%', 'available', 'ls']
    for i in range(df.shape[0]):
        if df.loc[i, 'available'] == 0 and df.loc[i, 'ls'] <= max_ls:
            # 存放该字段的其他类别煤矿分箱结果下标
            i_index = []
            i_index.extend(list(df[(df['POINT_CODE'] == df.loc[i, 'POINT_CODE']) &
                                   (df['MINE_CLASS_NAME'] == df.loc[i, 'MINE_CLASS_NAME'])].index))
            i_index.extend(list(df[df['POINT_CODE'] == df.loc[i, 'POINT_CODE']].index))

            # 查看该字段的其他类别煤矿中是否有标记为1的，若有，则直接复制使用
            for j in i_index:
                if df.loc[j, 'available'] == 1:
                    df.loc[i, 'min'] = min(df.loc[i, 'min'], df.loc[j, 'min'])
                    df.loc[i, 'max'] = max(df.loc[i, 'max'], df.loc[j, 'max'])
                    df.loc[i, copy_cols] = df.loc[j, copy_cols]
                    break


def adjust(des):
    """
    调整分箱结果 des，处理分箱区间段重复、缺失值过多无法分箱等情况
    :param des: 分箱结果数据，pd.DataFrame
    """
    des['available'] = 0
    des.fillna(0, inplace=True)
    des['ls'] = des.apply(lambda x: len(set(x[['min', '25%', '50%', '75%', 'max']])), axis=1)  # 五个值不相同的个数

    # 将五个值全都不相同的标记为1，表示可以直接使用；其余的标记为0，后续做相应修改
    des['available'] = des.apply(lambda x: 1 if x['ls'] == 5 else 0, axis=1)

    # 对五个值中有相同值但不完全相同的情况做线性插值处理：例：[0, 0, 0.1, 0.1, 0.2] -> [0, 0.05, 0.1, 0.15, 2]
    for i in range(des.shape[0]):
        while des.loc[i, 'available'] == 0 and des.loc[i, 'ls'] >= 2:
            bins = list(des.loc[i, ['min', '25%', '50%', '75%', 'max']])
            bins = interpolation_bins(bins)
            if des.loc[i, 'POINT_NAME'][-1] == '数' and bins[-1] >= 4:
                bins = bins_to_int(bins)
            des.loc[i, ['min', '25%', '50%', '75%', 'max']] = bins
            des.loc[i, 'ls'] = len(set(des.loc[i, ['min', '25%', '50%', '75%', 'max']]))
            if des.loc[i, 'ls'] == 5:
                des.loc[i, 'available'] = 1

    # 对五个值全相同的进行复制处理
    copy_des(des, max_ls=1)

    # 剩下的就是不同类别煤矿均无可用切五个值均相同的情况了，直接手动赋值
    for i in range(des.shape[0]):
        mmin, mmax = des.loc[i, 'min'], des.loc[i, 'max']
        if des.loc[i, 'available'] == 0:
            mmin = 0 if mmax > 0 else mmin - 1
            mmax = max(1, mmax) if mmax > 0 else mmax
            tmp = [mmin, 0.75 * mmin + 0.25 * mmax, 0.5 * mmin + 0.5 * mmax, 0.25 * mmin + 0.75 * mmax, mmax, 1, 5]
            des.loc[i, ['min', '25%', '50%', '75%', 'max', 'available', 'ls']] = tmp
        # 由于后续部分指标将使用0填充缺失值，因此需调整区间使其包含0
        if mmin > 0:
            des.loc[i, 'min'] = 0
        if mmax < 0:
            des.loc[i, 'max'] = 0


def dt_optimal_binning_boundary(x, y, criterion='gini'):
    # 利用决策树获得最优分箱的边界值列表,利用决策树生成的内部划分节点的阈值，作为分箱的边界
    clf = DecisionTreeClassifier(criterion=criterion,  # 最小化准则划分
                                 max_leaf_nodes=4,  # 最大叶子节点数
                                 min_samples_leaf=0.05,  # 叶子节点样本数量最小占比
                                 random_state=0)
    clf.fit(x, y)  # 训练决策树
    n_nodes = clf.tree_.node_count  # 决策树的节点数
    children_left = clf.tree_.children_left  # node_count大小的数组，children_left[i]表示第i个节点的左子节点
    children_right = clf.tree_.children_right  # node_count大小的数组，children_right[i]表示第i个节点的右子节点
    threshold = clf.tree_.threshold  # node_count大小的数组，threshold[i]表示第i个节点划分数据集的阈值

    boundary = []  # 待return的分箱边界值列表
    for ii in range(n_nodes):
        if children_left[ii] != children_right[ii]:  # 非叶节点
            boundary.append(threshold[ii])
    boundary.sort()
    return boundary


def binning(df_all, num_cols, df_indictor_config, df_all_class, dt_binning):
    """
    分箱函数，通过对未填充缺失值的原始数据，进行四分位或者决策树分箱，得到分箱后的表格
    :param df_all: 原始宽表，DataFrame
    :param num_cols: 数值型指标所在的列名列表，只对这些列进行分箱
    :param df_indictor_config: 指标配置表，DataFrame
    :param df_all_class: 如果要使用决策树分箱，需提供df_all_class，包含煤矿等级 MINE_CLASSIFY_NAME 列
    :param dt_binning: 使用决策树分箱调整四分位分箱结果，entropy、gini，若为空，则不使用决策树分箱。
    :return: 分箱结果，pd.DataFrame，包含 min 25% 50% 75% max 等列
    """
    mine_minetype = list(df_all['MINE_MINETYPE_NAME'].unique())
    mine_class = list(df_all['MINE_CLASS_NAME'].unique())

    # 先四分位分箱
    ll = []
    for i in mine_minetype:
        for j in mine_class:
            df1 = df_all[(df_all['MINE_MINETYPE_NAME'] == i) & (df_all['MINE_CLASS_NAME'] == j)][num_cols]
            des = df1.describe().round(4).T
            des['MINE_MINETYPE_NAME'] = i
            des['MINE_CLASS_NAME'] = j
            ll.append(des)
    des_all = pd.concat(ll, axis=0, join='inner')
    des_all = des_all.reset_index().sort_values(by=['index', 'MINE_CLASS_NAME'], ascending=False).reset_index(drop=True)
    des_all.rename(columns={'index': 'POINT_CODE'}, inplace=True)

    df_china = df_indictor_config.copy().set_index('POINT_CODE')
    des_all = des_all.join(df_china[['POINT_NAME']], on='POINT_CODE', how='left')

    # 尝试使用决策树监督方法分箱，对于叶子结点不足4个的指标，保留原来的四分位数分箱
    if dt_binning:
        np.random.seed(0)
        for col in num_cols:
            for i in df_all_class['MINE_MINETYPE_NAME'].unique():
                for j in df_all_class['MINE_CLASS_NAME'].unique():
                    idx = (df_all_class['MINE_MINETYPE_NAME'] == i) & (df_all_class['MINE_CLASS_NAME'] == j)
                    df = df_all_class.loc[idx, [col, 'MINE_CLASSIFY_NAME']]
                    df.dropna(how='any', inplace=True)

                    if df.shape[0] < 5:
                        continue
                    x_ = df[[col]]
                    y_ = df['MINE_CLASSIFY_NAME']
                    bins = dt_optimal_binning_boundary(x_, y_, criterion=dt_binning)

                    if len(bins) < 3:
                        continue
                    idx = (des_all['MINE_MINETYPE_NAME'] == i) & (des_all['MINE_CLASS_NAME'] == j) & \
                          (des_all['POINT_CODE'] == col)
                    des_all.loc[idx, ['25%', '50%', '75%']] = bins
    # 调整分箱结果
    adjust(des_all)
    return des_all


def generate_interval_score_lx_lb(df_all, df_indictor_config, df_all_class=None, dt_binning=None):
    """
    创建各字段的区间打分规则表，对于连续字段，每一行是某个字段的一个区间及其对应分数，对于离散字段，每一行为某个字段一个取值及其对应分数
    :param df_all: 原始宽表，DataFrame
    :param df_indictor_config: 指标配置表，DataFrame
    :param df_all_class: 原始数据表格（包含煤矿等级MINE_CLASSIFY_NAME列，若使用决策树分箱，则必须提供此数据集），DataFrame
    :param dt_binning: 使用决策树分箱调整四分位分箱结果，entropy、gini，若为空，则不使用决策树分箱。
    :return: 返回区间打分规则表
    """
    df_all_class = df_all_class.copy()
    df_all = df_all_class.drop('MINE_CLASSIFY_NAME', axis=1)

    num_cols = list(df_all.iloc[:, 5:].select_dtypes(exclude=['object']).columns)  # 连续列
    obj_cols = list(df_all.iloc[:, 5:].select_dtypes(include=['object']).columns)  # 离散列
    no_data_cols = [col for col in obj_cols if df_all.loc[0, col] == '暂未接入数据']  # 暂未接入数据列
    obj_cols = [col for col in obj_cols if col not in no_data_cols]  # 离散列（去除未接入数据的列）

    # 计算分箱结果
    des_all = binning(df_all, num_cols, df_indictor_config, df_all_class, dt_binning)

    # 将区间的分段与各分段的分数对应起来，创建结果表 interval_score_lx_lb
    model_names = ['层次分析法', '熵权法', '组合评价模型']
    mat_res = []
    prod = product(model_names, df_all['MINE_MINETYPE_NAME'].unique(), df_all['MINE_CLASS_NAME'].unique())
    for model_name, i_mmtn, i_mcn in prod:  # 等价于三重 for 循环：三种模型、两种煤矿类型、三种煤矿体量
        # 1. 连续型指标
        for i, col in enumerate(num_cols):
            if col in table_num_cols_describes[model_name]:
                if col in table_num_cols_manual_segmentations[model_name]:
                    print(f'字典 table_num_cols_describe 和 table_num_cols_manual_segmentation 都包含 {col}，该取哪一个呢？')
                bins = des_all[(des_all['MINE_MINETYPE_NAME'] == i_mmtn) & (des_all['MINE_CLASS_NAME'] == i_mcn) &
                               (des_all['POINT_CODE'] == col)][['min', '25%', '50%', '75%', 'max']].values.tolist()[0]
                scores = table_num_cols_describes[model_name][col]
            elif col in table_num_cols_manual_segmentations[model_name]:
                bins, scores = table_num_cols_manual_segmentations[model_name][col]
                bins2 = des_all[(des_all['MINE_MINETYPE_NAME'] == i_mmtn) & (des_all['MINE_CLASS_NAME'] == i_mcn) &
                                (des_all['POINT_CODE'] == col)][['min', '25%', '50%', '75%', 'max']].values.tolist()[0]
                bins[0], bins[-1] = min(bins[0], bins2[0]), max(bins[-1], bins2[-1])
            else:
                raise Exception(f'列名错误：分值字典 table_num_cols_... 中没有名为 {col} 的关键字！')
            for j in range(4):  # 是否可以合并区间而不是对区间边界强行插值？
                mat_res.append([col, round(bins[j], 4), round(bins[j + 1], 4), scores[j], i_mmtn, i_mcn, model_name])

        # 2. 离散型指标
        for col in obj_cols:
            for k in table_obj_colss[model_name][col].keys():
                mat_res.append([col, k, None, table_obj_colss[model_name][col][k], i_mmtn, i_mcn, model_name])

        # 3. 暂未接入数据，全部为100分
        for col in no_data_cols:
            mat_res.append([col, '暂未接入数据', None, 100, i_mmtn, i_mcn, model_name])

    df_res = pd.DataFrame(mat_res, columns=['POINT_CODE', 'LOWER', 'UPPER', 'SCORE', 'MINE_MINETYPE_NAME',
                                            'MINE_CLASS_NAME', 'MODEL_NAME'])

    df_res = df_res.join(df_indictor_config.copy().set_index('POINT_CODE')[['POINT_NAME']], on='POINT_CODE', how='left')

    # 将结果复制到各省份
    province_names = ['河北省', '山西省', '内蒙古自治区', '辽宁省', '吉林省', '黑龙江省', '江苏省', '安徽省', '福建省', '江西省', '山东省',
                      '河南省', '湖北省', '湖南省', '广西壮族自治区', '重庆市', '四川省', '贵州省', '云南省', '陕西省', '甘肃省', '青海省',
                      '宁夏回族自治区', '新疆维吾尔自治区', '新疆兵团']
    lis_res_all = []
    for PROVINCE_NAME in province_names:
        df_res['PROVINCE_NAME'] = PROVINCE_NAME
        lis_res_all.append(df_res.copy())
    df_res = pd.concat(lis_res_all)[['POINT_CODE', 'POINT_NAME', 'LOWER', 'UPPER', 'SCORE', 'PROVINCE_NAME',
                                     'MINE_MINETYPE_NAME', 'MINE_CLASS_NAME', 'MODEL_NAME']]

    print(df_res.shape)
    # 基础 397 行（离散22个共 147 行, 连续 (50 + 8) * 4 = 232 行, 未接入数据 18 行） * 6 矿井类型 * 3 模型 * 25 省 = 178650 行
    return df_res


def calculate_score(data, bins, scores):
    """
    根据区间以及分值计算data中每个值的对应得分
    :param data: pd.Series 原数据
    :param bins: 区间
    :param scores: 每个区间对应分值
    :return: pd.Series 与data对应的分值
    """
    n = len(scores)

    def get_score(d):
        if d < bins[0] or d > bins[-1]:
            print(f'警告：值 {d} 超出bins区间: {bins}')
        for i in range(n - 1):
            if d < bins[i + 1]:
                return scores[i]
        return scores[-1]

    return data.apply(get_score)


def expansion_df(df, df_indictor_config):
    """
    将指标得分明细表展开为每个字段单独一行
    :param df: 格式与原始数据相同的DataFrame，每行一个煤矿，每列一个指标
    :param df_indictor_config: 指标配置表，DataFrame
    :return: 返回展开后的DataFrame，每行为一个煤矿的一个指标
    """
    df = df.copy()
    cols = df.columns
    res_lis = []
    for col in cols[5:]:
        df_col = df[list(cols[:5]) + [col]].rename(columns={col: 'point_scorp'})
        df_col['POINT_CODE'] = col
        res_lis.append(df_col)
    res_df = pd.concat(res_lis, axis=0)

    # 连接字段名、重命名字段
    df_china = df_indictor_config.set_index('POINT_CODE')
    res_df = res_df.join(df_china[['POINT_NAME', 'ID']], on='POINT_CODE', how='left')
    res_df.rename(columns={'ID': 'point_id', 'CORP_ID': 'mine_id', 'CORP_NAME': 'mine_name'}, inplace=True)
    res_df.sort_values(by=['mine_name', 'point_id'], inplace=True)
    return res_df[['point_id', 'POINT_NAME', 'mine_id', 'mine_name', 'MINE_MINETYPE_NAME', 'MINE_CLASS_NAME',
                   'PROVINCE_NAME', 'point_scorp']]


def generate_base_inf_all(df_all, df_indictor_config, df_all_class, dt_binning=None):
    """
    计算每个煤矿在每个三级指标上的得分
    :param df_all: 原始宽表，DataFrame
    :param df_indictor_config: 指标配置表，DataFrame
    :param df_all_class: 原始数据表格（包含煤矿等级MINE_CLASSIFY_NAME列，若使用决策树分箱，则必须提供此数据集），DataFrame
    :param dt_binning: 使用决策树分箱调整四分位分箱结果，可选：'entropy'、'gini'、None，若为空，则不使用决策树分箱。
    :return: 返回三级指标得分表，每一行为某个煤矿的一个三级指标及其对应分数
    """
    df_all_class = df_all_class.copy()
    df_all = df_all_class.drop('MINE_CLASSIFY_NAME', axis=1)

    num_cols = list(df_all.iloc[:, 5:].select_dtypes(exclude=['object']).columns)  # 连续列
    obj_cols = list(df_all.iloc[:, 5:].select_dtypes(include=['object']).columns)  # 离散列
    no_data_cols = [col for col in obj_cols if df_all.loc[0, col] == '暂未接入数据']  # 暂未接入数据列
    obj_cols = [col for col in obj_cols if col not in no_data_cols]  # 离散列（去除未接入数据的列）

    des_all = binning(df_all, num_cols, df_indictor_config, df_all_class, dt_binning)

    df_all = fill_na(df_all)

    model_names = ['层次分析法', '熵权法', '组合评价模型']

    lis_res = []
    for model_name in model_names:
        df_all_i = df_all.copy()
        # 1. 连续型指标
        for i_mmtn, i_mcn in product(df_all['MINE_MINETYPE_NAME'].unique(), df_all['MINE_CLASS_NAME'].unique()):
            index_model = (df_all_i['MINE_MINETYPE_NAME'] == i_mmtn) & (df_all_i['MINE_CLASS_NAME'] == i_mcn)
            for i, col in enumerate(num_cols):
                if col in table_num_cols_describes[model_name]:
                    if col in table_num_cols_manual_segmentations[model_name]:
                        print(f'字典 table_num_cols_describe 和 table_num_cols_manual_segmentation 都包含 {col}')
                    index_col = (des_all['MINE_MINETYPE_NAME'] == i_mmtn) & (des_all['MINE_CLASS_NAME'] == i_mcn) & \
                                (des_all['POINT_CODE'] == col)
                    bins = des_all.loc[index_col, ['min', '25%', '50%', '75%', 'max']].values.tolist()[0]
                    scores = table_num_cols_describes[model_name][col]
                elif col in table_num_cols_manual_segmentations[model_name]:
                    bins, scores = table_num_cols_manual_segmentations[model_name][col]
                    bins2 = des_all[(des_all['MINE_MINETYPE_NAME'] == i_mmtn) & (des_all['MINE_CLASS_NAME'] == i_mcn) &
                                    (des_all['POINT_CODE'] == col)][['min', '25%', '50%', '75%', 'max']].values.tolist()[0]
                    bins[0], bins[-1] = min(bins[0], bins2[0]), max(bins[-1], bins2[-1])
                else:
                    raise Exception(f'列名错误：分值字典 table_num_cols_... 中没有名为 {col} 的关键字！')

                df_all_i.loc[index_model, col] = calculate_score(df_all_i.loc[index_model, col], bins, scores)

        # 2. 离散型指标
        for col in obj_cols:
            df_all_i[col] = df_all_i[col].apply(lambda x: table_obj_colss[model_name][col][x])

        # 3. 暂未接入数据，100分
        for col in no_data_cols:
            df_all_i[col] = 100

        # 上面得到的 df_all_i 是一个与df_all结构类似的DataFrame，每一行为一个煤矿的98个三级指标，
        # 利用 expansion_df 将其展开为目标格式，及每一行仅有一个指标
        df_all_i_expansion = expansion_df(df_all_i, df_indictor_config)
        df_all_i_expansion['MODEL_NAME'] = model_name
        lis_res.append(df_all_i_expansion)

    df_base_inf = pd.concat(lis_res)

    print(df_base_inf.shape)
    return df_base_inf


###########################################################生成缺失值填充表###########################################################
def generate_missing_data_imputation(df_all, df_indictor_config):
    from itertools import product

    df_all = df_all.copy()

    num_cols = list(df_all.iloc[:, 5:].select_dtypes(exclude=['object']).columns)  # 连续列
    obj_cols = list(df_all.iloc[:, 5:].select_dtypes(include=['object']).columns)  # 离散列（包含暂未接入数据列）

    mat_res = []
    for i_mmtn, i_mcn in product(df_all['MINE_MINETYPE_NAME'].unique(), df_all['MINE_CLASS_NAME'].unique()):
        for col in list(df_all.columns)[5:]:
            # 1. 连续
            if col in num_cols:
                des_i = df_all[(df_all['MINE_MINETYPE_NAME'] == i_mmtn) &
                               (df_all['MINE_CLASS_NAME'] == i_mcn)][col].describe()
                if des_i['count'] == 0:
                    des_i = df_all[col].describe()
                des_i = des_i[['mean', 'min', '25%', '50%', '75%', 'max']].values.tolist()
                mat_res.append([col] + des_i + ['数值类型', None, None, i_mmtn, i_mcn])

            # 2. 离散
            elif col in obj_cols:  # 离散
                des_i = df_all[(df_all['MINE_MINETYPE_NAME'] == i_mmtn) &
                               (df_all['MINE_CLASS_NAME'] == i_mcn)][col].describe()
                if des_i['count'] == 0:
                    des_i = df_all[col].describe()
                mode = des_i['top']
                mat_res.append([col, None, None, None, None, None, None, None, mode, '类别类型', i_mmtn, i_mcn])
            else:
                print(f'列名错误：{col}')

    df_res = pd.DataFrame(mat_res, columns=['POINT_CODE', 'mean_z', 'min_z', 'lower_z', 'median_z', 'upper_z', 'max_z',
                                            'type1', 'mode_z', 'type2', 'MINE_MINETYPE_NAME', 'MINE_CLASS_NAME'])

    df_res = df_res.join(df_indictor_config.copy().set_index('POINT_CODE')[['POINT_NAME']], on='POINT_CODE', how='left')

    # 复制
    province_names = ['河北省', '山西省', '内蒙古自治区', '辽宁省', '吉林省', '黑龙江省', '江苏省', '安徽省', '福建省', '江西省', '山东省',
                      '河南省', '湖北省', '湖南省', '广西壮族自治区', '重庆市', '四川省', '贵州省', '云南省', '陕西省', '甘肃省', '青海省',
                      '宁夏回族自治区', '新疆维吾尔自治区', '新疆兵团']
    model_names = ['层次分析法', '熵权法', '组合评价模型']

    lis_res_all = []
    for PROVINCE_NAME, MODEL_NAME in product(province_names, model_names):
        df_res['PROVINCE_NAME'] = PROVINCE_NAME
        df_res['MODEL_NAME'] = MODEL_NAME
        lis_res_all.append(df_res.copy())

    df_res = pd.concat(lis_res_all)[['POINT_CODE', 'POINT_NAME', 'mean_z', 'min_z', 'lower_z', 'median_z', 'upper_z',
                                     'max_z', 'type1', 'mode_z', 'type2', 'PROVINCE_NAME', 'MINE_MINETYPE_NAME',
                                     'MINE_CLASS_NAME', 'MODEL_NAME']]
    df_res=df_res.round(3)
    print(df_res.shape)
    # 98 字段 * 6煤矿类型 * 3 模型 * 25省 = 44100 行
    return df_res


####################################################################权重计算函数##########################################################
import os


def generate_comparison_matrix(df_model_detail):
    """
    根据 df_model_detail 中提供的指标重要性构造层次分析法判断矩阵
    :return: 一个字段，key 为字段的 PARENT_ID，值为 PARENT_ID 对应的子字段之间的比较矩阵 （ nxn DataFrame ）
    """
    def test_consitst(array, prt=False):
        """子函数，检验判断矩阵是否满足一致性"""
        n = array.shape[0]
        if n <= 2:
            if prt:
                print("小于或者等于两个子因素，不存在一致性问题")
            return True
        RI_list = [0, 0, 0.52, 0.89, 1.12, 1.26, 1.36, 1.41, 1.46, 1.49, 1.52, 1.54, 1.56, 1.58, 1.59]
        RI = RI_list[n - 1]
        # numpy.linalg.eig() 计算矩阵特征值与特征向量
        eig_val, eig_vector = np.linalg.eig(array)
        # 获取最大特征值
        max_val = np.max(eig_val).real
        # 计算CI值
        CI = (max_val - n) / (n - 1)
        # 计算CR值
        CR = CI / RI
        # CR < 0.10才能通过检验
        if CR < 0.10:
            if prt:
                print("判断矩阵的CR值为" + str(round(CR, 4)) + "，通过一致性检验")
            return True
        else:
            if prt:
                print("判断矩阵的CR值为" + str(round(CR, 4)) + "，未通过一致性检验")
            return False

    def reset_compare(c, p=0.3):
        """调整c的值使其满足整数或者半整数的条件"""
        reciprocal = False
        # 如果c小于1，则先将其取倒，再计算，最后返回结果前再取倒回来
        if c < 1:
            c = 1 / c
            reciprocal = True

        # 给出限制c可能的取值
        ci = [1, 1, 1.5, 2, 2.5, 3, 4, 5, 6, 7, 8, 9, 9]
        res = 9
        for i in range(1, len(ci) - 1):
            if c <= ci[i]:
                # 当值介于ci中两个值之间时，以一定概率去取左边或右边的值，而不是简单的四舍五入
                r = random.random()
                diff_rate = 0 if c == 1 else (ci[i] - c) / (ci[i] - ci[i - 1])
                if r < p / 2 + diff_rate * p / 2:
                    res = ci[i - 1]
                elif r > 1 - p / 2 + diff_rate * p / 2:
                    res = ci[i + 1]
                else:
                    res = ci[i]
                break
        return round(1 / res, 2) if reciprocal else res

    def get_matrix(weights, reset=True):
        """将一组权重weights (n, )，根据规则转换为判断矩阵 (n, n)"""
        n = weights.shape[0]

        # 维度小于2，无需转换
        if n == 1:
            return np.array([[1]])

        random.seed(0)
        # 构造nxn矩阵mat，保存判断矩阵
        mat = np.ones((n, n))
        # 对weights中的值进行两两对比，得到的比值进行适当调整即为判断矩阵中对应的值
        for i in range(n):
            for j in range(n):
                if i == j:
                    mat[i, j] = 1
                elif i < j:
                    mat[i, j] = reset_compare(weights[i] / weights[j]) if reset else weights[i] / weights[j]
                else:
                    mat[i, j] = round(1 / mat[j, i], 2)

        # 若判断矩阵不通过一致性检验，则以更保守的规则重新构造
        if not test_consitst(mat):
            for i in range(n):
                for j in range(n):
                    mat[i, j] = reset_compare(weights[i] / weights[j], p=0)
        return mat

    df = df_model_detail.copy()
    df = df[df['MODEL_NAME'] == '层次分析法']
    df = df[['COMPUTED_RESULT', 'MODEL_PK']]  # 'MODEL_NAME', 'PROVINCE_NAME'

    # 上一级指标的ID，根据此ID判断哪些指标属于同一组
    df['PARENT_ID'] = df['MODEL_PK'].apply(lambda x: str(x)[:-2])

    # 保存判断矩阵的字典
    dic_comparison_matrices = dict()
    for PARENT_ID in df['PARENT_ID'].unique():
        df_i = df[df['PARENT_ID'] == PARENT_ID]
        weights_old = df_i['COMPUTED_RESULT'].values
        com_mat = get_matrix(weights_old)
        df_com_mat = pd.DataFrame(com_mat, columns=list(df_i['MODEL_PK']), index=df_i['MODEL_PK'])
        dic_comparison_matrices[PARENT_ID] = df_com_mat

    # # 输出部分判断矩阵方便查看
    # for PARENT_ID in ['1', '101', '10101']:
    #     df_mat = dic_comparison_matrices[PARENT_ID]
    #     print(f'指标{list(df_mat.columns)}的判断矩阵如下：')
    #     print(df_mat)

    return dic_comparison_matrices


def analytic_hierarchy_process(df_model_detail, dic_comparison_matrices):
    """
    层次分析法
    :param df_model_detail: 原始数据表
    :param dic_comparison_matrices: 判断矩阵字典（由函数generate_comparison_matrix生成）
    :return: 层次分析法计算权重结果 DataFrame
    """

    def ahp_single_matrix(data):
        # 层次分析法计算函数，给定一个判断矩阵，返回这一组指标的权重
        if data.shape[0] == data.shape[1] - 1 and data.iloc[:, 0].dtype == 'object':
            data = data.iloc[:, 1:]
            data.index = data.columns
        elif data.shape[0] == data.shape[1]:
            data.index = data.columns
        else:
            raise Exception("please recheck your data structure , you must keep a equal num of the row and col")
        weight_matrix = data.values
        weight_vector = weight_matrix / np.sum(data.values, axis=0)  # axis=0按列求和
        sum_vector_col = weight_vector.sum(axis=1)
        return sum_vector_col / sum_vector_col.sum()

    df = df_model_detail.copy()
    df = df[df['MODEL_NAME'] == '层次分析法']
    df = df[['COMPUTED_RESULT', 'MODEL_PK']]  # 'MODEL_NAME', 'PROVINCE_NAME'
    df['PARENT_ID'] = df['MODEL_PK'].apply(lambda x: str(x)[:-2])

    # 根据每个判断矩阵，计算每一组指标的权重
    weights_lis = []
    for PARENT_ID in df['PARENT_ID'].unique():
        w = ahp_single_matrix(dic_comparison_matrices[PARENT_ID])
        w = pd.DataFrame(w, index=dic_comparison_matrices[PARENT_ID].index, columns=['层次分析法'])
        weights_lis.append(w)
    # 将每一组权重拼接起来，即为层次分析法的最终权重
    weights = pd.concat(weights_lis)
    return weights


def entropy_weight_method(df):
    """
    熵权法计算变量的权重  entropy weight method(EWM)
    :param df: df_all 原始数据，只包含连续指标列， DataFrame
    :return: 权重 w (DataFrame)，注意与层次分析法不同的是只包含了三级指标，并且全部之和为1
    """
    # 第一步读取文件，如果未标准化，则标准化
    data = df.copy()
    data.fillna(0, inplace=True)
    data = (data - data.min()) / (data.max() - data.min())
    m, n = data.shape

    # 将dataframe格式转化为matrix格式
    data = data.values

    # 第二步，计算pij
    pij = data / data.sum(axis=0)

    # 计算每种指标的信息熵
    tmp = pij * np.ma.log(pij).filled(0)
    ej = -1 / np.log(m) * (np.nansum(tmp, axis=0))

    # 计算每种指标的权重
    wi = (1 - ej) / np.sum(1 - ej)

    wi = pd.DataFrame(wi, index=df.columns, columns=['熵权法'])
    return wi


def supervision_method(df_all_class, save_fea_=True):  # todo  save_fea_改为False
    """
    监督方法计算权重，包括catboost、随机森林等，计算过程需要使用到煤矿的等级信息
    :param df_all_class: 包含等级信息的原始数据，等级信息在最后一列 MINE_CLASSIFY_NAME
    :param save_fea_: 是否将分类器计算的权重缓存到文件中，用于调试。部署后应将其设置为False，这样数据更新后才能及时得到新的权重
    :return: 返回catboost及随机森林计算的权重，与熵权法结果类似只包含了三级指标，并且全部之和为1
    """
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import LabelEncoder
    import pickle

    df_all = df_all_class.iloc[:, 5:]

    obj_features_indices = list(np.where(df_all.dtypes == np.object)[0])  # 离散指标
    num_features_indices = [i for i in range(df_all.shape[1]) if i not in obj_features_indices]
    # 类别型变量编码
    for col in df_all.iloc[:, obj_features_indices].columns:
        df_all[col] = LabelEncoder().fit_transform(df_all[col].values)
    # 数值型变量归一化
    from sklearn.preprocessing import MinMaxScaler
    for col in df_all.iloc[:, num_features_indices].columns:
        df_all[col] = MinMaxScaler().fit_transform(df_all[[col]])

    x_train, x_test, y_train, y_test = train_test_split(df_all.iloc[:, :-1], df_all.iloc[:, -1], test_size=0.2,
                                                        random_state=1234)

    # 1. catboost
    from catboost import CatBoostClassifier
    model = CatBoostClassifier(cat_features=obj_features_indices[:-1], iterations=200, random_state=0)

    fea_file_name = 'fea_catboost.data'
    if save_fea_ and os.path.exists(fea_file_name):
        with open(fea_file_name, 'rb') as f:
            fea_ = pickle.load(f)
    else:
        model.fit(x_train, y_train)
        fea_ = model.feature_importances_
        fea_ = fea_ / fea_.sum()
        if save_fea_:
            with open(fea_file_name, 'wb') as f:
                pickle.dump(fea_, f)

    weights = pd.DataFrame(fea_, index=x_train.columns, columns=['catboost'])

    # 2. 随机森林
    from sklearn.ensemble import RandomForestClassifier
    model = RandomForestClassifier(random_state=0)
    model.fit(x_train, y_train)
    fea_ = model.feature_importances_
    fea_ = fea_ / fea_.sum()
    weights['随机森林'] = fea_

    weights = round(weights, 5)
    return weights, list(weights.columns)


def quanzhongpipeidu(weights_all, weight_name, df_class, base_inf_all, prt=False):
    """
    通过计算每种权重下各煤矿的最终得分，并与煤矿等级（A、B、C、D）相比计算匹配程度，同时得到组合评价模型的组合加权
    :param weights_all: 所有权重所在表格 DataFrame
    :param weight_name: 权重名称 list
    :param df_class: 原始数据表
    :param base_inf_all: 三级指标得分表，DataFrame
    :param prt: 是否输出计算的分值
    :return: 一个列表，对应各权重方法的匹配程度，用于组合评价模型中的组合加权
    """
    df_class = df_class.copy()
    weights = weights_all.loc[weights_all['level'] == 3, ['MODEL_PK', weight_name]].copy().set_index('MODEL_PK')

    if round(weights.sum().values[0], 2) != 1:
        # 检查权重值和是否约等于1
        raise ValueError(f'weights.sum(): {weights.sum()}  ..mg')

    # 数据表格预处理
    base_inf_all = base_inf_all[['point_id', 'mine_id', 'point_scorp', 'MODEL_NAME']]
    base_inf_all = base_inf_all[base_inf_all['MODEL_NAME'] == '层次分析法'][['point_id', 'mine_id', 'point_scorp']]
    scores = base_inf_all.join(weights, on='point_id', how='left')
    scores['scores'] = scores['point_scorp'] * scores[weight_name]
    scores = scores.groupby(['mine_id'])['scores'].sum()
    df_class = df_class.join(scores, on='CORP_ID', how='left').sort_values(by='scores', ascending=False)

    df_class_h = df_class.iloc[:df_class.shape[0] // 2]
    rate = []

    print(f'================================\n{weight_name:}') if prt else 0
    classify = sorted(df_class_h['MINE_CLASSIFY_NAME'].unique())
    for cls in classify:
        c = df_class_h[df_class_h['MINE_CLASSIFY_NAME'] == cls].shape[0]
        c_all = df_class[df_class['MINE_CLASSIFY_NAME'] == cls].shape[0]
        rate.append(round(c / c_all, 2))
        print(f'{cls}平均分数：', df_class.loc[df_class['MINE_CLASSIFY_NAME'] == cls, 'scores'].mean()) if prt else 0
        # print(f'得分排名前1/2中，{cls}占比：{rate[-1]}   {c} / {c_all}') if prt else 0
    ww = ((rate[0] * 2 + rate[1]) / (rate[2] + rate[3] * 2)) if len(classify) == 4 else (rate[0] / rate[2])
    ww = round(ww, 4)
    # print(f'(2A+B)/(C+2D)或A/C, 得分与类别的匹配程度：{ww}') if prt else 0
    return ww


def check_weights(df_weights_all, standard_col, goal_cols):
    """
    检查并调整权重，使其满足以下条件：
    1. 检查权重方向，如果有和业务不一致的可以交换顺序（比如 catboost 的一般隐患数、重大隐患数）
    2. 调整其他权重，使其与 catboost 权重具有相似的趋势 （具体方式为：catboost 权重排序前30大的，在目标权重中找到这30个权重，并求和设为 a，
       其和如果小于 catboost 权重中这30个的和 b，则设 c=0.1a+0.9b 为我们需要将这30个指标权重之和调整的目标，剩余的权重等比缩放为 1-c ，
       然后，这30个权重挨个调整，使小于catboost权重的与 catboost 对应权重之差在 0.3倍catboost权重范围内随机产生。最后归一化处理即可）
    :param df_weights_all: 权重结果表，DataFrame
    :param standard_col: 基准列，及catboost权重
    :param goal_cols: 目标列，即待调整的权重
    :return: 调整后的权重表，DataFrame
    """
    df = df_weights_all.copy()

    # 检查业务上重要权重
    idx1, idx2 = df['POINT_NAME'] == '事故死亡人数', df['POINT_NAME'] == '事故重伤人数'
    if df.loc[idx1, standard_col].values[0] < df.loc[idx2, standard_col].values[0]:
        tmp = df.loc[idx1, standard_col].values[0]
        df.loc[idx1, standard_col] = df.loc[idx2, standard_col].values[0]
        df.loc[idx2, standard_col] = tmp
    idx1, idx2 = df['POINT_NAME'] == '重大隐患数', df['POINT_NAME'] == '一般隐患数'
    if df.loc[idx1, standard_col].values[0] < df.loc[idx2, standard_col].values[0]:
        tmp = df.loc[idx1, standard_col].values[0]
        df.loc[idx1, standard_col] = df.loc[idx2, standard_col].values[0]
        df.loc[idx2, standard_col] = tmp
    important_indicators = ['事故死亡人数', '重大隐患数', '处罚次数', '事故等级']
    for POINT_NAME in important_indicators:
        if df.loc[df['POINT_NAME'] == POINT_NAME, standard_col].values[0] < 0.022:
            df.loc[df['POINT_NAME'] == POINT_NAME, standard_col] = 0.022
    if df.loc[df['POINT_NAME'] == '初中人数总数占比', standard_col].values[0] > 0.012:
        df.loc[df['POINT_NAME'] == '初中人数总数占比', standard_col] = 0.012
    if df.loc[df['POINT_NAME'] == '见习技术人数占比', standard_col].values[0] > 0.008:
        df.loc[df['POINT_NAME'] == '见习技术人数占比', standard_col] = 0.008
    df[standard_col] = df[standard_col] / df[standard_col].sum()

    # 调整其他权重
    imp_num = 30
    df.sort_values(by=[standard_col], ascending=False, inplace=True)
    b = df.iloc[:imp_num][standard_col].sum()
    for col in goal_cols:
        a = df.iloc[:imp_num][col].sum()
        c = 0.1 * a + 0.9 * b
        idx_rear = df.iloc[imp_num:].index
        df.loc[idx_rear, col] = df.loc[idx_rear, col] / df.loc[idx_rear, col].sum() * (1 - c)

        for POINT_NAME in df.iloc[:imp_num]['POINT_NAME'].unique():
            random.seed(col + POINT_NAME)
            idx = df['POINT_NAME'] == POINT_NAME
            if df.loc[idx, col].values[0] < df.loc[idx, standard_col].values[0] * 0.7:
                df.loc[idx, col] = df.loc[idx, standard_col] * random.randrange(7000, 10000) / 10000 * 0.8 + \
                                   df.loc[idx, col] * 0.2
        df[col] = df[col] / df[col].sum()

    return df


def weight_models(df_indictor_config, df_model_detail, base_inf_all, df_all=None, df_all_class=None,
                  use_supervision_method=True):
    """
    三种模型计算权重（层次分析法、熵权法、组合评价模型），且组合评价模型中使用了catboost等监督方法
    :param df_indictor_config: 指标配置表，DataFrame
    :param df_model_detail: 指标重要性表，DataFrame
    :param base_inf_all: 三级指标得分表，DataFrame
    :param df_all: 原始宽表，DataFrame
    :param use_supervision_method: 是否使用监督方法调整最终权重，使其更接近ABCD分类结果,bool
    :param df_all_class: 原始数据，在df_all的最后多一列，为每个煤矿对应的ABCD等级，用于监督方法（df_all和df_all_class应至少提供一个）
    :return: 返回模型明细表model_detail_all，DataFrame
    """
    # todo ：del
    try:
        from 生成区间分段表及打分 import fill_na
    except:
        global fill_na

    if df_all is None and df_all_class is None:
        raise ValueError('both df_all and df_all_class are empty..mg')
    if use_supervision_method and df_all_class is None:
        raise ValueError(f'use_supervision_method but df_all_class is empty..mg')

    if use_supervision_method:
        df_all_class = df_all_class[df_all_class['MINE_CLASSIFY_NAME'] != 'D类']  # 只用ABC类
        df_all_class = fill_na(df_all_class)
        df_all = df_all_class.drop('MINE_CLASSIFY_NAME', axis=1)
    else:
        df_all = fill_na(df_all)

    def calculation_weights_level_3_by_mul_12(df_weights_all, col):
        """三级指标乘以对应二级和一级指标权重，得到展开的三级指标（即所有三级指标之和为1）"""
        for level in [2, 3]:
            for i in range(df_weights_all.shape[0]):
                if df_weights_all.loc[i, 'level'] == level:
                    df_weights_all.loc[i, col] *= df_weights_all.loc[
                        df_weights_all['MODEL_PK'] == df_weights_all.loc[i, 'PARENT_ID'], col].values
        df_weights_all.loc[df_weights_all['level'] <= 2, col] = None

    def calculation_weights_level_12_by_add_3(df_weights_all, col):
        """三级指标相加得到二级指标，二级指标相加得到一级指标"""
        for level in [2, 1]:
            for i in range(df_weights_all.shape[0]):
                if df_weights_all.loc[i, 'level'] == level:
                    df_weights_all.loc[i, col] = df_weights_all.loc[
                        df_weights_all['PARENT_ID'] == df_weights_all.loc[i, 'MODEL_PK'], col].sum()

    def weight_normalized(df_weights_all, col):
        """调整权重，使每一组指标权重相加为1"""
        for PARENT_ID in df_weights_all['PARENT_ID'].unique():
            df_i = df_weights_all[df_weights_all['PARENT_ID'] == PARENT_ID]
            w_new = df_i[col] / (df_i[col].sum() or 1)
            w_new = round(w_new, 3)
            s = round(w_new.sum(), 3)
            if s != 1:
                w_new.iloc[-1] += 1 - s
            df_weights_all.loc[df_weights_all['PARENT_ID'] == PARENT_ID, col] = w_new

    # 创建 weights_all，index为评价模型的一二三级指标，用于保存后续计算的权重
    weights_all = df_model_detail.loc[df_model_detail['MODEL_NAME'] == '层次分析法', ['MODEL_REMARK', 'MODEL_PK']].copy()
    weights_all = weights_all.join(df_indictor_config.set_index('ID')[['POINT_NAME', 'POINT_CODE']],
                                   on='MODEL_PK', how='left')
    weights_all['PARENT_ID'] = weights_all['MODEL_PK'].apply(lambda x: str(x)[:-2])
    weights_all['level'] = weights_all['PARENT_ID'].apply(lambda x: (len(str(x)) + 1) // 2)  # 指标级别
    idx_l3 = (weights_all['level'] == 3)

    # 1. 层次分析法
    dic_comparison_matrices = generate_comparison_matrix(df_model_detail)
    weights_ahp = analytic_hierarchy_process(df_model_detail, dic_comparison_matrices)  # 层次分析法
    weights_all = weights_all.join(weights_ahp[['层次分析法']], on='MODEL_PK', how='left')
    del dic_comparison_matrices
    calculation_weights_level_3_by_mul_12(weights_all, '层次分析法')

    # 2. 熵权法
    num_cols = list(df_all.iloc[:, 5:].select_dtypes(exclude=['object']).columns)  # 连续列
    weights_ewm = entropy_weight_method(df_all[num_cols])
    weights_all = weights_all.join(weights_ewm[['熵权法']], on='POINT_CODE', how='left')
    # 使熵权法连续指标的权重和等于相应的层次分析法的权重和
    weights_all['熵权法'] = weights_all['熵权法'] * weights_all.loc[weights_all['熵权法'].notnull(), '层次分析法'].sum()
    # 熵权法离散指标权重用层次分析法权重代替
    idx = weights_all['熵权法'].isna() & idx_l3
    weights_all.loc[idx, '熵权法'] = weights_all.loc[idx, '层次分析法']

    method_list = ['层次分析法', '熵权法']

    # 3. 组合评价模型
    if use_supervision_method:
        # 监督方法得出特征重要性作为权重参考，并据此调整组合评价模型的权重
        weights_supervision_method, method_list_sup = supervision_method(df_all_class)
        weights_all = weights_all.join(weights_supervision_method, on='POINT_CODE', how='left')
        method_list = method_list + method_list_sup

        weights_all = check_weights(weights_all, standard_col='catboost', goal_cols=[_ for _ in method_list if _ != 'catboost'])

        # 计算各权重组合加权方式，并查看各权重得分
        df_class = df_all_class[['CORP_ID', 'MINE_CLASSIFY_NAME']].set_index('CORP_ID').copy()
        wws = [max(quanzhongpipeidu(weights_all, method, df_class, base_inf_all, prt=False) ** 2 - 1, 0) for method in method_list]
        wws = [round(i / sum(wws), 6) for i in wws]
        print(f'{method_list}各权重组合占比：\n{wws}')
        # 计算组合权重
        weights_all.loc[idx_l3, '组合评价模型'] = (weights_all.loc[idx_l3, method_list] * wws).sum(axis=1)
        method_list.append('组合评价模型')

        # 查看组合评价模型得分
        quanzhongpipeidu(weights_all, '组合评价模型', df_class, base_inf_all, prt=False)
    else:
        # 如果不适用监督方法，则直接将层次分析法和熵权法加权平均得到组合法
        weights_all.loc[idx_l3, '组合评价模型'] = \
            weights_all.loc[idx_l3, '层次分析法'] * 0.8 + weights_all.loc[idx_l3, '熵权法'] * 0.2
        method_list.append('组合评价模型')

    # 三级指标类型
    weights_all['指标类别'] = weights_all['POINT_CODE'].apply(
        lambda x: None if x not in df_all.columns else '数值型' if x in num_cols else '类别型')

    # 根据三级指标权重计算二级指标、一级指标权重，并调整权重，使每一组相加为1
    for method in method_list:
        calculation_weights_level_12_by_add_3(weights_all, method)
        weight_normalized(weights_all, method)

    # 表结构变换，展开为model_detail_all所需格式
    model_names = ['层次分析法', '熵权法', '组合评价模型']
    df_res = df_model_detail.copy()
    for model_name in model_names:
        for MODEL_PK in df_res['MODEL_PK'].unique():
            df_res.loc[(df_res['MODEL_NAME'] == model_name) & (df_res['MODEL_PK'] == MODEL_PK), 'COMPUTED_RESULT'] \
                = weights_all.loc[weights_all['MODEL_PK'] == MODEL_PK, model_name].values[0]
    df_res.sort_values(by=['MODEL_NAME', 'MODEL_PK'], inplace=True)

    # 各省份复制拼接
    province_names = ['河北省', '山西省', '内蒙古自治区', '辽宁省', '吉林省', '黑龙江省', '江苏省', '安徽省', '福建省', '江西省', '山东省',
                      '河南省', '湖北省', '湖南省', '广西壮族自治区', '重庆市', '四川省', '贵州省', '云南省', '陕西省', '甘肃省', '青海省',
                      '宁夏回族自治区', '新疆维吾尔自治区', '新疆兵团']
    lis_res_all = []
    for PROVINCE_NAME in province_names:
        df_res['PROVINCE_NAME'] = PROVINCE_NAME
        lis_res_all.append(df_res.copy())
    df_res = pd.concat(lis_res_all)

    df_res['ID'] = range(1, df_res.shape[0] + 1)
    df_res.reset_index(drop=True, inplace=True)
    return df_res






#####################################################################模型得分计算函数#####################################################
#定义函数计算一二级和总分
def compute_total_score(df1,df2,df3):
    #类型判断
    #判断和转换df1字段类型
    if df1['point_id'].dtype == 'int64':
        df1['point_id']=df1['point_id'].astype('str')
    if df1['point_id'].dtype == 'float64':
        df1['point_id']=df1['point_id'].astype('int').astype('str')
    if df1['mine_id'].dtype == 'int64':
        df1['mine_id']=df1['mine_id'].astype('str')
    if df1['mine_id'].dtype == 'float64':
        df1['mine_id']=df1['mine_id'].astype('int').astype('str')    
    #判断和转换df2字段类型
    if df2['MODEL_PK'].dtype == 'int64':
        df2['MODEL_PK']=df2['MODEL_PK'].astype('str')
    if df2['MODEL_PK'].dtype == 'float64':
        df2['MODEL_PK']=df2['MODEL_PK'].fillna(0).astype('int').astype('str')
    #判断和转换df3字段类型
    if df3['ID'].dtype == 'int64':
        df3['ID']=df3['ID'].astype('str')
    if df3['ID'].dtype == 'float64':
        df3['ID']=df3['ID'].fillna(0).astype('int').astype('str')
    if df3['PARENT_ID'].dtype == 'int64':
        df3['PARENT_ID']=df3['PARENT_ID'].astype('str')
    if df3['PARENT_ID'].dtype == 'float64':
        df3['PARENT_ID']=df3['PARENT_ID'].fillna(0).astype('int').astype('str')     
    ###表关联
    #表关联:base_inf：基础表与model_detail：模型明细表关联
    dfc1=pd.merge(df1,df2,left_on='point_id',right_on='MODEL_PK',how='inner')
    dfc1.drop(columns=['MODEL_PK'],inplace=True)
    #根据一二三级指标设计规范，截取三级指标point_id前5位，得到二级指标编码
    dfc1['point_id2']=dfc1['point_id'].str[:5]
    dfc1['point_id2_scor']=dfc1['point_scorp']*dfc1['COMPUTED_RESULT']
    #以煤矿ID和煤矿编码及point_id2（二级指标编码）分组求和计算二级得分
    dfc2=dfc1[['mine_id','mine_name','point_id2','point_id2_scor']].groupby(['mine_id','mine_name','point_id2']).sum().reset_index()
    dfc2s=pd.merge(dfc2,df3,left_on='point_id2',right_on='ID',how='inner')
    dfc2s.drop(columns=['ID','POINT_CODE','PARENT_ID'],inplace=True)
    dfc2s['point_id']=dfc2s['point_id2']
    dfc2s['point_scorp']=dfc2s['point_id2_scor']
    dfc2s_cp=dfc2s.copy()
    #dfc2s_cp.head()
    #同理计算一级指标得分
    dfc3=dfc2s=pd.merge(dfc2s,df2,left_on='point_id2',right_on='MODEL_PK',how='inner')
    dfc3.drop(columns=['POINT_NAME'],inplace=True)
    #截取前3位作为一级指标
    dfc3['point_id1']=dfc3['point_id2'].str[:3]
    dfc3['point_id1_scor']=dfc3['point_id2_scor']*dfc3['COMPUTED_RESULT']
    #以煤矿ID和煤矿编码及point_id1（1级指标编码）分组求和计算1级得分
    dfc3=dfc3[['mine_id','mine_name','point_id1','point_id1_scor']].groupby(['mine_id','mine_name','point_id1']).sum().reset_index()
    dfc3s=pd.merge(dfc3,df3,left_on='point_id1',right_on='ID',how='inner')
    dfc3s['point_id']=dfc3s['point_id1']
    dfc3s['point_scorp']=dfc3s['point_id1_scor']
    dfc3s_cp=dfc3s.copy()
    #dfc3s_cp.head()
    #同理计算总分
    dfc4=dfc3s=pd.merge(dfc3s,df2,left_on='point_id1',right_on='MODEL_PK',how='inner')
    dfc4.drop(columns=['POINT_NAME'],inplace=True)
    dfc4['point_all_scor']=dfc4['point_id1_scor']*dfc4['COMPUTED_RESULT']
    #求和计算
    dfc4s=dfc4[['mine_id','mine_name','point_all_scor']].groupby(['mine_id','mine_name']).sum().reset_index()
    dfc4s['point_id']=list(df3[df3['POINT_CODE']=='total_score']['ID'])[0]
    dfc4s['POINT_NAME']=list(df3[df3['POINT_CODE']=='total_score']['POINT_NAME'])[0]
    dfc4s['point_scorp']=dfc4s['point_all_scor']
    ###合并二三级得分及总分
    tbname=list(df1.columns)
    hb2=dfc2s_cp[tbname]
    hb1=dfc3s_cp[tbname]
    hb0=dfc4s[tbname]
    dfall=pd.concat([hb1,hb0,hb2],axis=0,join='inner')
    dfall['point_scorp']=dfall['point_scorp'].astype('int')
    #print(dfall.head())
    print(dfall.shape)
    return dfall
#计算三种模型合并后总分
def cmopute_score_all(df1,df2,df3):
    #计算三个模型得分
    df2=df2[df2['PROVINCE_NAME']=='山西省']
    base_inf_col=['point_id','POINT_NAME','mine_id','mine_name','point_scorp']
    model_detail_col=['ID','MODEL_NAME','ALGORITHM_TYPE','COMPUTED_RESULT','YW_CONFORMITY','ENABLE_TYPE','MODEL_REMARK','MODEL_PK']
    ahp=compute_total_score(df1[df1['MODEL_NAME']=='层次分析法'][base_inf_col],df2[df2['MODEL_NAME']=='层次分析法'][model_detail_col],df3)
    ahp['MODEL_NAME']='层次分析法'
    sqf=compute_total_score(df1[df1['MODEL_NAME']=='熵权法'][base_inf_col],df2[df2['MODEL_NAME']=='熵权法'][model_detail_col],df3)
    sqf['MODEL_NAME']='熵权法'
    combine=compute_total_score(df1[df1['MODEL_NAME']=='组合评价模型'][base_inf_col],df2[df2['MODEL_NAME']=='组合评价模型'][model_detail_col],df3)
    combine['MODEL_NAME']='组合评价模型'
    #合并得分
    score_all=pd.concat([ahp,sqf,combine],axis=0,join='inner')
    df1new=df1[['mine_id','mine_name','MINE_MINETYPE_NAME','MINE_CLASS_NAME','PROVINCE_NAME']].drop_duplicates()
    model_score_all=pd.merge(score_all,df1new,on=['mine_id','mine_name'],how='inner')
    model_score_all=model_score_all[list(df1.columns)]
    return model_score_all

#先生成扣分数据，模型整体解释在文字生成后再添加
def deduce_score(t_bzh_jj,dfall):
    #t_bzh_jj:标准化降级原始表；dfall：原始宽表
    #生成标准化降级数据
    t_bzh_jj['XDJ']=t_bzh_jj['XDJ'].astype('float64')
    t_bzh_jj['YDJ']=t_bzh_jj['YDJ'].astype('float64')
    t_bzh_jj['bzh']=t_bzh_jj['XDJ']-t_bzh_jj['YDJ']
    t_bzh_jj=t_bzh_jj[t_bzh_jj['bzh']>0]
    t_bzh_jj['bzhjj']='标准化降级'
    JJ=t_bzh_jj[['CORP_ID','bzhjj']]
    JJ.drop_duplicates(inplace=True)
    #生成几个扣分字段
    deduct=dfall[['CORP_ID','CORP_NAME','ACCIDENT_LEVEL','YHPCZ_ZDYHS','LICENSE_STATUS']]
    dedut=pd.merge(deduct,JJ,on=['CORP_ID'],how='left')
    #生成重点扣分项规则
    #事故情况
    ACCIDENT_LEVEL=lambda x:-15 if x=='较大事故 ' else -20 if x=='重大事故' else -10 if x=='一般事故' else 0
    dedut['ACCIDENT_LEVEL'] = dedut['ACCIDENT_LEVEL'].apply(ACCIDENT_LEVEL)
    #重大隐患
    YHPCZ_ZDYHS=lambda x:-5 if x==1 else -10 if x>1 else 0
    dedut['YHPCZ_ZDYHS'] = dedut['YHPCZ_ZDYHS'].apply(YHPCZ_ZDYHS)
    #许可证吊销
    LICENSE_STATUS=lambda x:-100 if x=='吊销' else 0
    dedut['LICENSE_STATUS'] = dedut['LICENSE_STATUS'].apply(LICENSE_STATUS)
    #标准化降级
    jiangji=lambda x:-20 if x=='标准化降级' else 0 
    dedut['bzhjj'] = dedut['bzhjj'].apply(jiangji)
    dedut.columns=['CORP_ID', 'CORP_NAME', '事故情况', '重大隐患','许可证吊销', '标准化降级']
    #print(dedut.head(3))
    print(dedut.shape)
    #字段行转列
    ll=[]
    for ft in list(dedut.columns[2:]):
        dt=dedut[list(dedut.columns[:2])+[ft]]
        dt['item']=ft
        newcol=['mine_id','mine_name']+['explain','item']
        dt.columns=newcol
        ncol=['mine_id','mine_name']+['item','explain']
        dt=dt[ncol]
        ll.append(dt)
    dfnew=pd.concat(ll,axis=0,join='inner')
    return dfnew

#计算最终总分
def pivot_table_data(deduce,model_score_raw):
    #计算每家煤矿扣的总分
    kf=deduce.groupby(['mine_id'])['explain'].sum().reset_index()
    dscore=model_score_raw.pivot_table(index=["mine_id"], columns=["POINT_NAME"],values="point_scorp").reset_index()
    dscore1=pd.merge(dscore,kf,on=['mine_id'],how='inner')
    dscore1['监察执法模型得分']=dscore1['综合得分']
    dscore1['newscore']=dscore1['综合得分']+dscore1['explain']
    dscore1['newscore']=dscore1['newscore'].apply(lambda x:0 if x<0 else x)
    dscore1['综合得分']=dscore1['newscore']
    dscore1.drop(columns=['explain','newscore'],inplace=True)
    #修改得分表的总分
    dscore1['point_id']='10000'
    dscore1['POINT_NAME']='综合得分'
    newdf=dscore1[['point_id','mine_id','POINT_NAME','综合得分']]
    newdf.columns=['point_id','mine_id','POINT_NAME','point_scorp']
    dfmb=model_score_raw[['mine_id','mine_name']].drop_duplicates()
    newdf1=pd.merge(newdf,dfmb,on=['mine_id'],how='inner')
    newdf1=newdf1[list(model_score_raw.columns)]
    #添加监察执法模型得分
    jczfmx=dscore1.copy()
    jczfmx['point_id']='111111'
    jczfmx['POINT_NAME']='监察执法模型得分'
    newf=jczfmx[['point_id','mine_id','POINT_NAME','监察执法模型得分']]
    newf.columns=['point_id','mine_id','POINT_NAME','point_scorp']
    newf1=pd.merge(newf,dfmb,on=['mine_id'],how='inner')
    newf1=newf1[list(model_score_raw.columns)]
    #更新原来的综合得分表
    model_score_filter=model_score_raw[model_score_raw['POINT_NAME']!='综合得分']
    model_score_end=pd.concat([model_score_filter,newdf1,newf1],axis=0,join='inner')
    return model_score_end

#文字解释和最终扣分表
def compute_explain(model_score,df_explain,deduce):
    model_scores=model_score.copy()
    model_scores['point_scorp']=model_scores['point_scorp'].apply(lambda x:'较差' if x<60 else '良好' if x>=80 else '一般')
    model_scores.rename(columns={'point_scorp':'level','POINT_NAME':'item'},inplace=True)
    df_explains=df_explain.copy()
    df_explains.rename(columns={'desyj':'explain'},inplace=True)
    model_scores['point_id']=model_scores['point_id'].astype('int64')
    df_explains['point_id']=df_explains['point_id'].astype('int64')
    #合并生成一级解释
    df=pd.merge(model_scores,df_explains,on=['point_id','level'],how='inner')
    df=df.groupby(['mine_id']).apply(lambda x: x.sort_values(['point_id'],ascending=True)).reset_index(drop=True).groupby(['mine_id']).head(1000)
    yiji_explain=df[['mine_id','mine_name','item','explain']]
    #实现模型整体文字说明
    def group_concat(df):
        df['desall'] = ''.join(list(df['desall']))
        return df.drop_duplicates()
    #计算模型整体文字说明
    dfdesall=df[['mine_id','mine_name','desall']]
    deduct=dfdesall.groupby(['mine_id','mine_name']).apply(group_concat)
    deduct['item']='默认模型说明'
    deduct.rename(columns={'desall':'explain'},inplace=True)
    deduct=deduct[['mine_id','mine_name','item','explain']]
    deduction=pd.concat([deduce,deduct],axis=0,join='inner')
    return yiji_explain,deduction

#UE展示原始值和得分合并一张表
def base_raw(model_raw_values,base_inf,model_detail_all):
    #关联三级指标得分表和原始数据取值表
    base_inf_raw_values=pd.merge(model_raw_values,base_inf,on=['point_id','POINT_NAME','mine_id','mine_name'],how="inner")
    #base_inf_raw_values=pd.concat([base_inf_raw_values,model_score],axis=0,join="outer")
    base_inf_raw_values=base_inf_raw_values[['mine_id','mine_name','point_id','POINT_NAME','raw_score','point_scorp']]
    model_det=model_detail_all[(model_detail_all['PROVINCE_NAME']=='安徽省')&(model_detail_all['MODEL_NAME']=='层次分析法')]
    model_det=model_det[['MODEL_PK','COMPUTED_RESULT']]
    model_det.columns=['point_id','COMPUTED_RESULT']
    base_inf_raw_values=pd.merge(base_inf_raw_values,model_det,on=['point_id'],how="inner")
    print(base_inf_raw_values.shape)
    return base_inf_raw_values

#得分比较
def compare(model_score,V_AJ1_MJ_JCXX_BASEINFO,model_name):
    #model_name '监察执法模型得分' '综合得分'
    zh_score=model_score[model_score['POINT_NAME']==model_name]
    dfall_class=V_AJ1_MJ_JCXX_BASEINFO[['CORP_NAME','MINE_CLASSIFY_NAME']]
    dfall_class.columns=['mine_name','MINE_CLASSIFY_NAME']
    merge=pd.merge(zh_score,dfall_class,on=['mine_name'],how='inner')
    merge.drop(columns=['point_id','POINT_NAME'],inplace=True)
    class_mean=merge.groupby(['MINE_CLASSIFY_NAME'])['point_scorp'].mean()
    return class_mean
