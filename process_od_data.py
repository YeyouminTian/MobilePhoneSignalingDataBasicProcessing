import pandas as pd
import numpy as np

# 读取CSV文件
df = pd.read_csv(r"D:\LifeOS\1. 项目\实验室-时空数据分析\汇总提交\南昌时空数据分析\nc_3_3_user_work_home_location.csv")

# 计算总行数
total_rows = len(df)

# 创建一个生成器函数来分批处理数据
def batch_cross_merge(df, batch_size=1000):
    for i in range(0, total_rows, batch_size):
        batch = df.iloc[i:i+batch_size]
        yield batch.merge(df, how='cross')

# 初始化结果DataFrame
result = pd.DataFrame(columns=['HOME_LONGITUDE', 'HOME_LATITUDE', 'WORK_LONGITUDE', 'WORK_LATITUDE', 'FLOW_COUNT'])

# 使用生成器处理数据
for batch in batch_cross_merge(df):
    # 过滤掉相同的起始点和终点
    batch = batch[
        (batch['WORK_LONGITUDE_x'] != batch['WORK_LONGITUDE_y']) | 
        (batch['WORK_LATITUDE_x'] != batch['WORK_LATITUDE_y'])
    ]
    
    # 计算流量
    batch['FLOW_COUNT'] = 1
    
    # 重命名列
    batch = batch.rename(columns={
        'HOME_LONGITUDE_x': 'HOME_LONGITUDE',
        'HOME_LATITUDE_x': 'HOME_LATITUDE',
        'WORK_LONGITUDE_y': 'WORK_LONGITUDE',
        'WORK_LATITUDE_y': 'WORK_LATITUDE'
    })
    
    # 选择需要的列并分组计算流量
    batch = batch.groupby(['HOME_LONGITUDE', 'HOME_LATITUDE', 'WORK_LONGITUDE', 'WORK_LATITUDE'])['FLOW_COUNT'].sum().reset_index()
    
    # 将结果添加到最终结果中
    result = pd.concat([result, batch], ignore_index=True)

# 合并最终结果
result = result.groupby(['HOME_LONGITUDE', 'HOME_LATITUDE', 'WORK_LONGITUDE', 'WORK_LATITUDE'])['FLOW_COUNT'].sum().reset_index()

# 按FLOW_COUNT降序排序并显示前10条记录
print("Top 10 OD flows:")
print(result.sort_values('FLOW_COUNT', ascending=False).head(10))

# 统计总流动量
total_flow = result['FLOW_COUNT'].sum()
print(f"\nTotal flow: {total_flow}")

# 统计不同的起始点和终点数量
unique_origins = result[['HOME_LONGITUDE', 'HOME_LATITUDE']].drop_duplicates().shape[0]
unique_destinations = result[['WORK_LONGITUDE', 'WORK_LATITUDE']].drop_duplicates().shape[0]
print(f"\nUnique origins: {unique_origins}")
print(f"Unique destinations: {unique_destinations}")

# 保存结果到CSV文件
result.to_csv('nc_3_5_od_flow.csv', index=False)
print("\nResults saved to nc_3_5_od_flow.csv")