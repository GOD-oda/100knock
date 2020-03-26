import pandas as pd

# In [1]
uselog = pd.read_csv('use_log.csv')
# Out [1]
# print(len(uselog))
# print(uselog.head())

# In [2]
customer = pd.read_csv('customer_master.csv')
# Out [2]
# print(len(customer))
# print(customer.head())

# In [5]
class_master = pd.read_csv('class_master.csv')
# Out [5]
# print(len(class_master))
# print(class_master)

# In [6]
campaign_master = pd.read_csv('campaign_master.csv')
# Out [6]
# print(len(campaign_master))
# print(campaign_master.head())

# In [7]
customer_join = pd.merge(customer, class_master, on='class', how='left')
customer_join = pd.merge(customer_join, campaign_master, on='campaign_id', how='left')
# Out [7]
# print(customer_join.head())

# Out [8]
# print(len(customer))
# print(len(customer_join))

# Out [9]
# print(customer_join.isnull().sum())

# Out [8, 9, 10, 11]
# print(
#     customer_join.groupby('class_name').count()['customer_id'],
#     customer_join.groupby('campaign_name').count()['customer_id'],
#     customer_join.groupby('gender').count()['customer_id'],
#     customer_join.groupby('is_deleted').count()['customer_id']
# )

customer_join['start_date'] = pd.to_datetime(customer_join['start_date'])
customer_start = customer_join.loc[customer_join['start_date'] > pd.to_datetime('20180401')]
# print(len(customer_start))

# In [13]
customer_join['end_date'] = pd.to_datetime(customer_join['end_date'])
customer_newer = customer_join.loc[
    (customer_join['end_date'] >= pd.to_datetime('20190331'))
    | (customer_join['end_date'].isna())
]
# Out [13]
# print(len(customer_newer))
# print(customer_newer['end_date'].unique())

# Out [14, 15, 16, 17]
# print(
#     customer_newer.groupby('class_name').count()['customer_id'],
#     customer_newer.groupby('campaign_name').count()['customer_id'],
#     customer_newer.groupby('gender').count()['customer_id']
# )

# In [31]
uselog['usedate'] = pd.to_datetime(uselog['usedate'])
uselog['年月'] = uselog['usedate'].dt.strftime('%Y%m')
uselog_months = uselog.groupby(['年月', 'customer_id'], as_index=False).count()
uselog_months.rename(columns={'log_id':'count'}, inplace=True)
del uselog_months['usedate']
# Out [31]
# print(uselog_months.head())

# In [45]
uselog_customer = uselog_months.groupby('customer_id').agg(['mean', 'median', 'max', 'min'])['count']
uselog_customer = uselog_customer.reset_index(drop=False)
# Out [45]
# print(uselog_customer.head())

# In [85]
uselog['weekday'] = uselog['usedate'].dt.weekday
uselog_weekday = uselog.groupby(['customer_id', '年月', 'weekday'], as_index=False).count()[['customer_id', '年月', 'weekday', 'log_id']]
uselog_weekday.rename(columns={'log_id':'count'}, inplace=True)
# Out [85]
# print(uselog_weekday.head())

# In [112]
uselog_weekday = uselog_weekday.groupby('customer_id', as_index=False).max()[['customer_id', 'count']]
uselog_weekday['routine_flg'] = 0
uselog_weekday['routine_flg'] = uselog_weekday['routine_flg'].where(uselog_weekday['count'] < 4, 1)
# Out [112]
# print(uselog_weekday.head())

# In [21]
customer_join = pd.merge(customer_join, uselog_customer, on='customer_id', how='left')
customer_join = pd.merge(customer_join, uselog_weekday[['customer_id', 'routine_flg']], on='customer_id', how='left')
# Out [21]
# print(customer_join.head())
# print(customer_join.isnull().sum())

# In [24]
from dateutil.relativedelta import relativedelta
customer_join['calc_date'] = customer_join['end_date']
customer_join['calc_date'] = customer_join['calc_date'].fillna(pd.to_datetime('20190430'))
customer_join['membership_period'] = 0
for i in range(len(customer_join)):
    delta = relativedelta(customer_join['calc_date'].iloc[i], customer_join['start_date'].iloc[i])
    customer_join['membership_period'].iloc[i] = delta.years * 12 + delta.months
# Out [24]
# print(customer_join.head())

# print(customer_join.groupby('routine_flg').count()['customer_id'])

# TODO: 表示されない
# import matplotlib.pyplot as plt
# plt.hist(customer_join['membership_period'])
# plt.show()


# In [40]
customer_end = customer_join.loc[customer_join['is_deleted'] == 1]
# Out [40]
# print(customer_end)

# In [41]
customer_stay = customer_join.loc[customer_join['is_deleted'] == 0]
# Out [41]
# print(customer_stay.describe())

customer_join.to_csv('cutomer_join.csv', index=False)











