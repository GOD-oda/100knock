import pandas as pd

uriage_data = pd.read_csv('uriage.csv')
# print(uriage_data.head())

kokyaku_data = pd.read_excel('kokyaku_daicho.xlsx')
# TODO
# print(kokyaku_data.head())

# print(uriage_data["item_name"].head())
# print(uriage_data["item_price"].head())

uriage_data["purchase_data"] = pd.to_datetime(uriage_data["purchase_date"])
uriage_data["purchase_month"] = uriage_data["purchase_data"].dt.strftime("%Y%m")
res = uriage_data.pivot_table(index="purchase_month", columns="item_name", aggfunc="size", fill_value=0)
# print(res)

res = uriage_data.pivot_table(index="purchase_month", columns="item_name", values="item_price", aggfunc="sum", fill_value=0)
# print(res)

# print(len(pd.unique(uriage_data.item_name)))
uriage_data["item_name"] = uriage_data["item_name"].str.upper()
uriage_data["item_name"] = uriage_data["item_name"].str.replace("　", "")
uriage_data["item_name"] = uriage_data["item_name"].str.replace(" ", "")
# print(uriage_data.sort_values(by=["item_name"], ascending=True))
# print(pd.unique(uriage_data["item_name"]))
# print(len(pd.unique(uriage_data["item_name"])))

# print(uriage_data.isnull().any(axis=0))

# In [12]
flg_is_null = uriage_data["item_price"].isnull()
for trg in list(uriage_data.loc[flg_is_null, "item_name"].unique()):
    price = uriage_data.loc[(~flg_is_null) & (uriage_data["item_name"] == trg), "item_price"].max()
    # TODO: Warning: A value is trying to be set on a copy of a slice from a DataFrame
    uriage_data["item_price"].loc[(flg_is_null) & (uriage_data["item_name"] == trg)] = price
# Out [12]
# print(uriage_data.head())

# Out [13]
# print(uriage_data.isnull().any(axis=0))

# Out [14]
# for trg in list(uriage_data["item_name"].sort_values().unique()):
    # print(
    #     trg + "の最大額：" + str(uriage_data.loc[uriage_data["item_name"] == trg]["item_price"].max())
    #     + ", 最小額：" + str(uriage_data.loc[uriage_data["item_name"] == trg]["item_price"].min(skipna=False))
    # )

# Out [15]
# print(kokyaku_data["顧客名"].head())

# Out [16]
# print(uriage_data["customer_name"].head())

# In [17]
kokyaku_data["顧客名"] = kokyaku_data["顧客名"].str.replace("　", "")
kokyaku_data["顧客名"] = kokyaku_data["顧客名"].str.replace(" ", "")
# Out [17]
# print(kokyaku_data["顧客名"].head())

# In [18]
flg_is_serial = kokyaku_data["登録日"].astype("str").str.isdigit()
# Out [18]
# print(flg_is_serial.sum())

# In [19]
fromSerial = pd.to_timedelta(
    kokyaku_data.loc[flg_is_serial, "登録日"].astype("float"),
    unit="D"
) + pd.to_datetime("1900/01/01")
# Out [19]
# print(fromSerial)

# In [20]
fromString = pd.to_datetime(kokyaku_data.loc[~flg_is_serial, "登録日"])
# Out [20]
# print(fromString)

# In [21]
kokyaku_data["登録日"] = pd.concat([fromSerial, fromString])
# Out [21]
# print(kokyaku_data)

# In [22]
kokyaku_data['登録年月'] = kokyaku_data['登録日'].dt.strftime('%Y%m')
rslt = kokyaku_data.groupby('登録年月').count()['顧客名']
# Out [22]
# print(rslt)
# print(len(kokyaku_data))

# In [23]
flg_is_serial = kokyaku_data['登録日'].astype('str').str.isdigit()
# Out [23]
# print(flg_is_serial.sum())

# In [24]
join_data = pd.merge(uriage_data, kokyaku_data, left_on='customer_name', right_on='顧客名', how='left')
join_data = join_data.drop('customer_name', axis=1)
# Out [24]
# print(join_data)

# In [25]
dump_data = join_data[[
    'purchase_date',
    'purchase_month',
    'item_name',
    'item_price',
    '顧客名',
    'かな',
    '地域',
    'メールアドレス',
    '登録日',
]]
# Out [25]
# print(dump_data)

dump_data.to_csv('dump_data.csv', index=False)

# In [26]
import_data = pd.read_csv('dump_data.csv')
# Out [26]
# print(import_data)

# In [27]
byItem = import_data.pivot_table(index='purchase_month', columns='item_name', aggfunc='size', fill_value=0)
# Out [27]
# print(byItem)

# In [28]
byPrice = import_data.pivot_table(index='purchase_month', columns='item_name', values='item_price', aggfunc='sum', fill_value=0)
# Out [28]
# print(byPrice)

# In [29]
byCustomer = import_data.pivot_table(index='purchase_month', columns='顧客名', aggfunc='size', fill_value=0)
# Out [29]
# print(byCustomer)

# In [30]
byRegion = import_data.pivot_table(index='purchase_month', columns='地域', aggfunc='size', fill_value=0)
# Out [30]
# print(byRegion)

# In [31]
away_data = pd.merge(uriage_data, kokyaku_data, left_on='customer_name', right_on='顧客名', how='right')
# Out [31]
print(
    away_data[away_data['purchase_date'].isnull()][['顧客名', 'メールアドレス', '登録日']]
)

