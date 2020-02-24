import pandas as pd

customer_master = pd.read_csv('customer_master.csv')
# print(customer_master.head())

item_master = pd.read_csv('item_master.csv')
# print(item_master.head())

transaction_1 = pd.read_csv('transaction_1.csv')
# print(transaction_1.head())

transaction_detail_1 = pd.read_csv('transaction_detail_1.csv')
# print(transaction_detail_1.head())

transaction_2 = pd.read_csv('transaction_2.csv')
# print(transaction_2.head())

transaction_detail_2 = pd.read_csv('transaction_detail_2.csv')
# print(transaction_detail_2.head())

transaction = pd.concat([transaction_1, transaction_2], ignore_index=True)
# print(transaction.head())

# print(len(transaction_1))
# print(len(transaction_2))
# print(len(transaction))

transaction_detail = pd.concat([transaction_detail_1, transaction_detail_2], ignore_index=True)
# print(transaction_detail.head())

# print(len(transaction_detail_1))
# print(len(transaction_detail_2))
# print(len(transaction_detail))

join_data = pd.merge(transaction_detail, transaction[["transaction_id", "payment_date", "customer_id"]], on="transaction_id", how="left")
# print(join_data.head())
# print(len(join_data))

# print(len(transaction_detail))
# print(len(transaction))
# print(len(join_data))

join_data = pd.merge(join_data, customer_master, on="customer_id", how="left")
join_data = pd.merge(join_data, item_master, on="item_id", how="left")
# print(join_data.head())

join_data["price"] = join_data["quantity"] * join_data["item_price"]
# print(join_data[["quantity", "item_price", "price"]].head())

# print(join_data["price"].sum())
# print(transaction["price"].sum())
# print(join_data["price"].sum() == transaction["price"].sum())

# print(join_data.isnull().sum())
# print(join_data.describe())

# print(join_data["payment_date"].min())
# print(join_data["payment_date"].max())

# print(join_data.dtypes)

join_data["payment_date"] = pd.to_datetime(join_data["payment_date"])
join_data["payment_month"] = join_data["payment_date"].dt.strftime("%Y%m")
# print(join_data[["payment_date", "payment_month"]].head())

# print(join_data.groupby("payment_month").sum()["price"])

# print(join_data.groupby(["payment_month", "item_name"]).sum()[["price", "quantity"]])

# print(pd.pivot_table(join_data, index="item_name", columns="payment_month", values=["price", "quantity"], aggfunc="sum"))

graph_data = pd.pivot_table(join_data, index="payment_month", columns="item_name", values="price", aggfunc="sum")
# print(graph_data.head())

import matplotlib.pyplot as plt

plt.plot(list(graph_data.index), graph_data["PC-A"], label="PC-A")
plt.plot(list(graph_data.index), graph_data["PC-B"], label="PC-B")
plt.plot(list(graph_data.index), graph_data["PC-C"], label="PC-C")
plt.plot(list(graph_data.index), graph_data["PC-D"], label="PC-D")
plt.plot(list(graph_data.index), graph_data["PC-E"], label="PC-E")
plt.legend()

# TODO: 表示できるらしい
# plt.show()