from matplotlib.pylab import f
import pyhdfs
import pandas as pd
from collections import defaultdict

def get_recommendations(item_name, hdfs_path):
    # host = 'your_hdfs_host'
    # client = pyhdfs.HdfsClient(hosts=host)

    # with client.read(hdfs_path) as reader:
        # data = reader.read().decode('utf-8')
    # data = str(open(hdfs_path, 'r'))
    sells_df = pd.read_csv(hdfs_path)

    item_pairs = []

    for index, row in sells_df.iterrows():
        item1, item2, count = row['item1'], row['item2'], row['count']
        item_pairs.append(f'{item1},{item2},{count}')

    print(item_pairs)
    co_occurrences = defaultdict(int)
    
    #lines = data.splitlines()
    for line in item_pairs:
        item1, item2, count = line.split(',')
        count = int(count)
        if item1 == item_name:
            co_occurrences[item2] += count
        elif item2 == item_name:
            co_occurrences[item1] += count

    recommendations = sorted(co_occurrences.items(), key=lambda x: x[1], reverse=True)

    for item, count in recommendations[:10]:
        print(f"Рекоммендуем товар с номером: {item}, количество покупок вместе с {item_name}: {count}")

item_name = 'a1'
hdfs_path = 'cross_correlation_results.csv'
get_recommendations(item_name, hdfs_path)