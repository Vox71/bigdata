import pandas as pd
from itertools import combinations
from collections import Counter

# Загружаем данные из HDFS (предполагая, что вы используете PySpark или аналогичный инструмент для этой задачи)
sells_df = pd.read_csv('sells.csv')

grouped = sells_df.groupby('id')['cart'].apply(list).reset_index()

all_pairs = []

for items in grouped['cart']:
    pairs = combinations(sorted(set(items)), 2) 
    all_pairs.extend(pairs)

pair_counts = Counter(all_pairs)

result_df = pd.DataFrame(pair_counts.items(), columns=['item_pair', 'count'])
result_df[['item1', 'item2']] = pd.DataFrame(result_df['item_pair'].tolist(), index=result_df.index)
result_df = result_df[['item1', 'item2', 'count']]

# Сохраняем результат в новый CSV файл
result_df.to_csv('cross_correlation_results.csv', index=False)