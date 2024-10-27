import pandas as pd

# 创建一个示例 DataFrame
data = {
    'Name': ['Alice', 'Bob', 'Charlie'],
    'Age': [24, 30, 22],
    'City': ['New York', 'Los Angeles', 'Chicago']
}
df = pd.DataFrame(data)

# 写入到 xlsx 文件
df.to_excel('output.xlsx', index=False)
