import pandas as pd
import numpy as np
data = {
    'Name': ['Alice', ' Bob ', 'Charlie', 'Alice', np.nan],
    'Age': ['25', 'thirty', 35, 25, 40],
    'Email': ['alice@example.com', 'bob@example.com', 'charlie@EXAMPLE.COM', 'alice@example.com', ''],
    'JoinDate': ['2022-01-10', '2022/01/12', '10-01-2022', '2022-01-10', '2022-02-30']
}
df = pd.DataFrame(data) #creating a dataframe

#drop rows with missing values
df_cleaned=df.dropna()
# fill missing values
df['Name']=df['Name'].fillna('Unknown')
# Convert Age to numeric, coerce errors to NaN
df['Age']=pd.to_numeric(df['Age'], errors='coerce')
# Remove leading/trailing whitespace from string columns
df['Name']=df['Name'].str.strip()
# Convert email to lowercase
df['Email']=df['Email'].str.lower()
# Convert to datetime, coerce invalid formats to NaT #coerce force conversion
df['JoinDate'] = pd.to_datetime(df['JoinDate'], errors='coerce')
#remove duplicates
df=df.drop_duplicates()
# Remove ages outside expected range
df=df[(df['Age']>=18)&(df['Age']<=100)]
print("Cleaned Data")
print(df)
