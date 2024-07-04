from deltalake import DeltaTable
import sqlite3
from azure.identity import ClientSecretCredential

# store client and key in Key Vault
TENANT_ID = 'Tenant_ID'
CLIENT = 'App_ID'
KEY = 'App_Key'


credential = ClientSecretCredential(tenant_id=TENANT_ID, client_id=CLIENT, client_secret-KEY)
token = credential.get_token("https://storage.azure.com/.default").token

delta_table_path = f"abfss:// ABFSS path to lakehouse table"
storage options = {"bearer_token": token}
dt = DeltaTable(delta_table_path, storage_options=storage_options)

#Read data
df = dt.to_pyarrow_table().to_pandas()
print(dt.schema().to_pyarrow())
print(df)

#Write data to an external db (sqlite is just an example you can use whatever db you need)
conn = sqlite3.connect('movie.db')
df.to_sql('movie', conn, if_exists='replace', index=False)
conn.close()
