import pandas as pd
from sqlalchemy import create_engine

# Connection to PostgreSQL-Database
engine = create_engine(
    f"postgresql+psycopg2://postgres:"
    f"postgres@localhost:"
    f"5432/etrago",
    echo=False,
)

csv_file = '/home/student/Documents/Powerd/egon_etrago_line_new.csv'
NEP_lines_df = pd.read_csv(csv_file)



#Query for the highest line_id
table_name = 'grid.egon_etrago_line'
column_name = 'line_id'

query = f"SELECT MAX({column_name}) FROM {table_name};"

result = pd.read_sql(query, engine)
highest_id=result.iloc[0, 0]

print(f"Die höchste Zahl in der Spalte '{column_name}' ist: {highest_id}")



#new line_ids into csv-table
NEP_lines_df['line_id'] = range(highest_id, highest_id + len(NEP_lines_df))
NEP_lines_df.to_csv('/home/student/Documents/Powerd/egon_etrago_line_new.csv', index=False)
print('Column line_id was updated')




# CSV-File into new SQL-Database
csv_file = '/home/student/Documents/Powerd/egon_etrago_line_new.csv'
NEP_lines_df = pd.read_csv(csv_file)

table_name = 'New_NEP_lines'
schema_name='grid'
NEP_lines_df.to_sql(table_name, engine, schema=schema_name, if_exists='replace', index=False)

engine.dispose()

print("CSV-Datei wurde erfolgreich in eine neue PostgreSQL-Tabelle geladen.")




#CSV-File into existing SQL-Database
#table_name = 'egon_etrago_line'
#schema_name='grid'
#NEP_lines_df.to_sql(table_name, engine, schema=schema_name, if_exists='append', index=False)
