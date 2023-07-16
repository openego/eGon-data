import pandas as pd
import geopandas as gpd
import psycopg2



lines_df = pd.read_csv("/home/student/Documents/Powerd/egon_etrago_line_new.csv")
buildyear_df = pd.read_csv("/home/student/Documents/Powerd/NEP_tables_finalVersion.csv")
buildyear_df['anvisierte Inbetriebnahme'] = buildyear_df["anvisierte Inbetriebnahme"].fillna('').str.split(r'[,/-]').apply(lambda x: x[1].strip() if len(x) > 1 else x[0].strip())
#buildyear_df ['anvisierte Inbetriebnahme']=buildyear_df["anvisierte Inbetriebnahme"].str.split(',').str[1].str.strip()
buildyear_df.to_csv('/home/student/Documents/Powerd/buildyear_test.csv', index=False)
for index1, row1 in lines_df.iterrows():
    scn_name1 = str(row1['scn_name'])
    for index2, row2 in buildyear_df.iterrows():
        scn_name2 = str(row2['scn_name'])
        if scn_name1 == scn_name2:
            lines_df.loc[index1, 'build_year'] = row2['anvisierte Inbetriebnahme']

#merged_df = pd.merge(lines_df, buildyear_df, on='scn_name', how='inner')

#buildyear_df_filtered = buildyear_df[buildyear_df['scn_name'].isin(lines_df['scn_name'])]

# Auswahl der relevanten Spalten
#merged_df = merged_df[['scn_name', 'anvisierte Inbetriebnahme']]

#print('länge von merge:'+{len(merged_df)}+ ',länge von buildyear:' + len(buildyear_df))

# Iteration über die Zeilen des merged_df DataFrames
#for index, row in merged_df.iterrows():
 #   lines_df['build_year'] = merged_df['anvisierte Inbetriebnahme']


#for index, row in lines_df.iterrows():
 #   if lines_df.loc[index,'scn_name'] == buildyear_df_filtered.loc[index,'scn_name']:
  #      lines_df.loc[index,'build_year'] = buildyear_df_filtered.loc[index,'anvisierte Inbetriebnahme']

lines_df.to_csv('/home/student/Documents/Powerd/egon_etrago_line_new.csv', index=False)

print('operation succesfull')