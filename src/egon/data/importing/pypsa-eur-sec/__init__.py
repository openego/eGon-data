"""The central module containing all code dealing with importing data from
the pysa-eur-sec scenario parameter creation
"""

import os
import pandas as pd
from egon.data import db
from egon.data.importing.nep_input_data import scenario_config
from sqlalchemy.ext.declarative import declarative_base

### will be later imported from another file ###
Base = declarative_base()


def pypsa_eur_sec_eGon100_capacities():
    """Inserts installed capacities for the eGon100 scenario

    Returns
    -------
    None.

    """

    # Connect to local database
    engine = db.engine()

    # Delete rows if already exist
    db.execute_sql("DELETE FROM supply.egon_scenario_capacities "
                   "WHERE scenario_name = 'eGon100'")

    # read-in installed capacities per federal state of germany
    target_file = os.path.join(
        os.path.dirname(__file__),
        scenario_config('eGon100')['paths']['capacities'])

    df = pd.read_csv(target_file)
    df.columns = ['component', 'country', 'carrier', 'capacity']
    df['scenario_name'] = 'eGon100'
    # geometry?    
    # erste Spalten rausnehmen
    # Ausland doch einfach direkt in die grid-tables schreiben?

    # Insert data to db
    df.to_sql('egon_scenario_capacities',
                       engine,
                       schema='supply',
                       if_exists='append',
                       index=df.index)

