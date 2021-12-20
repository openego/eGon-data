import pandas as pd
from pathlib import Path
import egon.data.config

DOWNLOAD_DIRECTORY = Path(".") / "motorized_individual_travel"
COLUMNS_KBA = [
    'reg_district',
    'total',
    'mini',
    'medium',
    'luxury',
    'unknown',
]
CONFIG_EV = {
    'BEV_mini': {'column': 'mini',
                 'tech_share': 'BEV_mini_share',
                 'share': 'mini_share',
                 'factor': 'mini_factor'},
    'BEV_medium': {'column': 'medium',
                   'tech_share': 'BEV_medium_share',
                   'share': 'medium_share',
                   'factor': 'medium_factor'},
    'BEV_luxury': {'column': 'luxury',
                   'tech_share': 'BEV_luxury_share',
                   'share': 'luxury_share',
                   'factor': 'luxury_factor'},
    'PHEV_mini': {'column': 'mini',
                  'tech_share': 'PHEV_mini_share',
                  'share': 'mini_share',
                  'factor': 'mini_factor'},
    'PHEV_medium': {'column': 'medium',
                    'tech_share': 'PHEV_medium_share',
                    'share': 'medium_share',
                    'factor': 'medium_factor'},
    'PHEV_luxury': {'column': 'luxury',
                    'tech_share': 'PHEV_luxury_share',
                    'share': 'luxury_share',
                    'factor': 'luxury_factor'},
}


def read_kba_data():
    """Read KBA data from CSV"""
    return pd.from_csv(
        DOWNLOAD_DIRECTORY /
        egon.data.config.datasets()[
            "emobility_mit"]["original_data"][
            "sources"]["KBA"]["file_processed"]
    )


def read_rs7_data():
    """Read RegioStaR7 data from CSV"""
    return pd.from_csv(
        DOWNLOAD_DIRECTORY /
        egon.data.config.datasets()[
            "emobility_mit"]["original_data"][
            "sources"]["RS7"]["file_processed"]
    )
