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
    'bev_mini': {'column': 'mini',
                 'tech_share': 'bev_mini_share',
                 'share': 'mini_share',
                 'factor': 'mini_factor'},
    'bev_medium': {'column': 'medium',
                   'tech_share': 'bev_medium_share',
                   'share': 'medium_share',
                   'factor': 'medium_factor'},
    'bev_luxury': {'column': 'luxury',
                   'tech_share': 'bev_luxury_share',
                   'share': 'luxury_share',
                   'factor': 'luxury_factor'},
    'phev_mini': {'column': 'mini',
                  'tech_share': 'phev_mini_share',
                  'share': 'mini_share',
                  'factor': 'mini_factor'},
    'phev_medium': {'column': 'medium',
                    'tech_share': 'phev_medium_share',
                    'share': 'medium_share',
                    'factor': 'medium_factor'},
    'phev_luxury': {'column': 'luxury',
                    'tech_share': 'phev_luxury_share',
                    'share': 'luxury_share',
                    'factor': 'luxury_factor'},
}


def read_kba_data():
    """Read KBA data from CSV"""
    return pd.read_csv(
        DOWNLOAD_DIRECTORY /
        egon.data.config.datasets()[
            "emobility_mit"]["original_data"][
            "sources"]["KBA"]["file_processed"]
    )


def read_rs7_data():
    """Read RegioStaR7 data from CSV"""
    return pd.read_csv(
        DOWNLOAD_DIRECTORY /
        egon.data.config.datasets()[
            "emobility_mit"]["original_data"][
            "sources"]["RS7"]["file_processed"]
    )
