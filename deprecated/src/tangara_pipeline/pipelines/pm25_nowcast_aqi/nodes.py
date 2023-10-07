"""
This is a boilerplate pipeline 'pm25_nowcast_aqi'
generated using Kedro 0.18.1
"""

import math
import pandas as pd
import numpy as np
import sys


#
# Please check the Notebook pm25_nowcast_aqi.ipynb to refer information about these values
#

# Levels of Concern
GOOD = 0
MODERATE = 1
UNHEALTHY_FOR_SENSITIVE_GROUPS = 2
UNHEALTHY = 3
VERY_UNHEALTHY = 4
HAZARDOUS = 5
# AQI Levels
AQI_LEVELS = (
    (GOOD, 'Good', [0, 50]), # 0 - 50
    (MODERATE, 'Moderate', [51, 100]), # 51 - 100
    (UNHEALTHY_FOR_SENSITIVE_GROUPS, 'Unhealthy for sensitive groups', [101, 150]), # 101 - 150
    (UNHEALTHY, 'Unhealthy', [151, 200]), # 151 - 200
    (VERY_UNHEALTHY, 'Very Unhealthy', [201, 300]), # 201 - 300
    (HAZARDOUS, 'Hazardous', [301, sys.maxsize]), # 301 - higher
)
# PM25 Breakpoints Values
PM25_BREAKPOINTS = (
    (GOOD, 'Good', [0.0, 12.0]), # 0.0 - 12.0, Good
    (MODERATE, 'Moderate', [12.1, 35.4]), # 12.1 - 35.4, Moderate
    (UNHEALTHY_FOR_SENSITIVE_GROUPS, 'Unhealthy for sensitive groups', [35.5, 55.4]), # 35.5 - 55.4, Unhealthy for sensitive groups
    (UNHEALTHY, 'Unhealthy', [55.5, 150.4]), # 55.5 - 150.4, Unhealthy
    (VERY_UNHEALTHY, 'Very Unhealthy', [150.5, 250.4]), # 150.5 - 250.4, Very Unhealthy
    (HAZARDOUS, 'Hazardous', [250.5, 350.4]), # 250.5 - 350.4, Hazardous
    (HAZARDOUS, 'Hazardous', [350.5, 500.4]), # 350.5 - 500.4, Hazardous
)
# Max PM25 Value
MAX_PM25_VALUE = 500.5


# Define Equation 1 to Calculate AQI Value
#
# AQI = AQI for pollutant p
# Cp = The truncated concentrattion of pollutant p
# BP_hi = The concentration breakpoint that is greater than or equal to Cp
# BP_lo = The concentration breakpoint that is less than or equal to Cp
# AQI_hi = The AQI value corresponding to BP_hi
# AQI_lo = The AQI value corresponding to BP_lo
#
def equation1(Cp):
    # Truncate Cp
    Cp = float(f'{Cp:.1f}')
    # AQI Value to Concentration of Pollutant P
    AQI = None
    # Variables
    BP_hi = None
    BP_lo = None
    AQI_hi = None
    AQI_lo = None

    # Get BP_hi and BP_lo
    for bp in PM25_BREAKPOINTS:
        #print('bp:', bp)
        if (Cp >= bp[2][0]) and (Cp <= bp[2][-1]):
            BP_hi = bp[2][-1]
            BP_lo = bp[2][0]
            AQI_hi = AQI_LEVELS[bp[0]][2][-1]
            AQI_lo = AQI_LEVELS[bp[0]][2][0]
            break

    #print('Cp:', Cp)
    #print('Cp:', Cp, 'BP_hi:', BP_hi, 'BP_lo:', BP_lo, 'AQI_hi:', AQI_hi, 'AQI_lo:', AQI_lo, 'AQI:', AQI)
    AQI = Cp if math.isnan(Cp) else int(math.ceil((((AQI_hi - AQI_lo) / (BP_hi - BP_lo)) * (Cp - BP_lo)) + AQI_lo))
    #print('Cp:', Cp, 'BP_hi:', BP_hi, 'BP_lo:', BP_lo, 'AQI_hi:', AQI_hi, 'AQI_lo:', AQI_lo, 'AQI:', AQI)
    #print('Cp:', Cp, 'AQI:', AQI)

    return AQI


# Apply using equation1 for each PM25 value
def pm25_to_aqi(pm25_sensors):
    aqi_values = pm25_sensors.copy()
    for column in aqi_values.columns:
        if column != 'DATETIME':
            aqi_values[column] = aqi_values[column].apply(lambda x: equation1(np.float64(x)))
            aqi_values[column] = aqi_values[column].astype('Int64')
    return aqi_values


def aqi_instant(pm25_clean: pd.DataFrame) -> pd.DataFrame:
    """
        Process the PM25 values to calculate the AQI instant values using the 'Equation1'
        Please check the Notebook pm25_nowcast_aqi.ipynb to refer information about these values
    Args:
        pm25_clean: clean data for registered Tangara sensors without outliers
    Returns:
        aqi_instant: AQI instant values for registered Tangara sensors
    """
    # Data Frame Sensors
    return pm25_to_aqi(pm25_clean)


def aqi_last_hour(pm25_last_hour: pd.DataFrame) -> pd.DataFrame:
    """
        Process the PM25 last hour values to calculate the AQI last hour values using the 'Equation1'
        Please check the Notebook pm25_nowcast_aqi.ipynb to refer information about these values
    Args:
        pm25_last_hour: PM25 last hour values for registered Tangara sensors
    Returns:
        aqi_last_hour: AQI last hour values for registered Tangara sensors
    """
    # Data Frame Sensors
    return pm25_to_aqi(pm25_last_hour)


def aqi_last_8h(pm25_last_8h: pd.DataFrame) -> pd.DataFrame:
    """
        Process the PM25 last 8 hours values to calculate the AQI last 8 hours values using the 'Equation1'
        Please check the Notebook pm25_nowcast_aqi.ipynb to refer information about these values
    Args:
        pm25_last_8h: PM25 last 8 hours values for registered Tangara sensors
    Returns:
        aqi_last_8h: AQI last 8 hours values for registered Tangara sensors
    """
    # Data Frame Sensors
    return pm25_to_aqi(pm25_last_8h)


def aqi_last_12h(pm25_last_12h: pd.DataFrame) -> pd.DataFrame:
    """
        Process the PM25 last 12 hours values to calculate the AQI last 12 hours values using the 'Equation1'
        Please check the Notebook pm25_nowcast_aqi.ipynb to refer information about these values
    Args:
        pm25_last_12h: PM25 last 12 hours values for registered Tangara sensors
    Returns:
        aqi_last_12h: AQI last 12 hours values for registered Tangara sensors
    """
    # Data Frame Sensors
    return pm25_to_aqi(pm25_last_12h)


def aqi_last_24h(pm25_last_24h: pd.DataFrame) -> pd.DataFrame:
    """
        Process the PM25 last 24 hours values to calculate the AQI last 24 hours values using the 'Equation1'
        Please check the Notebook pm25_nowcast_aqi.ipynb to refer information about these values
    Args:
        pm25_last_24h: PM25 last 24 hours values for registered Tangara sensors
    Returns:
        aqi_last_24h: AQI last 24 hours values for registered Tangara sensors
    """
    # Data Frame Sensors
    return pm25_to_aqi(pm25_last_24h)
