'''
A Library that enables reading hive openings from a file.
Contains functions to consider those openings in excluding datetimes from data considerations.

Author: Cyril Monette
Initial date: 2025-07-23
'''

import pandas as pd

def build_openings_df():
    """
    Builds a DataFrame from the openings file.
    
    :return: DataFrame with columns ['start_opening', 'end_opening', 'hive_nb', 'comment'].
    """
    openings_file = "/Users/cyrilmonette/Library/CloudStorage/SynologyDrive-data/HiveOpenings/openings.txt"
    with open(openings_file, 'r') as file:
        openings = file.readlines()

    # Create a DataFrame from the openings, ignore lines that start with '#' or are empty
    openings_data = []
    for opening in openings:
        if opening.strip() and not opening.startswith('#'):
            parts = opening.strip().split(' ',3)
            if len(parts) == 3:
                date, time, hives = parts
                openings_data.append({'date': date.strip(), 'time': time.strip(), 'hives': hives.strip()})
            elif len(parts) == 4:
                date, time, hives, comment = parts
                openings_data.append({'date': date.strip(), 'time': time.strip(), 'hives': hives.strip(), 'comment': comment.strip()})

    # Convert the list of openings to a DataFrame
    openings_df = pd.DataFrame(openings_data)
    openings_df['start_opening'] = pd.to_datetime(openings_df['date'] + ' ' + openings_df['time'].str.split('-').str[0], format='%d/%m/%y %H:%M')
    openings_df['end_opening'] = pd.to_datetime(openings_df['date'] + ' ' + openings_df['time'].str.split('-').str[1], format='%d/%m/%y %H:%M')

    # Set tz to CET
    openings_df['start_opening'] = openings_df['start_opening'].dt.tz_localize('CET')
    openings_df['end_opening'] = openings_df['end_opening'].dt.tz_localize('CET')

    # Extract all digit characters individually from strings like 'h12' → ['1', '2']
    # Step 1: Extract the digits after 'h' (e.g., '12' from 'h12')
    openings_df['hive_nb'] = openings_df['hives'].str.extract(r'h(\d+)', expand=False)
    # Step 2: Split that string into a list of single characters
    openings_df['hive_nb'] = openings_df['hive_nb'].apply(lambda x: list(x) if pd.notnull(x) else [])
    # Step 3: Explode into one row per digit
    openings_df = openings_df.explode('hive_nb', ignore_index=True)
    openings_df['hive_nb'] = openings_df['hive_nb'].astype(int)

    # Remove the 'date' and 'time' columns as they are no longer needed
    openings_df.drop(columns=['date', 'time'], inplace=True)
    # Move comment to the end if it exists
    if 'comment' in openings_df.columns:
        openings_df = openings_df[['start_opening', 'end_opening', 'hive_nb', 'comment']]

    return openings_df

openings_df = build_openings_df()

def get_invalid_times(start_ts:pd.Timestamp, end_ts:pd.Timestamp, hive_nb:int, recovery_time:int=60):
    """
    Returns a list of invalid times for a given hive number.
    
    :param start_ts: Start timestamp for the period to consider.
    :param end_ts: End timestamp for the period to consider.
    :param hive_nb: Hive number.
    :param recovery_time: Expected recovery time in minutes for the hive to be back at normal behaviour(default is 60).
    :return: List of invalid times as pd.Timestamp objects.
    """

    # Make sure the timestamps are tz-aware
    assert start_ts.tzinfo is not None, "start_ts must be timezone-aware"
    assert end_ts.tzinfo is not None, "end_ts must be timezone-aware"

    # Filter openings for the given hive number
    filtered_openings = openings_df[openings_df['hive_nb'] == hive_nb] # Filter on hive
    filtered_openings = filtered_openings[(filtered_openings['start_opening'] < end_ts) & (filtered_openings['start_opening'] >= start_ts) |
                                          (filtered_openings['end_opening'] + pd.Timedelta(minutes=recovery_time) > start_ts) & (filtered_openings['end_opening'] + pd.Timedelta(minutes=recovery_time) <= end_ts) |
                                          (filtered_openings['start_opening'] <= start_ts) & (filtered_openings['end_opening'] + pd.Timedelta(minutes=recovery_time) >= end_ts)]

    # Create a list of invalid times
    invalid_times = []
    for _, row in filtered_openings.iterrows():
        start_exclusion = max(row['start_opening'], start_ts)
        end_exclusion = min(row['end_opening'] + pd.Timedelta(minutes=recovery_time), end_ts)
        # Create a dict with start and end
        dict_invalid = {
            'start': start_exclusion,
            'end': end_exclusion
        }
        invalid_times.append(dict_invalid)

    return invalid_times


def filter_timestamps(timestamps: list[pd.Timestamp], hive_nb: int, recovery_time: int = 60, verbose: bool = False):
    """
    Filters a list of timestamps to exclude those that fall within the invalid times for a given hive number.
    
    :param timestamps: List of pd.Timestamp objects to filter.
    :param hive_nb: Hive number to consider for filtering.
    :param recovery_time: Expected recovery time in minutes for the hive to be back at normal behaviour (default is 60).
    :return: List of valid pd.Timestamp objects.
    """

    # Make sure the timestamps are tz-aware
    for ts in timestamps:
        assert ts.tzinfo is not None, "All timestamps must be timezone-aware"

    timestamps = sorted(timestamps)
    
    start_ts = min(timestamps)
    end_ts = max(timestamps)

    invalid_times = get_invalid_times(start_ts, end_ts, hive_nb, recovery_time)

    if verbose:
        print(f"Invalid times for hive {hive_nb} from {start_ts} to {end_ts}:")
        for invalid in invalid_times:
            print(f"  - {invalid['start']} to {invalid['end']}")

    valid_timestamps = []
    for ts in timestamps:
        is_valid = True
        for invalid in invalid_times:
            if invalid['start'] <= ts <= invalid['end']:
                is_valid = False
                break
        if is_valid:
            valid_timestamps.append(ts)

    return valid_timestamps

def valid_ts(timestamp:pd.Timestamp, hive_nb:int, recovery_time:int=60):
    """
    Checks if a given timestamp is valid for a specific hive number.
    
    :param timestamp: pd.Timestamp object to check.
    :param hive_nb: Hive number to consider for validation.
    :param recovery_time: Expected recovery time in minutes for the hive to be back at normal behaviour (default is 60).
    :return: True if the timestamp is valid, False otherwise.
    """
    return len(filter_timestamps([timestamp], hive_nb, recovery_time)) > 0