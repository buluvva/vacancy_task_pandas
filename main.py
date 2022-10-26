import pandas as pd
from datetime import timedelta
from uuid import uuid4

def get_groups():
    df = pd.read_csv('TEST_DATASET.csv', delimiter=',')
    gb = df.groupby('customer_id')
    # session_id = uuid4()
    groups = []
    for name in gb.groups:
        df = gb.get_group(name).sort_values('timestamp', ascending=True).reset_index(drop=True)
        groups.append(sort_sessions(df))
    df = pd.concat(groups).reset_index(drop=True)
    return df



def sort_sessions(group: pd.DataFrame) -> pd.DataFrame:
    timestamps = pd.to_datetime(group['timestamp'].tolist())
    session_id = uuid4()
    for i in range(len(group)):
        try:
            if (timestamps[i+1] - timestamps[i]) < timedelta(minutes=3):
                group.at[i, 'session_id'] = session_id
                group.at[i + 1, 'session_id'] = session_id
            else:
                session_id = uuid4()
                group.at[i, 'session_id'] = session_id
        except IndexError:
            group.at[i, 'session_id'] = session_id
    return group

df = get_groups()





