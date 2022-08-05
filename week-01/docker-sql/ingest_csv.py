#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine


def main():
    

    engine = create_engine(f'postgresql://root:root@localhost:5431/ny_taxi')
    
    df = pd.read_csv("/Users/arturomartinez/Desktop/taxi_zone_lookup.csv", low_memory=False)
    df.to_sql(name="taxi_zones", con=engine, if_exists='append', chunksize=100000, index=False)


if __name__ == '__main__':

    main()