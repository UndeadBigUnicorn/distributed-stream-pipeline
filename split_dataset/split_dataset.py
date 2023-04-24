#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Split the dataset into training and validation sets.

Usage: split_dataset.py
"""
import os
import pandas as pd
from sklearn.model_selection import train_test_split

if __name__ == "__main__":
    print("Reading the CSV...")
    df = pd.read_csv("../data/tripadvisor_hotel_reviews.csv")
    print("Splitting the CSV...")
    train, test = train_test_split(df, test_size=0.2)
    print("Saving new CSVs...")
    train.to_csv("../data/train_data.csv", sep=',', encoding='utf-8', index=False)
    test.to_csv("../data/test_data.csv", sep=',', encoding='utf-8', index=False)

