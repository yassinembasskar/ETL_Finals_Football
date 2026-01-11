import pandas as pd
from transform.bs4_utils import extract_team_and_formation
import re

def integer(number):
    try:
        n = int(number)
    except:
        n = 0
    return n

def extract_possession(text):
    """Extract possession percentage as decimal"""
    if not text or pd.isna(text):
        return None
    match = re.search(r'(\d+)%', text)
    if match:
        return int(match.group(1)) / 100
    return None


def extract_passes(text, team):
    """Extract total passes and accurate passes"""
    if not text or pd.isna(text):
        return None, None
    
    if team == 'first':
        match = re.search(r'(\d+)\s+of\s+(\d+)', text)
    else:
        match = re.search(r'(\d+)\s+of\s+(\d+)', text)
    
    if match:
        accurate = int(match.group(1))
        total = int(match.group(2))
        return total, accurate
    return None, None


def extract_shots(text, team):
    """Extract total shots and shots on target"""
    if not text or pd.isna(text):
        return None, None
    
    if team == 'first':
        # Format: "9 of 24 — 38%"
        match = re.search(r'(\d+)\s+of\s+(\d+)', text)
    else:
        # Format: "25% — 1 of 4"
        match = re.search(r'(\d+)\s+of\s+(\d+)', text)
    
    if match:
        on_target = int(match.group(1))
        total = int(match.group(2))
        return total, on_target
    return None, None


def extract_saves(text, team):
    """Extract saves"""
    if not text or pd.isna(text):
        return None
    
    if team == 'first':
        # Format: "0 of 1 — 0%"
        match = re.search(r'(\d+)\s+of\s+(\d+)', text)
    else:
        # Format: "100% — 9 of 9"
        match = re.search(r'(\d+)\s+of\s+(\d+)', text)
    
    if match:
        saves = int(match.group(1))
        return saves
    return None



def clean_lineup_tables(table, order):
    first_column_name = table.columns[0]
    second_column_name = table.columns[1]
    team_formation = extract_team_and_formation(first_column_name)
    team = pd.DataFrame([{'team_name': team_formation[0], 'formation': team_formation[1]}])
    starters = pd.DataFrame([{"#": row[first_column_name], "Name": row[second_column_name]} for index, row in table.iterrows() if index < 11])
    bench = pd.DataFrame([{"#": row[first_column_name], "Name": row[second_column_name]} for index, row in table.iterrows() if index > 11])
    return [team, starters, bench]     



def clean_match_summary_table(table, order):
    column = table.columns[0] if order == "first" else table.columns[1]
    result = {}
    length = len(table)
    result["Possession"] = extract_possession(table.iloc[0][column]) if length > 0 else 0
    result["Total Passes"], result["Accurate Passes"] = extract_passes(table.iloc[2][column], order) if length > 2 else [0, 0]
    result["Total Shots"], result["Shots on Target"] = extract_shots(table.iloc[4][column], order) if length > 4 else [0, 0]
    result["Saves"] = extract_saves(table.iloc[6][column], order) if length > 6 else 0
    
    return pd.DataFrame([result])

def clean_minute(minute_str):
    if minute_str is None:
        return None
    match = re.search(r'(\d+)(?:\+(\d+))?', minute_str)
    if match:
        base_minute = int(match.group(1))
        added_time = int(match.group(2)) if match.group(2) else 0
        return base_minute + added_time
    return None