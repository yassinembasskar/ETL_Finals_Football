import pandas as pd
from utils.logging_setup import setup_logger

logger = setup_logger("transform", "logs/transform.log")

def flatten_columns(df):
    """Flatten multi-index columns to single level"""
    df = df.copy()
    df.columns = ['_'.join(col).strip('_') if isinstance(col, tuple) else col 
                  for col in df.columns]
    return df


def get_player_stats(stats_df, player_name, use_readable_names=True):
    """
    Extract all statistics for a specific player from the merged stats dataframe.
    
    Parameters:
    -----------
    stats_df : DataFrame
        Merged dataframe containing all player statistics
        Can have MultiIndex columns or flattened columns
    player_name : str
        Name of the player to search for
    use_readable_names : bool
        If True, converts abbreviated column names to readable names
    
    Returns:
    --------
    dict
        Dictionary with player statistics
        Returns empty dict if player not found
    """
    
    if isinstance(stats_df.columns, pd.MultiIndex):
        stats_df = flatten_columns(stats_df)
    
    player_col = None
    for col in stats_df.columns:
        if '0_level_0' in col:
            player_col = col
            break
    
    if player_col is None:
        logger.error("Error: No player column found in dataframe", player_col)
        return {}
    
    player_row = stats_df[stats_df[player_col].str.contains(player_name, case=False, na=False)]
    
    if player_row.empty:
        safe_name = player_name.encode('ascii', 'ignore').decode('ascii')
        logger.info(f"Player not found in stats {safe_name}")

        return {}
    
    player_dict = player_row.iloc[0].to_dict()
    
    player_dict = {k: v for k, v in player_dict.items() 
                   if v is not None and not pd.isna(v)}
    
    if use_readable_names:
        mapping = get_readable_stats_mapping()
        readable_dict = {}
        
        for key, value in player_dict.items():
            readable_key = mapping.get(key, key)
            readable_dict[readable_key] = value

        return readable_dict    
    return player_dict


def get_stats(tables_dict, order):
    table_names_stats = [key for key in tables_dict.keys() if key.startswith(f"stats_") and key.endswith("_summary")]
    table_names_passing = [key for key in tables_dict.keys() if key.startswith(f"stats_") and key.endswith("_passing")]
    table_names_passing_types = [key for key in tables_dict.keys() if key.startswith(f"stats_") and key.endswith("_passing_types")]
    table_names_defense = [key for key in tables_dict.keys() if key.startswith(f"stats_") and key.endswith("_defense")]
    table_names_possession = [key for key in tables_dict.keys() if key.startswith(f"stats_") and key.endswith("_possession")]
    table_names_misc = [key for key in tables_dict.keys() if key.startswith(f"stats_") and key.endswith("_misc")]
    table_names_keeper = [key for key in tables_dict.keys() if key.startswith(f"keeper_stats_")]

    i = 0 if order == "first" else 1
    results = []
    matching = {1: table_names_stats, 2: table_names_passing, 3: table_names_passing_types, 
                4: table_names_defense, 5: table_names_possession, 6: table_names_misc,
                7: table_names_keeper}
    
    for j in range(7):
        if matching[j+1]:
            results.append(flatten_columns(tables_dict[matching[j+1][i]]))

    return results


def merge_results(results):
    basic_cols_to_drop = ['Unnamed: 1_level_0_#', 'Unnamed: 2_level_0_Nation', 
                    'Unnamed: 3_level_0_Pos', 'Unnamed: 4_level_0_Age', 
                    'Unnamed: 5_level_0_Min']
    
    merge = results[0].copy()
    for df in results[1:]:
        if df is not None and not df.empty:
            df_to_merge = df.drop(columns=basic_cols_to_drop, errors='ignore')
            merge = merge.merge(df_to_merge, 
                                on='Unnamed: 0_level_0_Player', 
                                how='outer',
                                suffixes=('_left', '_right'))
            merge = merge.loc[:, ~merge.columns.str.endswith('_right')]
            merge.columns = merge.columns.str.replace('_left$', '', regex=True)

    return merge


def get_readable_stats_mapping():
    """
    Returns a comprehensive mapping of abbreviated column names to readable names.
    """
    
    mapping = {
        # Basic Info
        'Unnamed: 0_level_0_Player': 'Player Name',
        'Unnamed: 1_level_0_#': 'Jersey Number',
        'Unnamed: 2_level_0_Nation': 'Nationality',
        'Unnamed: 3_level_0_Pos': 'Position',
        'Unnamed: 4_level_0_Age': 'Age',
        'Unnamed: 5_level_0_Min': 'Minutes Played',
        
        # Performance Stats
        'Performance_Gls': 'Goals',
        'Performance_Ast': 'Assists',
        'Performance_PK': 'Penalty Kicks Made',
        'Performance_PKatt': 'Penalty Kicks Attempted',
        'Performance_Sh': 'Shots Total',
        'Performance_SoT': 'Shots on Target',
        'Performance_CrdY_x': 'Yellow Cards',
        'Performance_CrdY_y': 'Yellow Cards',
        'Performance_CrdR_x': 'Red Cards',
        'Performance_CrdR_y': 'Red Cards',
        'Performance_2CrdY': 'Second Yellow Card',
        'Performance_Touches': 'Touches',
        'Performance_Tkl': 'Tackles',
        'Performance_Int_x': 'Interceptions',
        'Performance_Int_y': 'Interceptions',
        'Performance_Blocks': 'Blocks',
        'Performance_Fls': 'Fouls Committed',
        'Performance_Fld': 'Fouls Drawn',
        'Performance_Off': 'Offsides',
        'Performance_Crs': 'Crosses',
        'Performance_TklW': 'Tackles Won',
        'Performance_PKwon': 'Penalty Kicks Won',
        'Performance_PKcon': 'Penalty Kicks Conceded',
        'Performance_OG': 'Own Goals',
        'Performance_Recov': 'Ball Recoveries',
        
        # Expected Stats
        'Expected_xG': 'Expected Goals (xG)',
        'Expected_npxG': 'Non-Penalty Expected Goals (npxG)',
        'Expected_xAG': 'Expected Assisted Goals (xAG)',
        
        # Shot Creating Actions
        'SCA_SCA': 'Shot Creating Actions',
        'SCA_GCA': 'Goal Creating Actions',
        
        # Passing Stats
        'Passes_Cmp': 'Passes Completed',
        'Passes_Att': 'Passes Attempted',
        'Passes_Cmp%': 'Pass Completion %',
        'Passes_PrgP': 'Progressive Passes',
        'Passes_Att (GK)': 'Passes Attempted (Goalkeeper)',
        'Passes_Thr': 'Throws',
        'Passes_Launch%': 'Launch %',
        'Passes_AvgLen': 'Average Pass Length',
        
        # Passing Detail
        'Total_Cmp': 'Total Passes Completed',
        'Total_Att': 'Total Passes Attempted',
        'Total_Cmp%': 'Total Pass Completion %',
        'Total_TotDist': 'Total Passing Distance',
        'Total_PrgDist': 'Progressive Passing Distance',
        
        'Short_Cmp': 'Short Passes Completed',
        'Short_Att': 'Short Passes Attempted',
        'Short_Cmp%': 'Short Pass Completion %',
        
        'Medium_Cmp': 'Medium Passes Completed',
        'Medium_Att': 'Medium Passes Attempted',
        'Medium_Cmp%': 'Medium Pass Completion %',
        
        'Long_Cmp': 'Long Passes Completed',
        'Long_Att': 'Long Passes Attempted',
        'Long_Cmp%': 'Long Pass Completion %',
        
        'Unnamed: 20_level_0_Ast': 'Assists',
        'Unnamed: 21_level_0_xAG': 'Expected Assisted Goals',
        'Unnamed: 22_level_0_xA': 'Expected Assists',
        'Unnamed: 23_level_0_KP': 'Key Passes',
        'Unnamed: 24_level_0_1/3': 'Passes into Final Third',
        'Unnamed: 25_level_0_PPA': 'Passes into Penalty Area',
        'Unnamed: 26_level_0_CrsPA': 'Crosses into Penalty Area',
        'Unnamed: 27_level_0_PrgP': 'Progressive Passes',
        
        # Pass Types
        'Unnamed: 6_level_0_Att': 'Pass Attempts',
        'Pass Types_Live': 'Live Ball Passes',
        'Pass Types_Dead': 'Dead Ball Passes',
        'Pass Types_FK': 'Free Kick Passes',
        'Pass Types_TB': 'Through Balls',
        'Pass Types_Sw': 'Switches',
        'Pass Types_Crs': 'Crosses',
        'Pass Types_TI': 'Throw-Ins',
        'Pass Types_CK': 'Corner Kicks',
        
        # Corner Kicks
        'Corner Kicks_In': 'Inswinging Corner Kicks',
        'Corner Kicks_Out': 'Outswinging Corner Kicks',
        'Corner Kicks_Str': 'Straight Corner Kicks',
        
        # Outcomes
        'Outcomes_Cmp': 'Completed Passes',
        'Outcomes_Off': 'Offside Passes',
        'Outcomes_Blocks': 'Blocked Passes',
        
        # Tackles
        'Tackles_Tkl': 'Tackles',
        'Tackles_TklW': 'Tackles Won',
        'Tackles_Def 3rd': 'Tackles in Defensive Third',
        'Tackles_Mid 3rd': 'Tackles in Middle Third',
        'Tackles_Att 3rd': 'Tackles in Attacking Third',
        
        # Challenges
        'Challenges_Tkl': 'Challenges',
        'Challenges_Att': 'Challenge Attempts',
        'Challenges_Tkl%': 'Challenge Success %',
        'Challenges_Lost': 'Challenges Lost',
        
        # Blocks
        'Blocks_Blocks': 'Blocks',
        'Blocks_Sh': 'Shots Blocked',
        'Blocks_Pass': 'Passes Blocked',
        
        'Unnamed: 18_level_0_Int': 'Interceptions',
        'Unnamed: 19_level_0_Tkl+Int': 'Tackles + Interceptions',
        'Unnamed: 20_level_0_Clr': 'Clearances',
        'Unnamed: 21_level_0_Err': 'Errors',
        
        # Touches
        'Touches_Touches': 'Total Touches',
        'Touches_Def Pen': 'Touches in Defensive Penalty Area',
        'Touches_Def 3rd': 'Touches in Defensive Third',
        'Touches_Mid 3rd': 'Touches in Middle Third',
        'Touches_Att 3rd': 'Touches in Attacking Third',
        'Touches_Att Pen': 'Touches in Attacking Penalty Area',
        'Touches_Live': 'Live Ball Touches',
        
        # Take-Ons (Dribbles)
        'Take-Ons_Att_x': 'Dribble Attempts',
        'Take-Ons_Att_y': 'Dribble Attempts',
        'Take-Ons_Succ_x': 'Successful Dribbles',
        'Take-Ons_Succ_y': 'Successful Dribbles',
        'Take-Ons_Succ%': 'Dribble Success %',
        'Take-Ons_Tkld': 'Times Tackled During Dribble',
        'Take-Ons_Tkld%': 'Tackled %',
        
        # Carries
        'Carries_Carries_x': 'Carries',
        'Carries_Carries_y': 'Carries',
        'Carries_TotDist': 'Total Carrying Distance',
        'Carries_PrgDist': 'Progressive Carrying Distance',
        'Carries_PrgC_x': 'Progressive Carries',
        'Carries_PrgC_y': 'Progressive Carries',
        'Carries_1/3': 'Carries into Final Third',
        'Carries_CPA': 'Carries into Penalty Area',
        'Carries_Mis': 'Miscontrols',
        'Carries_Dis': 'Dispossessed',
        
        # Receiving
        'Receiving_Rec': 'Passes Received',
        'Receiving_PrgR': 'Progressive Passes Received',
        
        # Aerial Duels
        'Aerial Duels_Won': 'Aerial Duels Won',
        'Aerial Duels_Lost': 'Aerial Duels Lost',
        'Aerial Duels_Won%': 'Aerial Duels Won %',
        
        # Goalkeeper Specific Stats
        'Unnamed: 1_level_0_Nation': 'Nationality (GK)',
        'Unnamed: 2_level_0_Age': 'Age (GK)',
        'Unnamed: 3_level_0_Min': 'Minutes Played (GK)',
        
        'Shot Stopping_SoTA': 'Shots on Target Against',
        'Shot Stopping_GA': 'Goals Against',
        'Shot Stopping_Saves': 'Saves',
        'Shot Stopping_Save%': 'Save Percentage',
        'Shot Stopping_PSxG': 'Post-Shot Expected Goals',
        
        'Launched_Cmp': 'Launched Passes Completed',
        'Launched_Att': 'Launched Passes Attempted',
        'Launched_Cmp%': 'Launched Pass Completion %',
        
        'Goal Kicks_Att': 'Goal Kicks Attempted',
        'Goal Kicks_Launch%': 'Goal Kick Launch %',
        'Goal Kicks_AvgLen': 'Goal Kick Average Length',
        
        'Crosses_Opp': 'Opponent Crosses',
        'Crosses_Stp': 'Crosses Stopped',
        'Crosses_Stp%': 'Cross Stop %',
        
        'Sweeper_#OPA': 'Defensive Actions Outside Penalty Area',
        'Sweeper_AvgDist': 'Average Distance of Defensive Actions',
    }
    
    return mapping