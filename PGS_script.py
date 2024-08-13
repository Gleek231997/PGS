import pandas as pd
import numpy as np

# Main script
file_path = "/Users/gloryekbote/Desktop/work/PGS/data/Class of 2024_May PGS_June 25, 2024_08.57.xlsx"

try:
    # Read the "Raw Data" sheet
    df_raw = pd.read_excel(file_path, sheet_name='Raw Data', engine='openpyxl')

    # Read the "Skeleton" sheet
    df_skeleton = pd.read_excel(file_path, sheet_name='Skeleton', engine='openpyxl')
    
except Exception as e:
    print(f"Error reading the Excel file: {e}")
    df_raw, df_skeleton = None, None

if df_raw is not None and df_skeleton is not None:
    try:
        # Identify duplicate RUIDs
        duplicate_ruids = df_raw[df_raw.duplicated('RUID', keep=False)]
        duplicate_ruids_sorted = duplicate_ruids.sort_values(by='RUID', ascending=True)
        df_skeleton['QUESTION NUMBER'] = df_skeleton['QUESTION NUMBER'].fillna(method='ffill')
        skeleton_columns = df_skeleton['QUESTION NUMBER']
        matching_columns = [col for col in skeleton_columns.unique() if col in duplicate_ruids_sorted.columns]
        matching_columns.append('RUID')
        Duplicate_data_df = duplicate_ruids_sorted[matching_columns]
        
        # Algorithm for Result selection
        Dupe_Ruids = []
        best_results_df = pd.DataFrame(columns=Duplicate_data_df.columns)  # To store the final selected rows

        if Duplicate_data_df is not None:
            for ruid in Duplicate_data_df['RUID']:
                if ruid not in Dupe_Ruids:
                    selected_rows = Duplicate_data_df[Duplicate_data_df['RUID'] == ruid]
                    #print(selected_rows)
                    
                    # Find the highest priority response for PGS1
                    question = 'PGS1'
                    relevant_skeleton = df_skeleton[df_skeleton['QUESTION NUMBER'] == question]
                    #print(relevant_skeleton)

                    # Compare and select the response with the highest priority
                    selected_response = None
                    highest_priority = np.inf  # Start with a very high number

                    for response in selected_rows[question]:
                        # Get the priority rank of the response from df_skeleton
                        priority = relevant_skeleton[relevant_skeleton['RESPONSE'] == response]['PRIORITY RANK']
                        if not priority.empty and priority.iloc[0] < highest_priority:
                            highest_priority = priority.iloc[0]
                            selected_response = response

                    # Create a new row with the best response and add it to best_results_df
                    best_row = selected_rows.iloc[0].copy()  # Start with a copy of the first row
                    best_row[question] = selected_response
                    print(best_row)
                    
                    best_results_df = best_results_df.append(best_row, ignore_index=True)
                    # Add the RUID to the list of processed RUIDs
                    Dupe_Ruids.append(ruid)
                
                break

        # Print the final DataFrame with the best results
        #print(best_results_df)

    except Exception as e:
        print(f"Error in main script: {e}")
