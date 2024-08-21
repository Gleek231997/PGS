import pandas as pd
import numpy as np

# Main script
file_path = "/Users/gloryekbote/Desktop/work/PGS/data/PGS.xlsx"

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
        

       # print(Duplicate_data_df)

        # # Algorithm for Result selection
        Dupe_Ruids = []
        best_results_df = pd.DataFrame(columns=Duplicate_data_df.columns)  # To store the final selected rows

        if Duplicate_data_df is not None:
            for ruid in Duplicate_data_df['RUID']:
                if ruid not in Dupe_Ruids:
                    selected_rows = Duplicate_data_df[Duplicate_data_df['RUID'] == ruid]
                    
                    # Find the highest priority response for PGS1
                    best_row = selected_rows.iloc[0].copy()  # Start with a copy of the first row
                    for question in df_skeleton['QUESTION NUMBER'].unique():
                            
                                relevant_skeleton = df_skeleton[df_skeleton['QUESTION NUMBER'] == question]

                                 #Region High Standard Question
                                if (relevant_skeleton['Question Type'] == 'Standard Question').any():
                                        # Compare and select the response with the highest priority
                                        selected_response = None
                                        highest_priority = np.inf  # Start with a very high number
                                        for response in selected_rows[question]:
                                            if pd.isna(response) or response == '':
                                               highest_priority = -10  # Assign a high priority rank number
                                            else:
                                                priority_series = relevant_skeleton[relevant_skeleton['RESPONSE'] == response]['PRIORITY RANK']
                                                priority = priority_series.iloc[0] if not priority_series.empty else np.inf
                                                # Now compare the priority with the current highest_priority
                                                if priority < highest_priority:
                                                    highest_priority = priority
                                                    selected_response = response

                                        # Create a new row with the best response and add it to best_results_df
                                        best_row[question] = selected_response
                            
                                #Region High Numeric Value
                                highest_numeric = None  # Start with None to easily handle blank or NaN responses
                                if (relevant_skeleton['Question Type'] == 'Numeric Question').any():    
                                    for response in selected_rows[question]:
                                        # Handle blank or NaN responses
                                        if pd.isna(response) or response == '':
                                            continue  # Skip this iteration if the response is blank or NaN
                                        
                                        # Convert response to numeric, if it's not already
                                        response_numeric = pd.to_numeric(response, errors='coerce')
                                        
                                        # Check if this is the highest numeric value found so far
                                        if highest_numeric is None or response_numeric > highest_numeric:
                                            highest_numeric = response_numeric
                                    
                                    # Assign the highest numeric value found (or leave as None if all were blank/NaN)
                                    best_row[question] = highest_numeric if highest_numeric is not None else ''

                               # Region Non Standard Question
                                if (relevant_skeleton['Question Type'] == 'Non Standard Question').any():
                                    selected_response = None

                                    for response in selected_rows[question]:
                                        if (pd.isna(response) or response == '') and selected_response is None :
                                            selected_response = ''  # Choose blank if all responses are blank
                                        else:
                                            selected_response = response
                                  
                                    # Create a new row with the best response and add it to best_results_df
                                    best_row[question] = selected_response if selected_response is not None else ''

                               #Region  Dependant Questions
                                if (relevant_skeleton['Question Type'].str.contains('Dependant Question')).any():
                                    # Ensure dependant_question is a string (assuming it's derived from splitting the 'Question Type' column)
                                    dependant_question = relevant_skeleton['Question Type'].str.split().str[0].iloc[0]
                                    dependant_columns = [col for col in best_row.index if isinstance(col, str) and col.startswith(dependant_question)]
                                    has_text = any(best_row[col] and not pd.isna(best_row[col]) for col in dependant_columns)
                                    selected_response = None
                                    for response in selected_rows[question]:
                                        if has_text:
                                            continue
                                        else:
                                            selected_response = 'None of the above'
                                    best_row[question] = selected_response if selected_response is not None else ''

                    best_results_df = pd.concat([best_results_df, best_row.to_frame().T], ignore_index=True)
                    Dupe_Ruids.append(ruid) 
        
        print(best_results_df)
     

         # Write duplicate_df to an Excel file
        output_file_path = "/Users/gloryekbote/Desktop/work/PGS/data/duplicate_ruid_data.xlsx"
        Duplicate_data_df.to_excel(output_file_path, sheet_name='PGS data', index=False)
        print(f"Combined DataFrame written to {output_file_path}")

         # Write duplicate_df to an Excel file
        output_file_path = "/Users/gloryekbote/Desktop/work/PGS/data/Processed_data.xlsx"
        best_results_df.to_excel(output_file_path, sheet_name='PGS data', index=False)
        print(f"Combined DataFrame written to {output_file_path}")



    except Exception as e:
        print(f"Error in main script: {e}")
