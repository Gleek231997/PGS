import pandas as pd
import numpy as np

# File path
file_path = '/Users/gloryekbote/Desktop/work/PGS/data/Try 2_2.3.25.xlsx'

# Read the Excel sheets
df_raw = pd.read_excel(file_path, sheet_name='Raw Data', engine='openpyxl')
df_skeleton = pd.read_excel(file_path, sheet_name='Skeleton', engine='openpyxl')

try: 
    # Remove the first header row if it's a duplicate
    df_raw = df_raw.iloc[1:].reset_index(drop=True)

    # Data type corrections
    df_raw['RUID'] = df_raw['RUID'].astype(str)  # Ensure RUID is a string
    df_skeleton['PRIORITY RANK'] = pd.to_numeric(df_skeleton['PRIORITY RANK'], errors='coerce')  # Convert PRIORITY RANK to numeric

    # Convert date columns without 'infer_datetime_format' as it's deprecated
    df_raw['RecordedDate'] = pd.to_datetime(df_raw['RecordedDate'], errors='coerce')
    df_raw['StartDate'] = pd.to_datetime(df_raw['StartDate'], errors='coerce')
    df_raw['EndDate'] = pd.to_datetime(df_raw['EndDate'], errors='coerce')

    # Fill missing QUESTION NUMBER values
    df_skeleton['QUESTION NUMBER'] = df_skeleton['QUESTION NUMBER'].ffill()

    # Check the data after adjustments
    # df_raw_head = df_raw.head()
    # df_skeleton_head = df_skeleton.head()

except Exception as e:
    print(f"Error reading the Excel file: {e}")
    df_raw, df_skeleton = None, None

if df_raw is not None and df_skeleton is not None:
    try:
        # Identify duplicate RUIDs
        duplicate_ruids = df_raw[df_raw.duplicated('RUID', keep=False)]
        duplicate_ruids_sorted = duplicate_ruids.sort_values(by='RUID', ascending=True)
       #df_skeleton['QUESTION NUMBER'] = df_skeleton['QUESTION NUMBER'].fillna(method='ffill')

        skeleton_columns = df_skeleton['QUESTION NUMBER']
        matching_columns = [col for col in skeleton_columns.unique() if col in duplicate_ruids_sorted.columns]
        matching_columns.append('RUID')
        Duplicate_data_df = duplicate_ruids_sorted[matching_columns]

        print(Duplicate_data_df)

        # Algorithm for result selection
        Dupe_Ruids = []
        best_results_df = pd.DataFrame(columns=Duplicate_data_df.columns)  # To store the final selected rows

        print(best_results_df)

        if Duplicate_data_df is not None:
            for ruid in Duplicate_data_df['RUID'].unique():
                if ruid not in Dupe_Ruids:
                    selected_rows = Duplicate_data_df[Duplicate_data_df['RUID'] == ruid]
                    best_row = selected_rows.iloc[0].copy()  # Start with a copy of the first row

                    for question in df_skeleton['QUESTION NUMBER'].unique():
                        try:
                            relevant_skeleton = df_skeleton[df_skeleton['QUESTION NUMBER'] == question]

                            # High Standard Question
                            if (relevant_skeleton['QUESTION TYPE'] == 'Standard Question').any():
                                selected_response = None
                                highest_priority = np.inf
                                for response in selected_rows[question]:
                                    if pd.isna(response) or response == '':
                                        priority = 10  # Assign high rank for blank responses
                                    else:
                                        priority_series = relevant_skeleton[relevant_skeleton['RESPONSE'] == response]['PRIORITY RANK']
                                        priority = priority_series.iloc[0] if not priority_series.empty else np.inf
                                    if priority < highest_priority:
                                        highest_priority = priority
                                        selected_response = response
                                best_row[question] = selected_response

                            # High Numeric Value
                            if (relevant_skeleton['QUESTION TYPE'] == 'Numeric Question').any():
                                highest_numeric = None
                                for response in selected_rows[question]:
                                    if not pd.isna(response) and response != '':
                                        response_numeric = pd.to_numeric(response, errors='coerce')
                                        if highest_numeric is None or response_numeric > highest_numeric:
                                            highest_numeric = response_numeric
                                best_row[question] = highest_numeric if highest_numeric is not None else ''

                            # Non-Standard Question
                            if (relevant_skeleton['QUESTION TYPE'] == 'Non Standard Question').any():
                                selected_response = None
                                for response in selected_rows[question]:
                                    if (pd.isna(response) or response == '') and selected_response is None:
                                        selected_response = ''
                                    elif not (pd.isna(response) or response == ''):
                                        selected_response = response
                                best_row[question] = selected_response if selected_response is not None else ''

                            # Dependant Questions
                            if (relevant_skeleton['QUESTION TYPE'].str.contains('Dependant Question')).any():
                                dependant_question = relevant_skeleton['QUESTION TYPE'].str.split().str[0].iloc[0]
                                dependant_columns = [col for col in best_row.index if col.startswith(dependant_question)]
                                has_text = any(best_row[col] and not pd.isna(best_row[col]) for col in dependant_columns)

                                selected_response = None
                                for response in selected_rows[question]:
                                    if has_text or (pd.isna(response) or response == ''):
                                        continue
                                    elif response == 'None of the above' and not has_text:
                                        selected_response = 'None of the above'
                                    elif response == 'No thank you, I am not interested at this time' and not has_text:
                                        selected_response = response
                                best_row[question] = selected_response if selected_response is not None else ''

                        except Exception as e:
                            print(f"Error processing question {question}: {e}")

                    best_results_df = pd.concat([best_results_df, best_row.to_frame().T], ignore_index=True)
                    Dupe_Ruids.append(ruid)

        # Update the main DataFrame with the best results
        for index, row in best_results_df.iterrows():
            ruid = row['RUID']
            excel_index = df_raw.index[df_raw['RUID'] == ruid]
            if not excel_index.empty:
                for column in best_results_df.columns:
                    if column in df_raw.columns:
                        df_raw.loc[excel_index, column] = row[column]

        # Final sorting and deduplication
        df_raw = df_raw.sort_values(by=['RUID', 'RecordedDate'], ascending=[True, False])
        df_raw = df_raw.drop_duplicates(subset=['RUID'], keep='first')

        # Check for any remaining duplicate RUIDs
        remaining_duplicates = df_raw[df_raw.duplicated('RUID', keep=False)]

        if not remaining_duplicates.empty:
            print(f"There are still {remaining_duplicates['RUID'].nunique()} duplicate RUIDs remaining.")
            print(remaining_duplicates[['RUID']].value_counts())
        else:
            print("No duplicate RUIDs found. Deduplication successful!")


        # Save the updated DataFrame
        output_file_path = "/Users/gloryekbote/Desktop/work/PGS/data/Clean_Data.xlsx"
        df_raw.to_excel(output_file_path, sheet_name='Raw Data', index=False)
        print(f"Updated Excel file saved at: {output_file_path}")

        # Save duplicate data
        output_file_path = "/Users/gloryekbote/Desktop/work/PGS/data/DuplicateRUIDs.xlsx"
        Duplicate_data_df.to_excel(output_file_path, sheet_name='PGS data', index=False)
        print(f"Duplicate DataFrame written to {output_file_path}")

        print(df_raw)
    except Exception as e:
        print(f"Error in main script: {e}")
