import pandas as pd
import os
import glob
import sys
import argparse

def merge_data_files(input_path=".", output_filename="merged_data.csv", include_source=False):
    """
    Merges all CSV and Excel (.xlsx, .xls) files found in the specified input_path
    into a single output file.

    Args:
        input_path (str): Directory path to search for input files (default is current directory).
        output_filename (str): Name of the final merged output file.
        include_source (bool): If True, adds 'Source_File' and 'Source_Sheet' columns.
    """
    # 1. Define file patterns to search for
    csv_files = glob.glob(os.path.join(input_path, '*.csv'))
    excel_files = glob.glob(os.path.join(input_path, '*.xlsx')) + glob.glob(os.path.join(input_path, '*.xls'))
    all_files = csv_files + excel_files

    if not all_files:
        print(f"Error: No CSV or Excel files found in the directory: {os.path.abspath(input_path)}")
        sys.exit(1)

    print(f"\n--- Found {len(all_files)} files to merge ---")
    if include_source:
        print("Source columns (Source_File, Source_Sheet) will be included.")

    # 2. Read and concatenate data
    all_data = []
    
    for filename in all_files:
        try:
            # Check file extension to determine how to read the file
            if filename.lower().endswith(('.xlsx', '.xls')):
                # Read all sheets from the Excel file
                df_dict = pd.read_excel(filename, sheet_name=None)
                
                sheets = []
                for sheet_name, sheet_df in df_dict.items():
                    # Conditionally add Source_Sheet column
                    if include_source:
                        sheet_df['Source_Sheet'] = sheet_name
                    sheets.append(sheet_df)
                
                current_df = pd.concat(sheets, ignore_index=True)
                
            elif filename.lower().endswith('.csv'):
                # Read CSV file
                current_df = pd.read_csv(filename)
                # Conditionally add Source_Sheet column (N/A for CSV)
                if include_source:
                    current_df['Source_Sheet'] = 'N/A' 
            else:
                continue
            
            # Conditionally add Source_File column
            if include_source:
                current_df['Source_File'] = os.path.basename(filename)
                
            all_data.append(current_df)
            
        except Exception as e:
            print(f"Warning: Could not read file {filename}. Skipping. Error: {e}")
            continue

    if not all_data:
        print("Error: Could not successfully read any data files.")
        sys.exit(1)

    # Combine all DataFrames into one final DataFrame
    merged_df = pd.concat(all_data, ignore_index=True)

    # 3. Write the combined data to the output file
    if output_filename.lower().endswith('.xlsx'):
        try:
            merged_df.to_excel(output_filename, index=False)
            print(f"\nSuccessfully merged data into Excel file: {output_filename}")
        except Exception as e:
             print(f"Error writing to Excel: {e}")
    else:
        try:
            merged_df.to_csv(output_filename, index=False)
            print(f"\nSuccessfully merged data into CSV file: {output_filename}")
        except Exception as e:
            print(f"Error writing to CSV: {e}")

# --- Command Line Argument Handling ---
if __name__ == "__main__":
    
    try:
        import pandas as pd
    except ImportError:
        print("The 'pandas' library is required. Please install it using: python -m pip install pandas openpyxl")
        sys.exit(1)

    parser = argparse.ArgumentParser(
        description="Merge CSV and Excel files into a single output file."
    )
    
    # Argument for including source columns
    parser.add_argument(
        '--include-source', 
        '-s', 
        action='store_true', 
        help="Include 'Source_File' and 'Source_Sheet' columns in the merged output. (Default: False)"
    )
    
    # Optional argument for output file name
    parser.add_argument(
        '--output', 
        '-o', 
        type=str, 
        default="combined_reports.csv",
        help="Specify the output filename (e.g., merged.csv or merged.xlsx). (Default: combined_reports.csv)"
    )

    # --- WELCOME MESSAGE AND USAGE GUIDE ---
    print("=====================================================")
    print("          PYTHON DATA MERGER UTILITY")
    print("=====================================================")
    print("This script automatically finds and merges all .csv, .xlsx, and .xls files")
    print("in the current directory into a single output file.")
    
    print("\n--- USAGE EXAMPLES ---")
    print("1. Basic Merge (Default):")
    print("   $ python Excel_Merger.py")
    print("   -> Output: 'combined_reports.csv' (Data columns only)")
    
    print("\n2. Merge WITH Source/Tracking Columns:")
    print("   $ python Excel_Merger.py --include-source")
    print("   -> Output: 'combined_reports.csv' (Includes Source_File, Source_Sheet)")
    
    print("\n3. Merge to a Custom Excel File:")
    print("   $ python Excel_Merger.py -o final_report.xlsx")
    print("   -> Output: 'final_report.xlsx'")
    
    print("\n4. For all options and details:")
    print("   $ python Excel_Merger.py --help")
    print("=====================================================")

    args = parser.parse_args()

    # --- Configuration ---
    input_directory = "." 
    output_file = args.output
    
    # Execute the merge
    merge_data_files(
        input_path=input_directory, 
        output_filename=output_file,
        include_source=args.include_source
    )

    print("\n--- Process Finished ---")
    print(f"The merged file is saved as: {os.path.join(os.path.abspath(input_directory), output_file)}")
