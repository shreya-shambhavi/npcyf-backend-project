import pandas as pd
import sys

def merge_files(file1, file2, join_type):

    def read_file(path):
        if path.endswith('.csv'):
            return pd.read_csv(path)
        elif path.endswith(('.xlsx')):
            return pd.read_excel(path)
        else:
            raise ValueError("Unsupported file format. Kindly use CSV or Excel files.")

    data_file_1 = read_file(file1)
    data_file_2 = read_file(file2)

    # Specifically adding for excel files
    data_file_1.columns = data_file_1.columns.str.strip().str.lower()
    data_file_2.columns = data_file_2.columns.str.strip().str.lower()

    if 'id' not in data_file_1.columns or 'id' not in data_file_2.columns:
        print("Error: Column 'id' not found in one or both files.")
        print(f"File1 columns: {list(data_file_1.columns)}")
        print(f"File2 columns: {list(data_file_2.columns)}")
        sys.exit(1)

    new_merged_file = pd.merge(data_file_1, data_file_2, on = 'id', how = join_type)
    
    output_filename = f"new_merged_file_by_{join_type}_join"
    
    if file1.endswith('.csv'):
        output_file = f"{output_filename}.csv"
        new_merged_file.to_csv(output_file, index = False)
    else:
        output_file = f"{output_filename}.xlsx"
        new_merged_file.to_excel(output_file, index = False)

    print(f"New merged file saved as: {output_file}")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Sample Usage: python main.py <file1> <file2> <join_type>")
        sys.exit(1)

    file1 = sys.argv[1]
    file2 = sys.argv[2]
    join_type = sys.argv[3].lower()

    if join_type not in ['inner', 'outer', 'left', 'right']:
        print("Invalid join type. Kindly use one of the provided - inner, outer, left, right.")
        sys.exit(1)

    merge_files(file1, file2, join_type)
