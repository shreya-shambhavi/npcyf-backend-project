from fastapi import FastAPI, UploadFile, File, Form
import pandas as pd
import tempfile
import os

app = FastAPI()

def read_file(file):
    if isinstance(file, UploadFile):
        filename = file.filename
        file = file.file
    else:
        filename = file
        file = file
    
    if filename.endswith('.csv'):
        return pd.read_csv(file)
    elif filename.endswith(('.xlsx')):
        return pd.read_excel(file)
    else:
        raise ValueError("Unsupported file format. Kindly use CSV or Excel files.")

@app.get("/")
async def root():
    return {"message": "Hieee, Welcome to the Task 2 - File Merger API endpoint. Use the /merge_files endpoint to merge two csv/excel files on the basis of 'id'. Also, kindly use swagger docs at /docs for all test purposes."}

@app.post("/merge_files")
async def api(
    file1: UploadFile = File(...),
    file2: UploadFile = File(...),
    join_type: str = Form(...)
):
    if join_type not in ['inner', 'outer', 'left', 'right']:
        return {"error": "Invalid join type. Kindly use one of the provided - inner, outer, left, right."}

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            file1_path = os.path.join(tmpdir, file1.filename)
            file2_path = os.path.join(tmpdir, file2.filename)

            with open(file1_path, 'wb') as f:
                f.write(await file1.read())
            with open(file2_path, 'wb') as f:
                f.write(await file2.read())

            data_file_1 = read_file(file1_path)
            data_file_2 = read_file(file2_path)
        
        # Specifically adding for excel files
        data_file_1.columns = data_file_1.columns.str.strip().str.lower()
        data_file_2.columns = data_file_2.columns.str.strip().str.lower()
        
        if 'id' not in data_file_1.columns or 'id' not in data_file_2.columns:
            return {
                "error": "Column 'id' not found in one or both files.", 
                "file1_columns": list(data_file_1.columns), 
                "file2_columns": list(data_file_2.columns)
                }
        
        new_merged_file = pd.merge(data_file_1, data_file_2, on = 'id', how = join_type)

        output_filename = f"new_merged_file_by_{join_type}_join"

        if file1.filename.endswith('.csv'):
            output_file = f"{output_filename}.csv"
            new_merged_file.to_csv(output_file, index=False)
        else:
            output_file = f"{output_filename}.xlsx"
            new_merged_file.to_excel(output_file, index=False)

        return {"message": "Merging two files completed", "output_file": output_file}
    except Exception as e:
        return {"error": str(e)}
