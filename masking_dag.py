import re
from pathlib import Path

def mask_credentials(code_text):
    code_text = re.sub(r"(?i)(aws_access_key_id\s*=\s*['\"])[A-Z0-9]{20}(['\"])", r"\1***REDACTED***\2", code_text)
    code_text = re.sub(r"(?i)(aws_secret_access_key\s*=\s*['\"])[A-Za-z0-9/+=]{40}(['\"])", r"\1***REDACTED***\2", code_text)
    code_text = re.sub(r"(?i)(password\s*=\s*['\"])[^'\"]+(['\"])", r"\1***REDACTED***\2", code_text)
    code_text = re.sub(r"(?i)(token|api_key|secret)[\s:=]+['\"][^'\"]+['\"]", r"\1 = '***REDACTED***'", code_text)
    code_text = re.sub(r"(aws_access_key_id|aws_secret_access_key)\s*=\s*['\"][^'\"]+['\"]", r"\1 = '***REDACTED***'", code_text)

    # ✅ Tambahan berikut:
    code_text = re.sub(r"(arn:aws:iam::\d{12}:role\/[\w+=,.@\-_/]+)", "***REDACTED_IAM_ROLE***", code_text)
    code_text = re.sub(r"(cluster_identifier\s*=\s*['\"])[^'\"]+(['\"])", r"\1***REDACTED_CLUSTER***\2", code_text)
    code_text = re.sub(r"(database\s*=\s*['\"])[^'\"]+(['\"])", r"\1***REDACTED_DB***\2", code_text)
    code_text = re.sub(r"(db_user\s*=\s*['\"])[^'\"]+(['\"])", r"\1***REDACTED_USER***\2", code_text)

    return code_text

def process_dag_file(input_path_str):
    input_path = Path(input_path_str)
    if not input_path.exists():
        raise FileNotFoundError(f"File tidak ditemukan: {input_path_str}")
    
    original_code = input_path.read_text()
    masked_code = mask_credentials(original_code)

    output_path = input_path.with_name(input_path.stem + "_masked.py")
    output_path.write_text(masked_code)

    print(f"✅ Masking selesai. Hasil disimpan di: {output_path}")
    print("\n📄 Contoh hasil masking:\n")
    print(masked_code[:1000])  # Print 1000 karakter pertama

# Contoh pemakaian
if __name__ == "__main__":
    # process_dag_file(r"C:\Users\250010\Documents\DAG\Get_data_lionairapp_per_month.py")
    process_dag_file(r"C:\Users\250010\Documents\DAG\DEV_test_dag_sql_py.py")
