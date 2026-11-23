select * from read_parquet('{{ var("phase3_data_root") }}/employee_history/**/*.parquet', union_by_name=true)
