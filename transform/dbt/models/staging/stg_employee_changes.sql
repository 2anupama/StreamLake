select * from read_parquet('{{ var("phase3_data_root") }}/employee_changes/**/*.parquet', union_by_name=true)
