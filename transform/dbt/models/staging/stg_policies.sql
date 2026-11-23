select * from read_parquet('{{ var("phase3_data_root") }}/policies/**/*.parquet', union_by_name=true)
