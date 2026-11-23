select * from read_parquet('{{ var("phase3_data_root") }}/companies/**/*.parquet', union_by_name=true)
