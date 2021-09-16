import awswrangler as wr

result_list = []

def read_parquet(s3_path, dataset=True):
    try :
        columns_types, partitions_types = wr.s3.read_parquet_metadata(path=s3_path)
        columns_types["filename"]= s3_path[0]
        result_list.append(columns_types)
    except Exception as e :
        print(e)
    
    

def lambda_handler(event, context):
    s3_path = "s3://bucket/prefix/"
    object_list= wr.s3.list_objects(s3_path)
    for x in object_list:
        read_parquet([x])
    for y in result_list:
        print(y)