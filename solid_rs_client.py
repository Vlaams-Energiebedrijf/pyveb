import boto3
import pandas as pd
from io import BytesIO, StringIO
import uuid
import psycopg2
from pyveb import rs_client

## stringIO and bytesIO are in-memory file-like objects. They provide an interface similar as file objects but operate entirely in memory
## rather than reading from and to physical objects. This makes them lightweight, fast and ideal for temporary storage or processing of string/binary data

"""
    a stringIO object provides  a text buffer in memory. You can write strings to it, read from it, or manipulate its data as it were a file. 
    Faster than writing to disk since no IO operations are required. It allows processing of data entirely in memory and avoids the creation om temp files on disk

"""


class DBStreamer:

    def __init__(self):
        self.unique_id  = str(uuid.uuid4()).replace('-','')
        self.cursor_name = f'server_side_cursor_{self.unique_id}'

    def stream(self,  query:str, batch_size: int):
        """
            Stream in batches to the client and yield to caller. 
            Implements server side cursor so full result set is only fetched in database memory

            https://wiki.postgresql.org/wiki/Using_psycopg2_with_PostgreSQL
        """
        raise NotImplementedError("This method should be implemented by its subclasses")

class RedshiftStreamer(DBStreamer):

    def __init__(self, rs_client):
        self.rs_client = rs_client

    def stream(self, query:str, batch_size: int):
        cursor = self.rs_client.conn.cursor(self.cursor_name, cursor_factory=psycopg2.extras.DictCursor )
        cursor.itersize = batch_size
        cursor.execute(query)
        try:
            while True:
                rows = cursor.fetchmany(batch_size)
                cols = cursor.description
                if not rows:
                    break
                yield rows, cols
        finally:
            cursor.close()

class CloudLoader:

    def upload(self, body) -> bool:
        raise NotImplementedError("This method should be implemented by its subclasses")
    
class AWSS3Loader:

    def __init__(self, s3_bucket:str, s3_prefix:str):
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix

    def upload(self, buffer, file_name, extension):
        s3 = boto3.resource('s3')
        s3.Object(self.s3_bucket, f'{self.s3_prefix}/{file_name}.{extension}').put(Body=buffer)

class DataFrameSerializer:

    def serialize(self, df: pd.DataFrame) -> bytes:
        raise NotImplementedError("This method should be implemented by the subclasses")
    
class ParquetSerializer(DataFrameSerializer):

    def serializer(self, df: pd.DataFrame) -> bytes:
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, allow_truncated_timestamps=True)
        buffer.seek(0)
        return buffer.get_value()
    
class CSVSerializer(DataFrameSerializer): 

    def serializer(self, df: pd.DataFrame) -> bytes:
        buffer = StringIO()
        df.to_csv(buffer, index=False )
        buffer.seek(0)
        return buffer.get_value()


class DataStreamer:

    def __init__(self, streamer: DBStreamer, serializer: DataFrameSerializer, loader: CloudLoader):
        self.streamer = streamer
        self.serializer = serializer
        self.loader = loader

    def rs_to_s3(self, query: str, batch_size: int, file_name:str):
        for rows, cols in self.streamer.stream(query, batch_size):
            get_data = [x for x in rows]
            col_names = [y[0] for y in cols]
            df = pd.DataFrame(get_data)
            df.columns = col_names
            serialized_data = self.serializer.serialize(df)
            extension = 'parquet' if isinstance(self.serializer, ParquetSerializer) else 'csv'
            self.loader.upload(serialized_data, file_name, extension)


rs = rs_client.rsClient('dev', 'iam_role')
rs_streamer = RedshiftStreamer(rs)
parquet_serializer = ParquetSerializer()
s3_loader = AWSS3Loader('veb_data_pipelines', 'dev/pipeline_1/...')

streamer = DataStreamer(rs_streamer, parquet_serializer, s3_loader)
streamer.rs_to_s3('select * from test', 50000, 'test')


"""

SOLID principles applied

1. Singe Responsibility principle:

Each class has a single responsibility. 

    uploader: handles uploading 
    serializer: handles serializing
    datastreamer: orchestrates streaming

2. Open/Closed Principle

added DataFrameSerializer as a base clase with serializer method. Net serialization methods can be easily added with subclassing without modifying existing code

3. Liskov Substition Principle

Within DataStreamer we use DataFrameSerializer base class. We can replace this with concrete implementations such as ParquetSerializer or CSVSerializer without altering
the behavior of DataStreamer

4. Interface Segregation principle

    Each class has focused interfaces: 

5. DataStreamer depends on abstractions, not on concrete implementations

"""


        


