from classes.pink import Pink
from classes.pinkMST import PinkMST
from classes.point import Point
from classes.edge import Edge
from classes.dataSplitter import DataSplitter
from classes.unionFind import UnionFind


def csv_to_binary(csv_path):
    df = pd.read_csv(csv_path)
    df = df.dropna(how='any')
    df['Gender'] = df['Gender'].map({'Male': 1, 'Female': 0})
    df = df.astype(float)
    df = df.drop("CustomerID", axis=1)
    print(df.dtypes)
    display(df)

    import struct

    num_points = len(df)
    num_dimensions = len(df.columns)
    print(num_points)
    binary_file_location = os.getcwd()+"/data/data.bin"
    with open('binary_file_location', 'wb') as f:
        f.write(struct.pack('<i', num_points))
        f.write(struct.pack('<i', num_dimensions))

        # write each point to file in binary format
        for _, row in df.iterrows():
            for value in row:
                f.write(struct.pack('<d', value))
                
                
def prepare_data(binary_file_location, num_data_splits, data_partitions_file_location, spark_config_file, id_partitions_file_location, num_subgraphs, num_points, num_dimensions):
    splitter = DataSplitter(binary_file_location, num_data_splits, data_partitions_file_location, spark_config_file)

    splitter.create_partition_files(id_partitions_file_location, num_subgraphs)

    splitter.write_parquet_files(num_points, num_dimensions)

    #sc = splitter.get_spark_context()
    
    #spark = SparkSession.builder.getOrCreate()
    #return spark