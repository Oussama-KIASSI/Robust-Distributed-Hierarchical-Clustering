## write_sequence_files -- > load_data --> saveAsSequenceFile


import shutil
import struct

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import logging
import fnmatch
import os
import re

from classes.point import Point
#from PointWritable import PointWritable
from typing import Tuple
from pyspark.sql.types import StructType, StructField, LongType, ArrayType, DoubleType, StringType, NullType


schema = StructType([
        StructField("id", LongType(), True),
        StructField("coords", ArrayType(DoubleType()), True)
    ])

class DataSplitter:

    def __init__(self, inputFileName, numSplits, outputDir, configFile):
        self.inputFileName = inputFileName
        self.numSplits = numSplits
        self.outputDir = outputDir
        self.sc = self.init_spark_context(configFile)
        self.numPoints = 0
        self.numDimension = 0
        self.data = []
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def swapBytesInt(t):
        return (0x000000ff & (t >> 24)) | (0x0000ff00 & (t >> 8)) | (0x00ff0000 & (t << 8)) | (0xff000000 & (t << 24))

    @staticmethod
    def get_glob_matcher(glob_p):
        regex = fnmatch.translate(glob_p)
        return re.compile(regex).match

    ##########################################
    @staticmethod
    def is_excluded(exclusion_patterns, file_path):
        file_name = os.path.basename(file_path)
        for exclusion_pattern in exclusion_patterns:
            if fnmatch.fnmatch(file_name, exclusion_pattern):
                return True
        return False

    ###########################################

    @staticmethod
    def add_jar_path(jarPaths: list, exclusionPathMatchers: list, jar: str) -> None:
        if DataSplitter.is_excluded(exclusionPathMatchers, jar):
            logging.debug("Excluding jar to send to workers: {}", jar)
        else:
            logging.debug("Including jar to send to workers: {}", jar)
            jarPaths.append(os.path.abspath(jar))

        ###########################

    @staticmethod
    def add_jar_paths(jar_paths, exclusion_path_matchers, client_config_path, jar_path):
        resolved_jar_path = os.path.join(client_config_path, jar_path)
        if os.path.exists(resolved_jar_path):
            if os.path.isdir(resolved_jar_path):
                print(f"Adding to SparkContext the jars from {resolved_jar_path}")
                for root, dirs, files in os.walk(resolved_jar_path):
                    for file in files:
                        if fnmatch.fnmatch(file, '*.jar'):
                            DataSplitter.add_jar_path(jar_paths, exclusion_path_matchers, os.path.join(root, file))
            else:
                print(f"Adding to SparkContext jar {resolved_jar_path}")
                DataSplitter.add_jar_path(jar_paths, exclusion_path_matchers, resolved_jar_path)
        elif not '*' in resolved_jar_path:
            print("file doesn't exist and wasn't a glob containing '*': " + resolved_jar_path)
        else:
            parent = os.path.dirname(resolved_jar_path)
            parent_file = os.path.abspath(parent)
            if not os.path.exists(parent_file) or not os.path.isdir(parent_file):
                print("parent path doesn't exist for glob: " + resolved_jar_path)
            else:
                print(f"Adding to SparkContext the jars matching {resolved_jar_path}")
                pattern = fnmatch.translate(os.path.basename(resolved_jar_path))
                regex = re.compile(pattern)
                for root, dirs, files in os.walk(parent_file):
                    for file in files:
                        if regex.match(file) and fnmatch.fnmatch(file, '*.jar'):
                            DataSplitter.add_jar_path(jar_paths, exclusion_path_matchers, os.path.join(root, file))

    def init_spark_context(self, configure_file=None):
        spark = SparkSession.builder \
        .appName("Pink") \
        .master("local[*]") \
        .getOrCreate()
        return spark.sparkContext


    def get_spark_context(self):
        return self.sc

    @staticmethod
    def display_value(points, size):
        print("==========================================")
        num = 0
        for p in points:
            print("=====: ", p)
            if num + 1 > size:
                break
            num += 1
        print("==========================================")

    #################################################################""

    # saveAsSequenceFile takes a list of Points, serialize them using PointWritable and store them in outputDir
    # in num_partitions files in the following format <key=None, value=PointWritable>
    # concept : takes the whole graph points, load them into and rdd and parallelize them into numSplits node
            
    def saveAsParquetFile(self, points):
        output_dir = self.outputDir
        if os.path.exists(output_dir):
            logging.info("the directory exists: " + output_dir)
            shutil.rmtree(output_dir)
        if os.path.exists(output_dir):
            raise RuntimeError("the directory still exists: " + output_dir)
        else:
            logging.info("the directory is deleted: " + output_dir)

        points_to_write = self.sc.parallelize(points, self.numSplits)

        #points_pair_to_write = points_to_write.map(lambda point: (None, point))
        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame(points_to_write, schema).repartition( self.numSplits)
        try:
            df.write.mode('overwrite').parquet(self.outputDir)
        except Exception as e:
            print(e)


    # this function takes a file and split it into multiple partitions num_partitions that will be stored in file_loc
    # num_partitions here refers to the total number of subgraphs
    def create_partition_files(self, file_loc: str, num_partitions: int) -> None:
        id_subgraphs = [str(i) for i in range(num_partitions)]
        print("================calling create_partition_files from DataSplitter==================")
        print("numPartions : "+str(num_partitions))
        if os.path.isdir(file_loc):
            shutil.rmtree(file_loc)
        print("create idSubgraph files: " + file_loc)
        try:
            self.sc.parallelize(id_subgraphs, num_partitions).saveAsTextFile(file_loc)
            print("idPartitions succefully written")
            print("================ create_partition_files completed ==================")
        except Exception as e:
            print(e)
            return e



    ##is reading in a binary input file, where each point is represented as a sequence of bytes, and converting it to a 2D Double array called data
    def load_data(self):
        double_size_in_bytes = 8
        point_size_in_bytes = double_size_in_bytes * self.numDimension 
        print("point_size_in_bytes : "+ str(point_size_in_bytes))
        with open(self.inputFileName, 'rb') as data_in:
            num_points = struct.unpack('<i', data_in.read(4))[0]
            num_dimensions = struct.unpack('<i', data_in.read(4))[0]
            print(f"data dimensions: {num_points},{num_dimensions}")
            self.data = [[0.0 for _ in range(num_dimensions)] for _ in range(num_points)]

            for i in range(num_points):
                result = bytearray(data_in.read(point_size_in_bytes))
                offset = 0

                for j in range(num_dimensions):
                    bytes_ = result[offset:offset + double_size_in_bytes]
                    self.data[i][j] = struct.unpack('<d', bytes_)[0]
                    offset += double_size_in_bytes

    @staticmethod
    def delete(file):
        if os.path.isdir(file):
            if not os.listdir(file):
                logging.info("delete the empty Dir: " + file)
                os.rmdir(file)
            else:
                for nested_file_name in os.listdir(file):
                    nested_file = os.path.join(file, nested_file_name)
                    DataSplitter.delete(nested_file)
                if not os.listdir(file):
                    os.rmdir(file)
        else:
            logging.info("delete the file: " + file)
            os.remove(file)

    def write_parquet_files(self, num_points, num_dimension):
        #load_data return a data 2Darray that contains data[Point][Dimensions]
        print("================calling write_parquet_files from DataSplitter==================")
        self.numDimension = num_dimension
        self.load_data()
        points = []
        for i in range(num_points):
            payload = self.data[i][:num_dimension]
            point = Point(i, payload)
            points.append(point.__clone__())
        self.display_value(points, 5)
        #return points
        try : 
            self.saveAsParquetFile(points)
            print("parquet files succefully written")
            print("================ write_parquet_files completed ==================")
        except e :
            print("error in writing parquet files")
            print(e)



  