import logging
import math
import os
import sys
import time

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from classes.dataSplitter import DataSplitter
from classes.pinkMST import PinkMST
from classes.point import Point
#from PointWritable import PointWritable
from classes.unionFind import UnionFind
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StructType, StructField, IntegerType, DoubleType, NullType, LongType, StringType
from classes.edge import Edge

schema = StructType([
        StructField("id", LongType(), True),
        StructField("coords", ArrayType(DoubleType()), True)
    ])

allPointsSchema = StructType([
        StructField("id", LongType(), True),
        StructField("left_points",ArrayType(schema), True),
        StructField("right_points",ArrayType(schema), True)
])

class Pink:
    @staticmethod
    def displayResults(mstToBeMerged):
        print(mstToBeMerged.count())
        # numEdges = 0
        # for record in mstToBeMerged.collect():
        #     print("===== key : " + str(record[0]))
        #     for edge in record[1]:
        #         if numEdges > 10:
        #             break
        #         numEdges += 1
        #         print("===== edge : " + str(edge))
        #     print()

    #this class takes two sets of edges and apply UnionFind on them to combine them into one subgraph in a way to avoid producing cycles
    class KruskalReducer:
        def __init__(self, numPoints):
            self.numPoints = numPoints

        def call(self, leftEdges, rightEdges):
            uf = UnionFind(self.numPoints)
            edges = []
            leftEdgesIterator = iter(leftEdges)
            rightEdgesIterator = iter(rightEdges)
            leftEdge = next(leftEdgesIterator)
            rightEdge = next(rightEdgesIterator)
            minEdge = None
            isLeft = None
            numEdges = self.numPoints - 1
            while True:
                if leftEdge.get_weight() < rightEdge.get_weight():
                    minEdgeIterator = leftEdgesIterator
                    minEdge = leftEdge
                    isLeft = True
                else:
                    minEdgeIterator = rightEdgesIterator
                    minEdge = rightEdge
                    isLeft = False
                if uf.unify(minEdge.get_left(), minEdge.get_right()):
                    edges.append(minEdge)
                minEdge = next(minEdgeIterator, None)
                if isLeft:
                    leftEdge = minEdge
                else:
                    rightEdge = minEdge
                if minEdge is None or len(edges) >= numEdges:
                    break
            minEdgeIterator = rightEdgesIterator if isLeft else leftEdgesIterator
            minEdge = rightEdge if isLeft else leftEdge
            while len(edges) < numEdges and minEdge is not None:
                if uf.unify(minEdge.get_left(), minEdge.get_right()):
                    edges.append(minEdge)
                minEdge = next(minEdgeIterator, None)
            return edges

    class CreateCombiner:
        @staticmethod
        def __call__(edge):
            edgeList = [edge]
            return edgeList

    class Merger:
        @staticmethod
        def __call__(edgeList, edge):
            mergeList = list(edgeList)
            mergeList.append(edge)
            return mergeList

    class SetPartitionIdFunction:
        def __init__(self, K):
            self.K = K

        def __call__(self, row):
            key, iterable_of_edges = row
            partition_id = key // self.K
            result = (partition_id, iterable_of_edges)
            return result

    @staticmethod
    def get_right_id(part_id):
        result = ((int(math.sqrt((int(part_id) << 3) + 1)) + 1) >> 1)
        return result

    @staticmethod
    def get_left_id(part_id, right_id):
        result = (int(part_id) - (((right_id - 1) * right_id) >> 1))
        return result

    @staticmethod
    def open_file(fileName):
        """conf = SparkConf().setAppName("openFile")
        sc = SparkContext.getOrCreate(conf=conf)
        fileRDD = sc.sequenceFile(fileName, NullType.__name__, PointWritable.__name__)
        points = fileRDD.values().collect()
        sc.stop()
        return points"""
        #return ((None,1),(None,2))
        """spark = SparkSession.builder \
        .getOrCreate()"""
        df = sp.read.parquet(fileName)
        #points = df.filter(df._1.isNull()).select(df._2).rdd.flatMap(lambda row: row).collect()
        points = df.filter(df._1.isNull()).select(df._2).collect()
        return points
        #return (1,1)
    #this function operates on an rdd, it takes the partitionId, checks if it should blongs to a bipartite subgraph or a normal subgraph
    #for each partitionId, the function checks if the partitionId points on an bipartite graph or not,
    #it then generates the name of the partition files based on the way they were stored by hadoop
    #the function then calls openFile function to retrive the data points in each filesLoc.
    #finally the function returns the data points in form of a pair of two disjoint dataset if it is a biparite graph, and return only one if it is a normal graph
    #it does this in a manner that the first num_bipartite_subgraphs elements are bipartite graphs and the rest are normal subgraphs
    #the data is stored in form of list of point of each subgraph
    #the bipartite subraphs have two sets of data returned as a pair of data left and right while normal subgraphs only have one set of data
    #!!!!!!!!!!!!!the logic here is that we only have numDataSplit sequence Files, but when processing the graph.
        #we process each sequence File as a separate set of points. and for bipartite graphs we process them by reading data from two separate sequence files, noted as left and right
        #this is done by generating the names of left and right files based on a loigic to test all of the subgraphs combinations " num_data_splits * (num_data_splits - 1) // 2"

    @staticmethod
    def get_subgraph_pair(partition_id, num_data_splits, input_data_files_loc):
        num_bipartite_subgraphs = num_data_splits * (num_data_splits - 1) // 2
        if partition_id < num_bipartite_subgraphs:
            right_id = Pink.get_right_id(partition_id)
            left_id = Pink.get_left_id(partition_id, right_id)
        else:
            left_id = partition_id - num_bipartite_subgraphs
            right_id = left_id

        left_file_name = "{}/part-{:05d}".format(input_data_files_loc, left_id)
        right_file_name = "{}/part-{:05d}".format(input_data_files_loc, right_id)

        points_left = Pink.open_file(left_file_name)
        points_right = None
        if left_file_name != right_file_name:
            points_right = Pink.open_file(right_file_name)

        return points_left, points_right



    #this class is called by an rdd to operate on a partitionId,
    #it takes the partitionId, calls get_subgraph_pair to get the pair of datasets that contain the data points of the particular subgraph noted by partitionId
    #and finally calls the PinkMST class to calculate the MST of the specific subgraph and returns the egdeList of the MST
    class GetPartitionFunction:

        def __init__(self, inputDataFilesLoc, numDataSplits):
            self.inputDataFilesLoc = inputDataFilesLoc
            self.numDataSplits = numDataSplits
        def __call__(self, row):
            print("0")
            partionId = int(row)
            print("1")
            subGraphsPair = Pink.get_subgraph_pair(partionId, self.numDataSplits, self.inputDataFilesLoc)
            print("2")
            pinkMst = PinkMST(subGraphsPair, partionId)
            edgeList = pinkMst.getEdgeList()
            # return [(i, j) for i, j in edgeList]
            return edgeList

    #
    def getFilesNames(partition_id, num_data_splits, input_data_files_loc):
        partition_id = int(partition_id)
        num_bipartite_subgraphs = num_data_splits * (num_data_splits - 1) // 2
        if partition_id < num_bipartite_subgraphs:
            right_id = Pink.get_right_id(partition_id)
            left_id = Pink.get_left_id(partition_id, right_id)
        else:
            left_id = partition_id - num_bipartite_subgraphs
            right_id = left_id

        left_file_name = "{}/part-{:05d}".format(input_data_files_loc, left_id)
        right_file_name = "{}/part-{:05d}".format(input_data_files_loc, right_id)
        return (partition_id, left_file_name, right_file_name)

    #this function takes a
    def getPointsList(filesNamesRddArray, spark):
        pointsList = []
        for graph in filesNamesRddArray:
            partitionId = int(graph[0])
            leftPointsFileName = graph[1]
            rightPointsFileName = graph[2]
            leftPointsData = spark.read.schema(schema).parquet(leftPointsFileName+"*"+".parquet").collect()
            rightPointsData = []
            if leftPointsFileName != rightPointsFileName:
                rightPointsData = spark.read.schema(schema).parquet(rightPointsFileName+"*"+".parquet").collect()
            graph_pair = (partitionId, leftPointsData, rightPointsData )
            pointsList.append(graph_pair)
            #print(str(partitionId)+" left File : "+leftPointsFileName+" ---------- right file :"+rightPointsFileName)
            #print("data_count : "+str(leftPointsDataDf.count()))
            #leftPointsDataDf.show()
            #df = spark.createDataFrame(pointsList, ["id", "left_points", "right_points"]).repartition(numSubGraphs)
        return pointsList
    
    def toPoints(x):
        leftPoints = []
        rightPoints = []
        for l in x[1]:
            leftPoints.append(Point(l.id,l.coords))
        for r in x[2]:
            rightPoints.append(Point(r.id,r.coords))
        return(x[0],leftPoints,rightPoints )

    """if __name__ == "__main__":
#spark conf dir : '/usr/local/spark/conf'
        K = 2
        numDataSplits = 4
        numPoints = 200
        numDimensions = 4
        dataSetName = "data"
        configFile = None
        PROJECT_HOME = "/home/jovyan/work/"
        FS_PREFIX = "file:///"

        args = None  # Set the value of args here


        print("PinkConfig: dataSet=%s numPoints=%d numDimensions=%d numDataSplits=%d K=%d configFile=%s"
                % (dataSetName, numPoints, numDimensions, numDataSplits, K, configFile))

        #numSubGraphs is the total number of subgraph inclding the combinations of bipartite graphs
        numSubGraphs = int(numDataSplits * (numDataSplits - 1) / 2 + numDataSplits)

        #the home directory where the
        EXPERIMENT_HOME = "/home/jovyan/work/"+ dataSetName + "_d" + str(numDimensions) + "_s" + str(numDataSplits)
        os.makedirs(EXPERIMENT_HOME, exist_ok=True)
        print("Experiment_Home : "+EXPERIMENT_HOME)
        
        # number of subgraphs
        idPartitionFilesLoc = EXPERIMENT_HOME + "/subgraphIds"
        # number of dataPartitions
        dataPartitionFilesLoc = EXPERIMENT_HOME + "/dataPartitions"

        binaryFileName = PROJECT_HOME + dataSetName + ".bin"
        splitter = DataSplitter(binaryFileName, numDataSplits, dataPartitionFilesLoc, configFile)
        splitter.create_partition_files(idPartitionFilesLoc, numSubGraphs)

        splitter.write_sequence_files(numPoints, numDimensions)

        sc = splitter.get_spark_context()
        # create an rdd that have the number of partitions :: each line has the id of each partition with a total of num_subgraphs id
        partitionRDD = sc.textFile(idPartitionFilesLoc, numSubGraphs)
        start = time.time()
        partitions = partitionRDD.flatMap(GetPartitionFunction(dataPartitionFilesLoc, numDataSplits))
        print(partitions.collect())"""

    