from classes.pink import Pink
from classes.pinkMST import PinkMST
from classes.point import Point
from classes.edge import Edge
from classes.dataSplitter import DataSplitter
from classes.unionFind import UnionFind

import time
import logging
import math
import os
import sys
import time

from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructType, StructField, IntegerType, DoubleType, NullType, LongType, StringType

from sklearn.metrics import silhouette_score
import networkx as nx
from scipy.sparse import lil_matrix
from sklearn.cluster import AgglomerativeClustering
from scipy.cluster.hierarchy import dendrogram, linkage
import matplotlib.pyplot as plt







def pink_MST(id_partitions_file_location, data_partitions_file_location, num_subgraphs, num_data_splits, num_points, k):
    start_time_in_milliseconds = int(time.time() * 1000)
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    subgraphs_ids_rdd = sc.textFile(id_partitions_file_location, num_subgraphs)
    subgraphs_files_names_rdd = subgraphs_ids_rdd.map(lambda subgraph_id : Pink.getFilesNames(subgraph_id, num_data_splits, data_partitions_file_location))
    subgraphs_files_names_array = subgraphs_files_names_rdd.collect()
    points_lists = Pink.getPointsList(subgraphs_files_names_array, spark)
    subgraphs_points_pair_Rdd = sc.parallelize(points_lists).map(lambda subgraph_points_pair : Pink.toPoints(subgraph_points_pair))
    subgraph_mst_edges = subgraphs_points_pair_Rdd.flatMap(lambda subgraph: PinkMST( (subgraph[1],subgraph[2]), subgraph[0]).getEdgeList())
    kruskal_reducer = Pink.KruskalReducer(num_points)
    msts_to_merge = subgraph_mst_edges.combineByKey(Pink.CreateCombiner(), Pink.Merger(), kruskal_reducer)
    num_subgraphs = int(num_data_splits * (num_data_splits - 1) / 2 + num_data_splits)
    final_mst = None
    kruskal_reducer = Pink.KruskalReducer(num_points)
    while num_subgraphs > 1:
        num_subgraphs = (num_subgraphs + (k - 1)) // k
        final_mst = msts_to_merge.map(Pink.SetPartitionIdFunction(k)).reduceByKey(kruskal_reducer.call, numPartitions=num_subgraphs)
        msts_to_merge = final_mst
    end_time_in_milliseconds = int(time.time() * 1000)
    print("execution_time : "+str(end_time_in_milliseconds - start_time_in_milliseconds)+ " milliseconds")
    return msts_to_merge

def plot_dendrogram(mst_edges, method, orientation):
    graph_data = mst_edges.flatMap(lambda partition : partition[1]).map(lambda edge : (edge.get_left(), edge.get_right(), edge.get_weight()))
    MST = nx.Graph()
    MST.add_weighted_edges_from(graph_data.collect())
    nodes_number = len(MST.nodes)
    dist_matrix = lil_matrix((nodes_number, nodes_number))
    for u, v, d in MST.edges(data=True):
        dist_matrix[u-1, v-1] = d['weight']
        dist_matrix[v-1, u-1] = d['weight']
    dense_matrix = dist_matrix.toarray()
    linkage_matrix = linkage(dense_matrix, method=method)
    fig = plt.figure(figsize=(20, 50))  # Adjust the width and height as desired
    dendrogram_data = dendrogram(linkage_matrix, orientation=orientation) #labels=labels_
    plt.title('Dendrogram')
    plt.xlabel('Nodes')
    plt.ylabel('Distance')
    plt.show()