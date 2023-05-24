"""next keeps track on the point that will be added to the MST. it takes the point that has the smallest key value
shift stores the index of the smallest key in the current iteration
left stotres the unprocessed point. points that are not added yet to the MST
minV is used to keep track of the minimum key value encountered in the loop where it is processed.
globalNext is the global index of the next"""

#this class is used to find the MST of a given subgraph. it uses the Primlocal on normal graphs and BipartiteMST on bipartite graphs
import numpy as np
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

from classes.edge import Edge
from classes.point import Point

class MyEdgeComparable:
    def __call__(self, t1, t2):
        e1 = t1[1]
        e2 = t2[1]
        return -1 if e1.get_weight() < e2.get_weight() else (0 if e1.get_weight() == e2.get_weight() else 1)

"""class MyEdgeComparable:
    def __call__(self, item):
        return item[1].weight"""
class PinkMST:
    def __init__(self, paired_data, partition_id):
        self.left_data = paired_data[0]
        self.right_data = paired_data[1]
        self.partition_id = partition_id
        self.is_bipartite = True if self.right_data!=[] else False
        self.mst_to_be_merged = None

    def get_mst(self):
        return self.mst_to_be_merged.values()

    @staticmethod
    def display_value(left_data, right_data, num_points):
        for i in range(num_points):
            print("==== samples {}, {} || {}".format(i, left_data[i], right_data[i] if right_data else None))

    from pyspark.sql.functions import col, struct
    from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

    def prim_local(self):
        data = self.left_data
        num_points = len(data)
        edge_pairs = []

        left = list(range(num_points))
        parent = np.empty(num_points, dtype=int)
        key = [float('inf')] * num_points
        key[0] = 0
        parent[0] = -1

        for i in range(num_points):
            left[i] = i+1

        next_index = 0
        for j in range(num_points - 1, 0, -1):
            curr_pt = next_index
            shift = 0
            next_index = left[shift]
            min_dist = float('inf')
            for i in range(j):
                other_pt = left[i]
                try:
                    dist = data[curr_pt].distanceTo(data[other_pt])
                    if dist < key[other_pt]:
                        key[other_pt] = dist
                        parent[other_pt] = curr_pt
                except Exception as e:
                    print(e)
                if key[other_pt] < min_dist:
                    shift = i
                    min_dist = key[other_pt]
                    next_index = other_pt
            global_next = data[next_index].getId()
            global_next_parent = data[parent[next_index]].getId()
            edge = Edge(min(global_next,global_next_parent), max(global_next,global_next_parent), min_dist)
            edge_pairs.append((self.partition_id, edge))
            left[shift] = left[j-1]
            j -= 1
        edge_pairs = sorted(edge_pairs, key=lambda pair: pair[1].get_weight())
        return edge_pairs

    def BipartiteMST(self):
        data1 = self.left_data
        data2 = self.right_data
        localLeftData = data1
        localRightData = data2

        numLeftPoints = len(data1)
        numRightPoints = len(data2)
        numLeftPointsInTrack = 0
        numRightPointsInTrack = 0

        edgePairs = []
        next1 = [i for i in range(numLeftPoints)]
        parent1 = [-1] * numLeftPoints
        key1 = [float('inf')] * numLeftPoints

        next2 = [i for i in range(numRightPoints)]
        parent2 = [-1] * numRightPoints
        key2 = [float('inf')] * numRightPoints

        nextLeft = None
        nextRight = None
        parentLeft = None
        parentRight = None
        keyLeft = None
        keyRight = None

        tmpData = []
        tmpKey = []
        tmpNext = []
        tmpParent = []
        tmpNumPoints = 0

        if numLeftPoints <= numRightPoints:
            numLeftPointsInTrack = numLeftPoints
            numRightPointsInTrack = numRightPoints

            keyLeft = key1
            nextLeft = next1
            parentLeft = parent1

            keyRight = key2
            nextRight = next2
            parentRight = parent2

            localLeftData = data1
            localRightData = data2
        else:
            numLeftPointsInTrack = numRightPoints
            numRightPointsInTrack = numLeftPoints

            keyLeft = key2
            nextLeft = next2
            parentLeft = parent2

            keyRight = key1
            nextRight = next1
            parentRight = parent1

            localLeftData = data2
            localRightData = data1

        parentLeft[0] = -1
        next_i = 0
        shift = 0
        currPoint = 0
        otherPoint = 0

        minV = 0
        dist = 0
        isSwitch = True
        gnextParent = 0
        gnext = 0

        while numRightPointsInTrack > 0:
            shift = 0
            currPoint = next_i
            next_i = nextRight[shift]
            minV = float('inf')

            for i in range(numRightPointsInTrack):
                isSwitch = True
                otherPoint = nextRight[i]

                try:
                    dist = localLeftData[currPoint].distanceTo(localRightData[otherPoint])
                    if keyRight[otherPoint] > dist:
                        keyRight[otherPoint] = dist
                        parentRight[otherPoint] = currPoint
                except Exception as e:
                    print("curr, other: ", currPoint, ", ", otherPoint)
                    print(e)

                if keyRight[otherPoint] < minV:
                    shift = i
                    minV = keyRight[otherPoint]
                    next_i = otherPoint

                if dist < keyLeft[currPoint]:
                    keyLeft[currPoint] = dist
                    parentLeft[currPoint] = otherPoint

            gnext = localRightData[next_i].getId()
            gnextParent = localLeftData[parentRight[next_i]].getId()

            for i in range(numLeftPointsInTrack):
                currPoint = nextLeft[i]
                if minV > keyLeft[currPoint]:
                    isSwitch = False
                    minV = keyLeft[currPoint]
                    otherPoint = parentLeft[currPoint]
                    gnextParent = localLeftData[currPoint].getId()
                    gnext = localRightData[otherPoint].getId()
                    next_i = currPoint
                    shift = i

            for i in range(numLeftPointsInTrack):
                currPoint = nextLeft[i]
                otherPoint = parentLeft[currPoint]
            if numLeftPointsInTrack == numLeftPoints and numRightPointsInTrack == numRightPoints:
                ################warn#################"
                nextLeft[0] = nextLeft[numLeftPointsInTrack-1]
                numLeftPointsInTrack -= 1
            edge = Edge(min(gnext, gnextParent), max(gnext, gnextParent), minV)
            edgePairs.append((self.partition_id, edge))
            if not isSwitch:
                nextLeft[shift] = nextLeft[numLeftPointsInTrack-1]
                numLeftPointsInTrack -= 1
                continue
            nextRight[shift] = nextRight[numRightPointsInTrack-1]
            numRightPointsInTrack -= 1

            tmpData = localRightData
            localRightData = localLeftData
            localLeftData = tmpData

            # swap keyLeft and keyRight
            tmpKey = keyRight
            keyRight = keyLeft
            keyLeft = tmpKey

            # swap parentLeft and parentRight
            tmpParent = parentRight
            parentRight = parentLeft
            parentLeft = tmpParent

            # swap nextLeft and nextRight
            tmpNext = nextRight
            nextRight = nextLeft
            nextLeft = tmpNext

            # swap nptsLeft and nptsRight
            tmpNumPoints = numRightPointsInTrack
            numRightPointsInTrack = numLeftPointsInTrack
            numLeftPointsInTrack = tmpNumPoints

        for i in range(numLeftPointsInTrack):
            currPoint = nextLeft[i]
            otherPoint = parentLeft[currPoint]
            minV = keyLeft[currPoint]
            gnextParent = localLeftData[currPoint].getId()
            gnext = localRightData[otherPoint].getId()
            edge = Edge(min(gnext, gnextParent), max(gnext, gnextParent), minV)
            edgePairs.append((self.partition_id, edge))

        #edgePairs.sort(key=lambda x: x[1])
        edgePairs = sorted(edgePairs, key=lambda pair: pair[1].get_weight())
        print("edgePairs:", len(edgePairs))
        return edgePairs

    def getEdgeList(self):
        if self.is_bipartite:
            edgeList = self.BipartiteMST()
        else:
            edgeList = self.prim_local()
        return edgeList
