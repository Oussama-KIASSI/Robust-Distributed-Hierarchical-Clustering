from sklearn.metrics import silhouette_score
import networkx as nx
from scipy.sparse import lil_matrix
from sklearn.cluster import AgglomerativeClustering




def silhouette_metric_score(mst_edges,linkage, distance_threshold, n_clusters ):
    graph_data = mst_edges.flatMap(lambda partition : partition[1]).map(lambda edge : (edge.get_left(), edge.get_right(), edge.get_weight()))
    MST = nx.Graph()
    MST.add_weighted_edges_from(graph_data.collect())
    nodes_number = len(MST.nodes)
    dist_matrix = lil_matrix((nodes_number, nodes_number))
    for u, v, d in MST.edges(data=True):
        dist_matrix[u-1, v-1] = d['weight']
        dist_matrix[v-1, u-1] = d['weight']
    dense_matrix = dist_matrix.toarray()
    clustering = AgglomerativeClustering(n_clusters=n_clusters, linkage=linkage, distance_threshold=distance_threshold).fit(dense_matrix)
    labels = clustering.labels_
    silhouette_avg = silhouette_score(dense_matrix, labels)
    print("The silhouette score is:", silhouette_avg)