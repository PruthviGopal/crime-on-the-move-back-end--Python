#!flask/bin/python3

import random

from sklearn import neighbors

from sklearn.cluster import KMeans
from sklearn.cluster import SpectralClustering
from sklearn.cluster import AffinityPropagation
from sklearn.cluster import AgglomerativeClustering

# These are the expected columns, all of these columns except for 'label
# should be present in each crime_rows entry passed to the 
# respective clustering algorithms
#column_names = ['label', 'x_cord', 'y_cord', 'id', 'offense', 'report_date']

def _format_clustering(labels, crime_xy, crime_info, column_names, num_clusters=None):
    # Create list of dictionary entries from the crime data and the labels
    # using the keys given in "column_names"
    labeled_crime_xy = [dict(zip(column_names, [str(label)] + list(crime) + list(info))) 
        for label, crime, info in zip(labels, crime_xy, crime_info)]
    # Compute the number of clusters if they are not predetermined
    if num_clusters is None:
        unique_labels = set(labels)
        print("Unique labels: " + str(unique_labels))
        num_clusters = len(unique_labels)
    # Create bounding convex hulls for clusters
    clusters = [[] for n in range(num_clusters)]
    for labeled_crime in labeled_crime_xy:
        clusters[int(labeled_crime['label'])].append(labeled_crime)
    cluster_convex_hulls = {}
    # Compute convex hull for each cluster
    for cluster_number in range(num_clusters):
        cluster_convex_hulls[cluster_number] = compute_convex_hull_gift_wrapping(clusters[cluster_number])
    # Remove labels from convex hull entries
    for cluster_number in range(num_clusters):
        for i in range(len(cluster_convex_hulls[cluster_number])):
            if 'label' in cluster_convex_hulls[cluster_number][i]:
                del cluster_convex_hulls[cluster_number][i]['label']
    return cluster_convex_hulls, clusters

def _is_right_turn(p, q, r):
    """
    Do the vectors pq:qr form a right turn?
    """
    # Use cross product
    v1x = q['x_cord'] - p['x_cord']
    v1y = q['y_cord'] - p['y_cord']
    v2x = r['x_cord'] - q['x_cord']
    v2y = r['y_cord'] - q['y_cord']

    if v1x * v2y - v1y * v2x > 0:
        return False
    else:
        return True

def compute_convex_hull_gift_wrapping(labeled_points):
    # Remove duplicates...
    found_points = {}
    for p in labeled_points:
        key = str(p['x_cord']) + str(p['y_cord'])
        try:
            found_points[key]
        except:
            found_points[key] = p
    labeled_points = [found_points[key] for key in found_points.keys()]
    # find left-most point
    leftmost_index = 0
    for i in range(len(labeled_points)):
        if labeled_points[i]['x_cord'] < labeled_points[leftmost_index]['x_cord']:
            leftmost_index = i
    point_on_hull = labeled_points[leftmost_index]
    convex_hull = []
    i = 0
    while True:
        convex_hull.append(point_on_hull)
        end_point = labeled_points[0]
        for j in range(len(labeled_points)):
            if end_point == point_on_hull\
                    or not _is_right_turn(convex_hull[i], end_point, labeled_points[j]):
                end_point = labeled_points[j]
        i += 1
        point_on_hull = end_point
        if end_point == convex_hull[0]:
            break
    return convex_hull

def random_sampling(crime_xy, num_samples):
    if len(crime_xy) <= num_samples:
        return crime_xy
    print("randomly sampling %d samples out of %d" % (num_samples, len(crime_xy)))
    samples = []
    for n in range(num_samples):
        samples.append(crime_xy[random.randint(0, len(crime_xy) - 1)])
    return samples

def spectral_clustering(crime_rows, column_names, num_clusters, affinity='rbf', n_neighbors=0,
        assign_labels='kmeans'):
    """
        n_clusters : integer, optional
            The dimension of the projection subspace.
        affinity : string, array-like or callable, default ‘rbf’
            If a string, this may be one of ‘nearest_neighbors’, ‘precomputed’, ‘rbf’ 
            or one of the kernels supported by sklearn.metrics.pairwise_kernels.
            Only kernels that produce similarity scores 
                (non-negative values that increase with similarity) should be used. 
                This property is not checked by the clustering algorithm.
        gamma : float
            Scaling factor of RBF, polynomial, exponential chi^2 and sigmoid affinity kernel. 
            Ignored for affinity='nearest_neighbors'.
        degree : float, default=3
            Degree of the polynomial kernel. Ignored by other kernels.
        coef0 : float, default=1
            Zero coefficient for polynomial and sigmoid kernels. Ignored by other kernels.
        n_neighbors : integer
            Number of neighbors to use when constructing the affinity matrix 
            using the nearest neighbors method. Ignored for affinity='rbf'.
        n_init : int, optional, default: 10
            Number of time the k-means algorithm will be run with different 
                centroid seeds. 
            The final results will be the best output of n_init consecutive runs in 
                terms of inertia.
        assign_labels : {‘kmeans’, ‘discretize’}, default: ‘kmeans’
            The strategy to use to assign labels in the embedding space. 
            There are two ways to assign labels after the laplacian embedding. 
            k-means can be applied and is a popular choice. 
            But it can also be sensitive to initialization. 
            Discretization is another approach which is less sensitive to 
            random initialization.
        kernel_params : dictionary of string to any, optional
            Parameters (keyword arguments) and values for kernel passed 
                as callable object. Ignored by other kernels.
    """
    crime_xy = [crime[0:2] for crime in crime_rows]
    crime_info = [crime[2:] for crime in crime_rows]
    #crime_xy = [crime[1:] for crime in crime_rows]
    spectral_clustering = SpectralClustering(
            n_clusters=num_clusters, 
            affinity=affinity, 
            n_neighbors=n_neighbors, 
            assign_labels=assign_labels)
    print("Running spectral clustering....")
    print("length crimexy")
    print(len(crime_xy))
    spectral_clustering_labels = spectral_clustering.fit_predict(
            random_sampling(crime_xy, num_samples=3000))
    print("Formatting......")
    return _format_clustering(spectral_clustering_labels, crime_xy, crime_info,
            column_names, num_clusters=num_clusters)


def k_means(crime_rows, column_names, num_clusters):
    """
        Parameters: 
        n_clusters : int, optional, default: 8
        max_iter : int, default: 300
        Maximum number of iterations of the k-means algorithm for a single run.
        n_init : int, default: 10
            Number of time the k-means algorithm will be run with 
                different centroid seeds. 
            The final results will be the best output of n_init 
                consecutive runs in terms of inertia.
        init : {‘k-means++’, ‘random’ or an ndarray}
                    Method for initialization, defaults to ‘k-means++’:
                    ‘k-means++’ : selects initial cluster centers for k-mean 
                        clustering in a smart way to speed up convergence. 
                        See section Notes in k_init for more details.
                    ‘random’: choose k observations (rows) at random from data 
                        for the initial centroids.
                    If an ndarray is passed, it should be of shape 
                        (n_clusters, n_features) and gives the initial centers.
        precompute_distances : {‘auto’, True, False}
                    Precompute distances (faster but takes more memory).
                    ‘auto’ : do not precompute distances 
                        if n_samples * n_clusters > 12 million. 
                        This corresponds to about 100MB overhead per job 
                        using double precision.
                    True : always precompute distances
                    False : never precompute distances
    """
    crime_xy = [crime[0:2] for crime in crime_rows]
    crime_info = [crime[2:] for crime in crime_rows]
    print("Running K-Means")
    # TODO: Parameterize this
    kmeans = KMeans(n_clusters=num_clusters, 
            max_iter=5000)
    try:
        kmeans_labels = kmeans.fit_predict(crime_xy)
    except:
        return None, None
    print("formatting....")
    return _format_clustering(kmeans_labels, crime_xy, crime_info, 
            column_names, num_clusters=num_clusters)

def affinity_propagation(crime_rows, column_names):
    """
        damping : float, optional, default: 0.5
            Damping factor between 0.5 and 1.
        convergence_iter : int, optional, default: 15
            Number of iterations with no change in the number of estimated 
            clusters that stops the convergence.
        max_iter : int, optional, default: 200
            Maximum number of iterations.
        preference : array-like, shape (n_samples,) or float, optional
            Preferences for each point - points with larger values of preferences 
            are more likely to be chosen as exemplars. 
            The number of exemplars, ie of clusters, is influenced by the input 
            preferences value. If the preferences are not passed as arguments, 
            they will be set to the median of the input similarities.
        affinity : string, optional, default=``euclidean``
            Which affinity to use. At the moment precomputed and euclidean are 
            supported. euclidean uses the negative squared euclidean distance 
            between points.
    """
    crime_xy = [crime[0:2] for crime in crime_rows]
    crime_info = [crime[2:] for crime in crime_rows]
    print("Running Affinity Propagation")
    # TODO: Parameterize this
    affinity_prop = AffinityPropagation()
    #affinity_propagation_labels = affinity_prop.fit_predict(crime_xy)
    affinity_prop.fit(random_sampling(crime_xy, num_samples=5000))
    affinity_propagation_labels = affinity_prop.predict(crime_xy)
    print("formatting....")
    return _format_clustering(affinity_propagation_labels, crime_xy, crime_info, 
            column_names)

def agglomerative_clustering(crime_rows, column_names, num_clusters):
    crime_xy = [crime[0:2] for crime in crime_rows]
    crime_info = [crime[2:] for crime in crime_rows]
    print("Running Agglomerative Clustering")
    agglo_clustering = AgglomerativeClustering(n_clusters=num_clusters, 
            connectivity=neighbors.kneighbors_graph(crime_xy, n_neighbors=2))
    agglomerative_clustering_labels = agglo_clustering.fit_predict(crime_xy)
    print("formatting....")
    return _format_clustering(agglomerative_clustering_labels, 
            crime_xy, crime_info, column_names)