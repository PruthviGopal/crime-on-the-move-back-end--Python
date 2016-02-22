#!flask/bin/python3

import hashlib
import json
import time
import os
from datetime import datetime
from datetime import date

import clustering
import crime_statistics

USE_CACHING = False
CACHE_DIR = '.cache/'

def k_means(crime_rows, column_names, num_clusters):
    # Compute hash over all data and parameters
    md5hash = hashlib.new('md5')
    md5hash.update('k_means'.encode('utf-8'))
    md5hash.update(num_clusters.to_bytes(2, "little"))
    return _cache_hit_or_run(md5hash, 
            clustering.k_means, crime_rows, column_names, num_clusters)

def spectral_clustering(crime_rows, column_names, num_clusters):
    # Compute hash over all data and parameters
    md5hash = hashlib.new('md5')
    md5hash.update('spectral_clustering'.encode('utf-8'))
    md5hash.update(num_clusters.to_bytes(2, "little"))
    return _cache_hit_or_run(md5hash, clustering.spectral_clustering, 
        crime_rows, column_names, num_clusters)

def affinity_propagation(crime_rows, column_names):
    # Compute hash over all data and parameters
    md5hash = hashlib.new('md5')
    md5hash.update('affinity_propagation'.encode('utf-8'))
    return _cache_hit_or_run(md5hash, clustering.affinity_propagation, 
        crime_rows, column_names, num_clusters=None)    

def agglomerative_clustering(crime_rows, column_names, num_clusters):
    # Compute hash over all data and parameters
    md5hash = hashlib.new('md5')
    md5hash.update('agglomerative_clustering'.encode('utf-8'))
    return _cache_hit_or_run(md5hash, clustering.agglomerative_clustering, 
        crime_rows, column_names, num_clusters)

# Class used to handle JSON encoding of datetime objects
class DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%dT%H:%M:%SZ')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)

def _cache_hit_or_run(md5hash, func, crime_rows, column_names, num_clusters=None):
    convex_hulls_file_path = CACHE_DIR\
             + 'convex_hulls__' + md5hash.hexdigest() + '.cache'
    cluster_points_file_path = CACHE_DIR\
            + 'cluster_points__' + md5hash.hexdigest() + '.cache'
    if USE_CACHING:
        for r in crime_rows:
            formatted_r = list(r)
            formatted_r[4] = str(r[4])
            md5hash.update(json.dumps(formatted_r, sort_keys=True).encode('utf-8'))
        md5hash.update(func.__name__.encode('utf-8'))
        cached_file = None
        # Check for cached version
        print("Files in Cache: ")
        print("current hash: " + md5hash.hexdigest())
        for f in os.listdir(CACHE_DIR):
            print(f)
            if f == (md5hash.hexdigest() + '.cache'):
                cached_file = f
                break
    # Check for cached version or compute and cache
    if not USE_CACHING or cached_file is None:
        start_time = time.perf_counter()
        if num_clusters is not None:
            cluster_convex_hulls, cluster_points = func(crime_rows, column_names, num_clusters)
        else:
            cluster_convex_hulls, cluster_points = func(crime_rows, column_names)
        if cluster_convex_hulls is None or cluster_points is None:
            return None
        end_time = time.perf_counter()
        print("Clustering ran in time: " + str(end_time - start_time))
        if USE_CACHING:
            with open(convex_hulls_file_path, 'w') as new_cache_file:
                json.dump(cluster_convex_hulls, new_cache_file, cls=DatetimeEncoder)
            with open(cluster_points_file_path, 'w') as new_cache_file:
                json.dump(cluster_points, new_cache_file, cls=DatetimeEncoder)
    else:
        print("RETRIEVED CACHED VERSION!")
        with open(convex_hulls_file_path, 'r') as cache_file:
            cluster_convex_hulls = json.load(cache_file, cls=DatetimeEncoder)
        with open(cluster_points_file_path, 'r') as cache_file:
            cluster_points = json.load(cache_file, cls=DatetimeEncoder)
    statistics = {}
    for i in range(len(cluster_points)):
        cluster_statistics = {}
        cluster_statistics['top_5_crimes'] = crime_statistics.top_n_crimes(
                cluster_points[i], column_names, n=5)
        """
        cluster_statistics['crime_per_year'] = crime_statistics.crimes_per_year(
                cluster_points[i])
        """
        statistics[i] = cluster_statistics
    return_dict = {}
    return_dict['area_outline'] = cluster_convex_hulls
    return_dict['statistics'] = statistics
    return return_dict