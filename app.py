#!flask/bin/python3

# TODO: CLOSE DB SESSIONS!!!

from flask import Flask, jsonify
from flask import abort
from flask import request

from sqlalchemy import create_engine
from sqlalchemy import desc
from sqlalchemy import or_
from sqlalchemy import func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
from sqlalchemy.orm import scoped_session, sessionmaker, Query

from geoalchemy2.elements import WKTElement
import geoalchemy2.functions as geo_func
from geoalchemy2.types import Geometry

import hashlib
import json
import time
import os

import clustering
import request_routing
import crime_statistics

from sqlalchemy import PrimaryKeyConstraint

DISTANCE_MARGIN = 0.000001

CACHE_DIR = '.cache/'

app = Flask(__name__)
# Uncomment and add password to make this work
#app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:<PASSWORD>@localhost/census_2013'

# Uncomment and add password to make this work
#engine = create_engine('postgresql://postgres:<PASSWORD>@localhost/census_2013')
Base = declarative_base()
Base.metadata.reflect(engine)

class DCCrime(Base):
    __tablename__ = 'dc_crime'

class NovaCrime(Base):
    __tablename__ = 'nova_crime'

class StateOutlines(Base):
    __tablename__ = 'state_outlines'

class CountyOutlines(Base):
    __tablename__ = 'county_outlines'

MAX_CRIMES_TO_RETURN = 94416

nova_column_mappings = {
                        'id': NovaCrime.id,
                        'report_date': NovaCrime.report_date,
                        'offense': NovaCrime.offense_general,
                        'address': NovaCrime.address,
                        'city': NovaCrime.city,
                        'county': NovaCrime.county,
                        'zip_code': NovaCrime.zip_code,
                        'x_cord': NovaCrime.x_cord,
                        'y_cord': NovaCrime.y_cord,
                        'start_date': NovaCrime.start_date,
                        'end_date': NovaCrime.end_date,
                    }
dc_column_mappings = {
                        'id': DCCrime.id, 
                        'report_date': DCCrime.report_date,
                        'offense': DCCrime.offense,
                        'method': DCCrime.method,
                        'address': DCCrime.address,
                        'x_cord': DCCrime.x_cord,
                        'y_cord': DCCrime.y_cord,
                        'start_date': DCCrime.start_date,
                        'end_date': DCCrime.end_date,
                        'ward': DCCrime.ward,
                        'anc': DCCrime.anc,
                        'district': DCCrime.district,
                    }

################################################################################
#--------------------------Raw Crime--------------------------------------------
################################################################################
@app.route('/spatialdb/dc-crimes-checksum', methods=['GET'])
def get_dc_crime_checksum():
    count = None
    db_session = scoped_session(sessionmaker(bind=engine))
    count = db_session.query(DCCrime.id).count()
    return jsonify({'count': count})

dc_column_names = ['id', 'report_date', 'offense', 
                    'address', 'ward', 'x_cord', 'y_cord']
@app.route('/spatialdb/dc-crime', methods=['GET'])
def get_dc_crime():
    db_session = scoped_session(sessionmaker(bind=engine))
    dc_crime_rows = db_session.query(DCCrime.id, DCCrime.report_date,\
            DCCrime.offense,\
            DCCrime.address, DCCrime.ward,\
            DCCrime.x_cord, DCCrime.y_cord)\
            .order_by(DCCrime.id)\
            .limit(MAX_CRIMES_TO_RETURN)
    db_session.close()
    return jsonify({'dc_crimes': [dict(zip(dc_column_names, dc_crime)) 
        for dc_crime in dc_crime_rows]})

@app.route('/spatialdb/dc-crime-2', methods=['GET'])
def get_dc_crime_2():
    db_session = scoped_session(sessionmaker(bind=engine))
    dc_crime_rows = db_session.query(DCCrime.id, DCCrime.report_date,\
            DCCrime.address, DCCrime.ward,\
            DCCrime.x_cord, DCCrime.y_cord)\
            .order_by(desc(DCCrime.id))\
            .limit(MAX_CRIMES_TO_RETURN)
    db_session.close()
    return jsonify({'dc_crimes': [dict(zip(dc_column_names, dc_crime)) 
        for dc_crime in dc_crime_rows]})

@app.route('/spatialdb/nova-crimes-checksum', methods=['GET'])
def get_nova_crime_checksum():
    count = None
    db_session = scoped_session(sessionmaker(bind=engine))
    count = db_session.query(NovaCrime.id).count()
    db_session.close()
    return jsonify({'count': count})

nova_column_names = ['id','report_date', 'offense_specific', 'offense_general', 
                    'address', 'city', 'county', 'zip_code', 
                    'x_cord', 'y_cord']
@app.route('/spatialdb/nova-crime', methods=['GET'])
def get_nova_crime():
    db_session = scoped_session(sessionmaker(bind=engine))
    nova_crime_rows = db_session.query(NovaCrime.id, NovaCrime.report_date,\
            NovaCrime.offense_specific, NovaCrime.offense_general,\
            NovaCrime.address, NovaCrime.city,\
            NovaCrime.county, NovaCrime.zip_code,\
            NovaCrime.x_cord, NovaCrime.y_cord)\
            .order_by(NovaCrime.id)\
            .all()
    print(nova_crime_rows)
    db_session.close()
    return jsonify({'nova_crimes': [dict(zip(nova_column_names, nova_crime)) 
        for nova_crime in nova_crime_rows]})

def point_list_to_polygon(point_list):
    #'POLYGON((0 0,1 0,1 1,0 1,0 0))'
    polygon = 'POLYGON(('
    for point, i in zip(point_list, range(len(point_list))):
        point = point.replace('(', '')
        point = point.replace(',', '')
        point = point.replace(')', '')
        polygon += point
        if i < (len(point_list) - 1):
            polygon += ", "
    polygon += '))'
    polygon = WKTElement(polygon, srid=4326)
    return polygon

@app.route('/spatialdb/crimes-in-outlines', methods=['GET'])
def get_crimes_in_outlines():
    request_args = request.args
    print("get_crimes_in_outlines")
    print("Omitting request_args for length...")
    #print(request_args)
    outlines = request_args['outlines']
    outlines = json.loads(outlines)
    areas_crimes = {}
    db_session = scoped_session(sessionmaker(bind=engine))
    # Loop over outlines
    for i in range(len(outlines)):
        outline = outlines[i]
        # Compute area id
        outlineMd5hash = hashlib.new('md5')
        for j in range(len(outline)):
            outlineMd5hash.update(str(outline[j]).encode('utf-8'))
        # Get crimes in area
        area_id = outlineMd5hash.hexdigest()
        # Convert outline to a polygon definition
        polygon_outline = point_list_to_polygon(outline)
        # Nova query
        crime_outlines_nova_column_names = ['id', 'report_date', 
                'offense_specific', 'address', 'county', 'x_cord', 'y_cord']
        nova_area_crimes = db_session.query(NovaCrime.id, NovaCrime.report_date,\
                NovaCrime.offense_specific, NovaCrime.address, NovaCrime.county,\
                NovaCrime.x_cord, NovaCrime.y_cord)\
                .filter(func.ST_DWithin(NovaCrime.geom, polygon_outline, DISTANCE_MARGIN))\
                .all()
        nova_area_crimes = [dict(zip(crime_outlines_nova_column_names, nova_crime))
                for nova_crime in nova_area_crimes]

        # DC query
        crime_outlines_dc_column_names = ['id', 'report_date', 
                'offense', 'address', 'ward', 'x_cord', 'y_cord']
        # TODO: Come up with better solution than limiting the query?
        dc_area_crimes = db_session.query(DCCrime.id, DCCrime.report_date,\
                DCCrime.offense, DCCrime.address, DCCrime.ward,\
                DCCrime.x_cord, DCCrime.y_cord)\
                .filter(func.ST_DWithin(DCCrime.geom, polygon_outline, DISTANCE_MARGIN))\
                .limit(MAX_CRIMES_TO_RETURN)\
                .all()
        dc_area_crimes = [dict(zip(crime_outlines_dc_column_names, dc_crime))
                for dc_crime in dc_area_crimes]
        
        # Combine results
        area_crimes = nova_area_crimes + dc_area_crimes
        for area_crime in areas_crimes:
            area_crime.report_date = str(area_crime.report_date)
        areas_crimes[area_id] = [area_crime for area_crime in area_crimes]
    db_session.close()
    return jsonify(areas_crimes=areas_crimes)

################################################################################
#--------------------------Outlines---------------------------------------------
################################################################################
county_names = (
        'Alexandria', 
        'Arlington', 
        'Fairfax', 
        'Fairfax City', 
        'Falls Church', 
        'Loudoun'
)
@app.route('/spatialdb/nova-counties-checksum', methods=['GET'])
def get_nova_county_checksum():
    db_session = scoped_session(sessionmaker(bind=engine))

    nova_counties_count = db_session.query(StateOutlines, CountyOutlines)\
            .join(CountyOutlines, CountyOutlines.statefp == StateOutlines.statefp)\
            .filter(StateOutlines.name == 'Virginia')\
            .filter(or_(CountyOutlines.name == county_name for county_name in county_names))\
            .count()
    db_session.close()
    print(nova_counties_count)
    return jsonify({'count': nova_counties_count})

@app.route('/spatialdb/nova-counties', methods=['GET'])
def get_nova_county_outlines():
    """
    SELECT c.gid,
        s.name AS state_name,
        s.statefp AS geom,
        c.name AS county_name,
        c.geom AS geom
    FROM state_outlines s,
        county_outlines c
    WHERE s.name::text = 'Virginia'::text;
    """
    print("Get Nova County Outlines request!")
    print(request)
    print(request.args)
    request_args = request.args
    db_session = scoped_session(sessionmaker(bind=engine))
    nova_county_rows = db_session.query(StateOutlines, CountyOutlines)\
            .join(CountyOutlines, CountyOutlines.statefp == StateOutlines.statefp)\
            .filter(StateOutlines.name == 'Virginia')\
            .filter(or_(CountyOutlines.name == county_name for county_name in county_names))

    county_outlines = {}
    county_statistics = {}
    
    # Parse out the coordinates
    for state, county in nova_county_rows:
        outline_points = json.loads(db_session.scalar(county.geom.ST_AsGeoJSON()))
        # For some reason there's two absolutely pointless lists containing the
        # content. Like, all they do is enclose another list....wtf mate
        county_outline_points = list()
        for i in range(len(outline_points['coordinates'][0][0])):
            county_point_dict = dict()
            # longitude
            county_point_dict['x_cord'] = outline_points['coordinates'][0][0][i][0]
            # latitude
            county_point_dict['y_cord'] = outline_points['coordinates'][0][0][i][1]
            county_outline_points.append(county_point_dict)
        if county.name in county_outlines:
            county_outlines[county.name + '_2'] = county_outline_points
        else:
            county_outlines[county.name] = county_outline_points

        # Crimes for Statistics
        nova_crime_rows = db_session.query(NovaCrime.id, NovaCrime.report_date,\
                NovaCrime.offense_specific, NovaCrime.offense_general,\
                NovaCrime.address, NovaCrime.city,\
                NovaCrime.county, NovaCrime.zip_code,\
                NovaCrime.x_cord, NovaCrime.y_cord)\
                .filter(NovaCrime.county == county.name)\
                .order_by(NovaCrime.id)
        formatted_nova_crime_rows = [dict(zip(nova_column_names, r)) for r in nova_crime_rows]

        crime_type_tuples = crime_statistics.top_n_crimes(
                formatted_nova_crime_rows,
                column_names=nova_column_names,
                offense_column_name='offense_specific')
        if county.name in county_statistics:
            county_statistics[county.name + '_2'] = {'top_5_crimes': crime_type_tuples}
        else:
            county_statistics[county.name] = {'top_5_crimes': crime_type_tuples}

    db_session.close()
    print("Returned: " + str(len(county_outlines)) + " county outlines")
    return jsonify({'area_outline': county_outlines,
                    'area_statistics': county_statistics})

@app.route('/spatialdb/dc-outline-checksum', methods=['GET'])
def get_dc_outline_checksum():
    db_session = scoped_session(sessionmaker(bind=engine))
    dc_outline_count = db_session.query(StateOutlines)\
            .filter(StateOutlines.name == 'District of Columbia')\
            .count()
    db_session.close()
    return jsonify({'count': dc_outline_count})

@app.route('/spatialdb/dc-outline')
def get_dc_outline():
    print("Get DC Outline request!")
    print(request)
    print(request.args)
    request_args = request.args
    db_session = scoped_session(sessionmaker(bind=engine))
    dc_row = db_session.query(StateOutlines)\
            .filter(StateOutlines.name == 'District of Columbia')
    # Parse out the coordinates
    outline_points = json.loads(db_session.scalar(dc_row[0].geom.ST_AsGeoJSON()))
    # For some reason there's two absolutely pointless lists containing the
    # content. Like, all they do is enclose another list....wtf mate
    dc_outline_points = list()
    for i in range(len(outline_points['coordinates'][0][0])):
        dc_point_dict = dict()
        # longitude
        dc_point_dict['x_cord'] = outline_points['coordinates'][0][0][i][0]
        # latitude
        dc_point_dict['y_cord'] = outline_points['coordinates'][0][0][i][1]
        dc_outline_points.append(dc_point_dict)

    # Crimes for Statistics
    dc_crime_rows = db_session.query(DCCrime.id, DCCrime.report_date,\
            DCCrime.offense,\
            DCCrime.address, DCCrime.ward,\
            DCCrime.x_cord, DCCrime.y_cord)\
            .order_by(DCCrime.id)
    formatted_dc_crime_rows = [dict(zip(dc_column_names, r)) for r in dc_crime_rows]

    crime_type_tuples = crime_statistics.top_n_crimes(
            formatted_dc_crime_rows,
            column_names=dc_column_names,
            offense_column_name='offense')
    db_session.close()
    return jsonify({'area_outline': {'District of Columbia': dc_outline_points},
            'area_statistics': {'District of Columbia': 
                {'top_5_crimes': crime_type_tuples}
            }})

################################################################################
#--------------------------Crime Feature Selection------------------------------
################################################################################
@app.route('/spatialdb/dc-crime-types')
def dc_crime_types():
    db_session = scoped_session(sessionmaker(bind=engine))
    dc_crime_types = db_session.query(DCCrime.offense).group_by(DCCrime.offense).all()
    dc_crime_types = [dc_crime_type[0] for dc_crime_type in dc_crime_types]
    db_session.close()
    return jsonify(dc_crime_types=dc_crime_types)

@app.route('/spatialdb/nova-crime-types')
def nova_crime_types():
    db_session = scoped_session(sessionmaker(bind=engine))
    nova_crime_types = db_session\
            .query(NovaCrime.offense_specific,\
                func.count(NovaCrime.offense_specific).label('num'))\
            .group_by(NovaCrime.offense_specific)\
            .order_by(desc('num'))\
            .limit(50000)\
            .all()
    nova_crime_types = [nova_crime_type[0] for nova_crime_type in nova_crime_types]
    db_session.close()
    return jsonify(nova_crime_types=nova_crime_types)

################################################################################
#--------------------------Clustering-------------------------------------------
################################################################################

def get_nova_data_for_clustering(request_args):
    # Crime levels to consider
    nova_crimes_considered = None
    try:
        # JSON dictionary with two keys ('dc' and 'nova') to two lists of crimes
        crimes_considered = request_args.get('crimes_considered')
        crimes_considered = json.loads(crimes_considered)
        nova_crimes_considered = json.loads(crimes_considered['nova'])
    except:
        # Consider all crimes
        crimes_considered = None
    print("Nova Crimes Considered: " + str(nova_crimes_considered))
    # Areas
    try:
        outlines = request_args.get('area_outline')
        outlines = json.loads(outlines)
        polygon_outline = point_list_to_polygon(outlines)
    except:
        polygon_outline = None
    db_session = scoped_session(sessionmaker(bind=engine))
    nova_crime_rows = None
    if 'nova_data' in request_args:
        print("Nova data!!!")
        nova_counties_req = json.loads(request_args['nova_data'])
        # Take all crimes, no filter
        if nova_crimes_considered is None:
            print("nova_crimes_considered is None")
            if polygon_outline is not None:
                print("polygon_outline is NOT None")
                nova_crime_rows = db_session.query(NovaCrime.x_cord, NovaCrime.y_cord, 
                        NovaCrime.id, NovaCrime.offense_specific, NovaCrime.report_date)\
                        .filter(NovaCrime.county.in_(nova_counties_req))\
                        .filter(func.ST_DWithin(NovaCrime.geom, polygon_outline, DISTANCE_MARGIN))\
                        .order_by(NovaCrime.id)\
                        .all()
            else:
                print("polygon_outline is None")
                nova_crime_rows = db_session.query(NovaCrime.x_cord, NovaCrime.y_cord, 
                        NovaCrime.id, NovaCrime.offense_specific, NovaCrime.report_date)\
                        .filter(NovaCrime.county.in_(nova_counties_req))\
                        .order_by(NovaCrime.id)\
                        .all()
        # Filter by crimes considered
        else:
            print("nova_crimes_considered is NOT None")
            if polygon_outline is not None:
                print("polygon_outline is not None")
                nova_crime_rows = db_session.query(NovaCrime.x_cord, NovaCrime.y_cord, 
                        NovaCrime.id, NovaCrime.offense_specific, NovaCrime.report_date)\
                        .filter(NovaCrime.county.in_(nova_counties_req))\
                        .filter(NovaCrime.offense_specific.in_(nova_crimes_considered))\
                        .filter(func.ST_DWithin(NovaCrime.geom, polygon_outline, DISTANCE_MARGIN))\
                        .order_by(NovaCrime.id)\
                        .all()
            else:
                print("polygon_outline is None")
                nova_crime_rows = db_session.query(NovaCrime.x_cord, NovaCrime.y_cord, 
                        NovaCrime.id, NovaCrime.offense_specific, NovaCrime.report_date)\
                        .filter(NovaCrime.county.in_(nova_counties_req))\
                        .filter(NovaCrime.offense_specific.in_(nova_crimes_considered))\
                        .order_by(NovaCrime.id)\
                        .all()
    db_session.close()  
    return nova_crime_rows

def get_dc_data_for_clustering(request_args):
    # Crime levels to consider
    dc_crimes_considered = None
    try:
        # JSON dictionary with two keys ('dc' and 'dc') to two lists of crimes
        crimes_considered = request_args.get('crimes_considered')
        crimes_considered = json.loads(crimes_considered)
        dc_crimes_considered = json.loads(crimes_considered['dc'])
    except:
        # Consider all crimes
        crimes_considered = None
    # Areas
    try:
        outlines = request_args.get('area_outline')
        outlines = json.loads(outlines)
        polygon_outline = point_list_to_polygon(outlines)
    except:
        polygon_outline = None
    db_session = scoped_session(sessionmaker(bind=engine))
    dc_crime_rows = None
    if 'dc_data' in request_args:
        print("DC data!!!")
        # Take all crimes, no filter
        if dc_crimes_considered is None:
            print("dc_crimes_considered is None")
            if polygon_outline is not None:
                print("polygon_outline is NOT None")
                dc_crime_rows = db_session.query(DCCrime.x_cord, DCCrime.y_cord, 
                        DCCrime.id, DCCrime.offense, DCCrime.report_date)\
                        .filter(func.ST_DWithin(DCCrime.geom, polygon_outline, DISTANCE_MARGIN))\
                        .order_by(DCCrime.id)\
                        .all()
            else:
                print("polygon_outline is None")
                dc_crime_rows = db_session.query(DCCrime.x_cord, DCCrime.y_cord, 
                        DCCrime.id, DCCrime.offense, DCCrime.report_date)\
                        .order_by(DCCrime.id)\
                        .all()
        # Filter by crimes considered
        else:
            print("dc_crimes_considered is NOT None")
            if polygon_outline is not None:
                print("polygon_outline is not None")
                dc_crime_rows = db_session.query(DCCrime.x_cord, DCCrime.y_cord, 
                        DCCrime.id, DCCrime.offense, DCCrime.report_date)\
                        .filter(DCCrime.offense.in_(dc_crimes_considered))\
                        .filter(func.ST_DWithin(DCCrime.geom, polygon_outline, DISTANCE_MARGIN))\
                        .order_by(DCCrime.id)\
                        .all()
            else:
                print("polygon_outline is None")
                dc_crime_rows = db_session.query(DCCrime.x_cord, DCCrime.y_cord, 
                        DCCrime.id, DCCrime.offense, DCCrime.report_date)\
                        .filter(DCCrime.offense.in_(dc_crimes_considered))\
                        .order_by(DCCrime.id)\
                        .all()
    db_session.close()  
    return dc_crime_rows

clustering_nova_column_names = ['label', 'x_cord', 'y_cord', 'id', 
        'offense_specific', 'report_date']
clustering_dc_column_names = ['label', 'x_cord', 'y_cord', 'id', 
        'offense', 'report_date']
@app.route('/spatialdb/clustering/kmeans', methods=['GET'])
def k_means():
    print("K-Means request!")
    #print(request)
    #print(request.args)
    request_args = request.args
    # Number of clusters
    try:
        num_clusters = request_args.get('num_clusters')
        num_clusters = int(num_clusters)
    except:
        print("Return some error response...")
        # TODO: Raise error response, and nix the default value
        num_clusters = 4
    db_session = scoped_session(sessionmaker(bind=engine))
    dc_crime_return_dict = None
    nova_crime_return_dict = None
    # data set(s) to be queries

    dc_crime_rows = get_dc_data_for_clustering(request_args)
    if dc_crime_rows is not None:
        dc_crime_return_dict = request_routing.k_means(dc_crime_rows,
                clustering_dc_column_names, num_clusters)

    nova_crime_rows = get_nova_data_for_clustering(request_args)
    if nova_crime_rows is not None:
        # Perform K-Means for nova data
        nova_crime_return_dict = request_routing.k_means(nova_crime_rows, 
                clustering_nova_column_names, num_clusters)
    db_session.close()    

    # Handle data combination and return
    if nova_crime_return_dict and dc_crime_return_dict:
        return_dict = nova_crime_return_dict
        # Loop over DC crime return dict and combine into return_dict
        # by continuing the cluster count from Nova crime return dict
        for top_level_key in dc_crime_return_dict.keys():
            for cluster_key in dc_crime_return_dict[top_level_key].keys():
                return_dict[top_level_key][cluster_key + num_clusters]\
                        = dc_crime_return_dict[top_level_key][cluster_key]
    elif nova_crime_return_dict:
        return_dict = nova_crime_return_dict
    elif dc_crime_return_dict:
        return_dict = dc_crime_return_dict
    else:
        print("Failure...since dc_crime_return_dict and nova_crime_return_dict are none?")
        return jsonify(failure="too few rows, decrease num clusters. "
                + "Also, see server logs")

    print("Sending K-Means clustering results back to client...")
    return jsonify(return_dict)

@app.route('/spatialdb/clustering/spectral-clustering', methods=['GET'])
def spectral_clustering():
    print("Spectral clustering request!")
    print(request)
    print(request.args)
    """
    request_args = request.args
    # Number of clusters
    try:
        num_clusters = request_args.get('num_clusters')
        num_clusters = int(num_clusters)
    except:
        print("Return some error response...")
        # TODO: Raise error response, and nix the default value
        num_clusters = 4
    # Crime levels to consider
    dc_crimes_considered = None
    nova_crimes_considered = None
    try:
        # JSON dictionary with two keys ('dc' and 'nova') to two lists of crimes
        crimes_considered = request_args.get('crimes_considered')
        crimes_considered = json.loads(crimes_considered)
        dc_crimes_considered = crimes_considered['dc']
        nova_crimes_considered = crimes_considered['nova']
    except:
        # Consider all crimes
        crimes_considered = None
    db_session = scoped_session(sessionmaker(bind=engine))
    dc_crime_return_dict = None
    nova_crime_return_dict = None
    # data set(s) to be queries
    if 'dc_data' in request_args:
        # Take all crimes, no filter
        if dc_crimes_considered is None:
            dc_crime_rows = db_session.query(DCCrime.x_cord, DCCrime.y_cord,
                    DCCrime.id, DCCrime.offense, DCCrime.report_date)\
                    .order_by(DCCrime.id)
        # Filter by crimes_considered
        else:
            dc_crime_rows = db_session.query(DCCrime.x_cord, DCCrime.y_cord,
                    DCCrime.id, DCCrime.offense, DCCrime.report_date)\
                    .filter(DCCrime.offense.in_(dc_crimes_considered))\
                    .order_by(DCCrime.id)
        # Perform Spectral Clustering
        dc_crime_return_dict = request_routing.spectral_clustering(
                dc_crime_rows, clustering_dc_column_names, num_clusters)
    if 'nova_data' in request_args:
        nova_counties_req = json.loads(request_args['nova_data'])
        # Take all crimes, no filter
        if nova_crimes_considered is None:
            nova_crime_rows = db_session.query(NovaCrime.x_cord, NovaCrime.y_cord, 
                    NovaCrime.id, NovaCrime.offense_specific, NovaCrime.report_date)\
                    .filter(NovaCrime.county.in_(nova_counties_req))\
                    .order_by(NovaCrime.id)
        # Filter by crimes_considered
        else:
            nova_crime_rows = db_session.query(NovaCrime.x_cord, NovaCrime.y_cord, 
                    NovaCrime.id, NovaCrime.offense_specific, NovaCrime.report_date)\
                    .filter(NovaCrime.county.in_(nova_counties_req))\
                    .filter(NovaCrime.offense_specific.in_(nova_crimes_considered))\
                    .order_by(NovaCrime.id)
        # Perform Spectral Clustering
        nova_crime_return_dict = request_routing.spectral_clustering(
                nova_crime_rows, clustering_nova_column_names, num_clusters)

    # Handle data combination and return
    if nova_crime_return_dict and dc_crime_return_dict:
        return_dict = nova_crime_return_dict
        # Loop over DC crime return dict and combine into return_dict
        # by continuing the cluster count from Nova crime return dict
        for top_level_key in dc_crime_return_dict.keys():
            for cluster_key in dc_crime_return_dict[top_level_key].keys():
                return_dict[top_level_key][cluster_key + num_clusters]\
                        = dc_crime_return_dict[top_level_key][cluster_key]
    elif nova_crime_return_dict:
        return_dict = nova_crime_return_dict
    else:
        return_dict = dc_crime_return_dict
    """
    #print(request)
    #print(request.args)
    request_args = request.args
    # Number of clusters
    try:
        num_clusters = request_args.get('num_clusters')
        num_clusters = int(num_clusters)
    except:
        print("Return some error response...")
        # TODO: Raise error response, and nix the default value
        num_clusters = 4
    db_session = scoped_session(sessionmaker(bind=engine))
    dc_crime_return_dict = None
    nova_crime_return_dict = None

    # data set(s) to be queries
    dc_crime_rows = get_dc_data_for_clustering(request_args)
    if dc_crime_rows is not None and len(dc_crime_rows) > 0:
        print("length dc_crime_rows")
        print(len(dc_crime_rows))
        dc_crime_return_dict = request_routing.spectral_clustering(dc_crime_rows, 
                clustering_dc_column_names, num_clusters)

    nova_crime_rows = get_nova_data_for_clustering(request_args)
    if nova_crime_rows is not None and len(nova_crime_rows) > 0:
        print("length nova_crime_rows")
        print(len(nova_crime_rows))
        # Perform K-Means for nova data
        nova_crime_return_dict = request_routing.spectral_clustering(
                nova_crime_rows, clustering_nova_column_names, num_clusters)

    # Handle data combination and return
    if nova_crime_return_dict and dc_crime_return_dict:
        return_dict = nova_crime_return_dict
        # Loop over DC crime return dict and combine into return_dict
        # by continuing the cluster count from Nova crime return dict
        for top_level_key in dc_crime_return_dict.keys():
            for cluster_key in dc_crime_return_dict[top_level_key].keys():
                return_dict[top_level_key][cluster_key + num_clusters]\
                        = dc_crime_return_dict[top_level_key][cluster_key]
    elif nova_crime_return_dict:
        return_dict = nova_crime_return_dict
    elif dc_crime_return_dict:
        return_dict = dc_crime_return_dict
    else:
        print("Failure...since dc_crime_return_dict and nova_crime_return_dict are none?")
        return jsonify(failure="too few rows, decrease num clusters. "
                + "Also, see server logs")

    db_session.close()
    print("Sending Spectral Clustering results back to client...")
    return jsonify(return_dict)

@app.route('/spatialdb/clustering/affinity-propagation', methods=['GET'])
def affinity_propagation():
    print("Affinity Propagation request!")
    print(request)
    print(request.args)
    request_args = request.args
    # Crime levels to consider
    dc_crimes_considered = None
    nova_crimes_considered = None
    try:
        # JSON dictionary with two keys ('dc' and 'nova') to two lists of crimes
        crimes_considered = request_args.get('crimes_considered')
        crimes_considered = json.loads(crimes_considered)
        dc_crimes_considered = crimes_considered['dc']
        nova_crimes_considered = crimes_considered['nova']
    except:
        # Consider all crimes
        crimes_considered = None
    db_session = scoped_session(sessionmaker(bind=engine))
    dc_crime_return_dict = None
    nova_crime_return_dict = None
    # data set(s) to be queries
    if 'dc_data' in request_args:
        # Take all crimes, no filter
        if dc_crimes_considered is None:
            dc_crime_rows = db_session.query(DCCrime.x_cord, DCCrime.y_cord,
                    DCCrime.id, DCCrime.offense, DCCrime.report_date)\
                    .order_by(DCCrime.id)
        # Filter by crimes_considered
        else:
            dc_crime_rows = db_session.query(DCCrime.x_cord, DCCrime.y_cord,
                    DCCrime.id, DCCrime.offense, DCCrime.report_date)\
                    .filter(DCCrime.offense.in_(dc_crimes_considered))\
                    .order_by(DCCrime.id)
        # Perform Spectral Clustering
        dc_crime_return_dict = request_routing.affinity_propagation(
                dc_crime_rows, clustering_dc_column_names)
    if 'nova_data' in request_args:
        nova_counties_req = json.loads(request_args['nova_data'])
        # Take all crimes, no filter
        if nova_crimes_considered is None:
            nova_crime_rows = db_session.query(NovaCrime.x_cord, NovaCrime.y_cord, 
                    NovaCrime.id, NovaCrime.offense_specific, NovaCrime.report_date)\
                    .filter(NovaCrime.county.in_(nova_counties_req))\
                    .order_by(NovaCrime.id)
        # Filter by crimes_considered
        else:
            nova_crime_rows = db_session.query(NovaCrime.x_cord, NovaCrime.y_cord, 
                    NovaCrime.id, NovaCrime.offense_specific, NovaCrime.report_date)\
                    .filter(NovaCrime.county.in_(nova_counties_req))\
                    .filter(NovaCrime.offense_specific.in_(nova_crimes_considered))\
                    .order_by(NovaCrime.id)
        # Perform Spectral Clustering
        nova_crime_return_dict = request_routing.affinity_propagation(
                nova_crime_rows, clustering_nova_column_names)

    # Handle data combination and return
    if nova_crime_return_dict and dc_crime_return_dict:
        num_nova_clusters = len(nova_crime_return_dict['area_outline'])
        return_dict = nova_crime_return_dict
        # Loop over DC crime return dict and combine into return_dict
        # by continuing the cluster count from Nova crime return dict
        for top_level_key in dc_crime_return_dict.keys():
            for cluster_key in dc_crime_return_dict[top_level_key].keys():
                return_dict[top_level_key][cluster_key + num_nova_clusters]\
                        = dc_crime_return_dict[top_level_key][cluster_key]
    elif nova_crime_return_dict:
        return_dict = nova_crime_return_dict
    else:
        return_dict = dc_crime_return_dict

    db_session.close()
    print("Sending Affinity Propagation results back to client...")
    return jsonify(return_dict)

@app.route('/spatialdb/clustering/agglomerative-clustering', methods=['GET'])
def agglomerative_clustering():
    print("Agglomerative clustering request!")
    print(request)
    print(request.args)
    request_args = request.args
    # Number of clusters
    try:
        num_clusters = request_args.get('num_clusters')
        num_clusters = int(num_clusters)
    except:
        print("Return some error response...")
        # TODO: Raise error response, and nix the default value
        num_clusters = 4
    # Crime levels to consider
    dc_crimes_considered = None
    nova_crimes_considered = None
    try:
        # JSON dictionary with two keys ('dc' and 'nova') to two lists of crimes
        crimes_considered = request_args.get('crimes_considered')
        crimes_considered = json.loads(crimes_considered)
        dc_crimes_considered = crimes_considered['dc']
        nova_crimes_considered = crimes_considered['nova']
    except:
        # Consider all crimes
        crimes_considered = None
    db_session = scoped_session(sessionmaker(bind=engine))
    dc_crime_return_dict = None
    nova_crime_return_dict = None
    # data set(s) to be queries
    if 'dc_data' in request_args:
        # Take all crimes, no filter
        if dc_crimes_considered is None:
            dc_crime_rows = db_session.query(DCCrime.x_cord, DCCrime.y_cord,
                    DCCrime.id, DCCrime.offense, DCCrime.report_date)\
                    .order_by(DCCrime.id)
        # Filter by crimes_considered
        else:
            dc_crime_rows = db_session.query(DCCrime.x_cord, DCCrime.y_cord,
                    DCCrime.id, DCCrime.offense, DCCrime.report_date)\
                    .filter(DCCrime.offense.in_(dc_crimes_considered))\
                    .order_by(DCCrime.id)
        # Perform Spectral Clustering
        dc_crime_return_dict = request_routing.agglomerative_clustering(
                dc_crime_rows, clustering_dc_column_names, num_clusters)
    if 'nova_data' in request_args:
        nova_counties_req = json.loads(request_args['nova_data'])
        # Take all crimes, no filter
        if nova_crimes_considered is None:
            nova_crime_rows = db_session.query(NovaCrime.x_cord, NovaCrime.y_cord, 
                    NovaCrime.id, NovaCrime.offense_specific, NovaCrime.report_date)\
                    .filter(NovaCrime.county.in_(nova_counties_req))\
                    .order_by(NovaCrime.id)
        # Filter by crimes_considered
        else:
            nova_crime_rows = db_session.query(NovaCrime.x_cord, NovaCrime.y_cord, 
                    NovaCrime.id, NovaCrime.offense_specific, NovaCrime.report_date)\
                    .filter(NovaCrime.county.in_(nova_counties_req))\
                    .filter(NovaCrime.offense_specific.in_(nova_crimes_considered))\
                    .order_by(NovaCrime.id)
        # Perform Spectral Clustering
        nova_crime_return_dict = request_routing.agglomerative_clustering(
                nova_crime_rows, clustering_nova_column_names, num_clusters)

    # Handle data combination and return
    if nova_crime_return_dict and dc_crime_return_dict:
        return_dict = nova_crime_return_dict
        # Loop over DC crime return dict and combine into return_dict
        # by continuing the cluster count from Nova crime return dict
        for top_level_key in dc_crime_return_dict.keys():
            for cluster_key in dc_crime_return_dict[top_level_key].keys():
                return_dict[top_level_key][cluster_key + num_clusters]\
                        = dc_crime_return_dict[top_level_key][cluster_key]
    elif nova_crime_return_dict:
        return_dict = nova_crime_return_dict
    else:
        return_dict = dc_crime_return_dict

    db_session.close()
    print("Sending Agglomerative Clustering results back to client...")
    return jsonify(return_dict)

################################################################################
#-------------------------Pre-computation---------------------------------------
################################################################################
def precompute_and_cache_cluster_values():
    print("Precomputing spectral clustering values....")
    db_session = scoped_session(sessionmaker(bind=engine))
    # TODO: Change limit
    dc_crime_rows = db_session.query(DCCrime.id,\
        DCCrime.x_cord, DCCrime.y_cord).order_by(DCCrime.id).limit(5000)
    print("Running on DC data...")
    _iterate_over_params(dc_crime_rows)

    nova_crime_rows = db_session.query(NovaCrime.id,\
        NovaCrime.x_cord, NovaCrime.y_cord).order_by(NovaCrime.id).limit(5000)
    print("Running on Nova data...")
    _iterate_over_params(nova_crime_rows)

def _iterate_over_params(crime_rows):
    # Compute hash over crime_rows
    md5hash = hashlib.new('md5')
    for r in crime_rows:
        md5hash.update(json.dumps(r, sort_keys=True).encode('utf-8'))
    for num_clusters in range(3, 4):#50):
        for affinity in ['rbf', 'nearest_neighbors']:
            if affinity == 'nearest_neighbors':
                for num_neighbors in range(2, 3):#100):
                    md5hash_copy = md5hash.copy()
                    md5hash_copy.update(num_clusters.to_bytes(2, "little"))
                    md5hash_copy.update(num_neighbors.to_bytes(2, "little"))
                    md5hash_copy.update(affinity.encode('utf-8'))
                    print("Running with params: num_clusters=" 
                        + str(num_clusters) +  "  affinity=" + affinity 
                        + "  num_neighbors=" + str(num_neighbors))
                    cluster_convex_hulls = clustering.spectral_clustering(
                            crime_rows, num_clusters, affinity, num_neighbors)
                    with open(CACHE_DIR + md5hash.hexdigest() + '.cache', 'w') as new_cache_file:
                        json.dump(cluster_convex_hulls, new_cache_file)
            else:
                md5hash_copy = md5hash.copy()
                md5hash_copy.update(num_clusters.to_bytes(2, "little"))
                md5hash_copy.update(affinity.encode('utf-8'))
                print("Running with params: num_clusters=" 
                        + str(num_clusters) +  "  affinity=" + affinity)
                cluster_convex_hulls = clustering.spectral_clustering(
                        crime_rows, num_clusters)
                with open(CACHE_DIR + md5hash.hexdigest() + '.cache', 'w') as new_cache_file:
                    json.dump(cluster_convex_hulls, new_cache_file)

def test_query():
    db_session = scoped_session(sessionmaker(bind=engine))
    for item in db_session.query(DCCrime.id, DCCrime.report_date, DCCrime.address):
        print(item)

if __name__ == '__main__':
    app.run(host='0.0.0.0', threaded=True, debug=True)
    #precompute_and_cache_cluster_values()
