def top_n_crimes(crime_rows, n=5):
    crime_types = {}
    for r in crime_rows:
        try:
            crime_types[r['offense']] += 1
        except:
            crime_types[r['offense']] = 1
    crime_type_tuples = [(crime_type, crime_types[crime_type])
            for crime_type in crime_types.keys()]
    crime_type_tuples.sort(key=lambda x: x[1])
    return crime_type_tuples[:5]

def crime_per_year(crime_rows):
    pass