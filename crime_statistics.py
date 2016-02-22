# These are the expected columns, all of these columns except for 'label
# should be present in each crime_rows entry passed to the statistics 
# calculating functions
# The entries in crime_rows are expected to be in this order as well
#column_names = ['label', 'x_cord', 'y_cord', 'id', 'offense', 'report_date']

def top_n_crimes(crime_rows, column_names, n=5, offense_column_name=None):
    if offense_column_name is None:
        for col_name in column_names:
            if col_name.find('offense') != -1:
                if offense_column_name is None:
                    offense_column_name = col_name
                else:
                    error_string = "THERE ARE TWO COLUMNS PASSED TO TOP_N_CRIMES"\
                            + "CONTAINING OFFENSE! THIS MUST BE FIXED!"
                    print(error_string)
                    raise RuntimeError(error_string)
    crime_types = {}
    for r in crime_rows:
        try:
            crime_types[r[offense_column_name].lower()] += 1
        except:
            if r[offense_column_name] is not None:
                crime_types[r[offense_column_name].lower()] = 1
    crime_type_tuples = [(crime_type, crime_types[crime_type])
            for crime_type in crime_types.keys()]
    crime_type_tuples.sort(key=lambda x: x[1], reverse=True)
    crime_type_tuples = crime_type_tuples[:n]
    for i in range(len(crime_type_tuples)):
        crime_type_tuples[i] = crime_type_tuples[i][0]
    return crime_type_tuples

def crimes_per_year(crime_rows):
    years = {}
    for r in crime_rows:
        try:
            years[str(r['report_date']).strip()[0:4]] += 1
        except:
            years[str(r['report_date']).strip()[0:4]] = 1
    average = 0
    for year_key in years.keys():
        average = years[year_key]
    average /= len(years)
    average = int(average)
    return average

def crimes_per_week(crime_rows):
    pass

def crimes_per_day(crime_rows):
    pass