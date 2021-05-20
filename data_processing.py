import code
import pandas as pd
import numpy as np
from datetime import datetime


def import_final_om():
	data = pd.read_csv('final_om_dataset.csv')
	data.Date = pd.to_datetime(data.Date, errors = 'coerce')
	data.LastScoreDate = pd.to_datetime(data.LastScoreDate, errors = 'coerce')
	data.ScoreDate = pd.to_datetime(data.ScoreDate, errors = 'coerce')

	return data


def import_service_data():
	services = pd.read_csv('services.csv')
	services.Date = pd.to_datetime(services.Date, errors = 'coerce')
	return services


services = import_service_data()

# Set services to default value
def pivot_services(row, services = services):
	print(row.EntityID)
	try:
		# Filter services by EntityID matching row index
		client_services = services[services.EntityID == row.EntityID]
		# Filter services by date range
		time_frame = client_services[(client_services.Date >= row.LastScoreDate) & (client_services.Date < row.ScoreDate)]
		# Calculate sum service totals by service type
		# This returns a pd.Series
		sums = time_frame.groupby('Description')['Total'].agg(sum) # Sum of service totals by service type
		sums.rename(lambda x: x.replace(' ','_').replace('-','_').replace.('/','_')+'_s', inplace = True)
		# Since row is also a pd.Series, they can just be stuck together
		with_totals = pd.concat([row,sums])
		# Rename the new series to match the original row name
		with_totals.name = row.name
		sums.name = row.name
	except IndexError:
		# IndexError is thrown when a client received no services in the date range
		# In this case there is nothing to add to the row, so it just returns the row
		return row
	except ValueError:
		print('ValueError')
		code.interact(local = locals())
	except:
		print('Other Exception')
		code.interact(local = locals())
	return with_totals

# These work
# om = import_final_om().head(1000)
# Row 4150 is bad
# om.iloc[11882]
# om.iloc[273], 499, 848


# .head(4149)
# 
# 
# .tail(11675)
om = import_final_om()
# pivot_services(om.iloc[4149])
# om = om.drop([4149,5721,9931,10328,10493,10494, 11889])
# om = om.drop([4149,5721,9930,10328,10494,11890])
# om = import_final_om()
serv = om.apply(pivot_services, axis = 1)
data = pd.merge(om, serv, how = 'outer')
# data = pd.read_csv('test_sample.csv')
code.interact(local = locals())
