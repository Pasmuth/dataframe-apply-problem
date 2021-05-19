### Issue

I am trying to use `DataFrame.apply()` to add new columns to a dataframe. The number of columns being added is dependent on each row of the original dataframe. There is overlap between the columns created for each row, these overlapping columns should be represented by a single column. 

The apply function seems to work just fine on each individual row of the original dataframe, but throws `ValueError: cannot reindex from a duplicate axis` in the combine phase. I'm not sure how to isolate which axis is being duplicated, since it's hidden behind `.apply()`

To make things more complicated, this process works on various subsets of the data (n = 23565), but for some reason when I try to apply to the whole dataframe it fails. I think there may be a handful of rows that are causing the issue, but I haven't been able to isolate exactly which rows. 

Any advice on isolating the error or clarifying the question is welcome.

### Background

The original dataframe `om` contains columns representing scores, changes in scores, and the date range of the change in score. `om` is indexed on EntityID and Date, where EntityID is a unique identifier for the client receiving scores. I want to incorporate values from another dataframe, `services`, which contains information about services provided to clients indexed on date.

For each row in `om` I want to perform the following transformation:

- Filter `service` by `EntityID` and between `om.ScoreDate` and `om.LastScoreDate`
- Find the sum of `service.Total` by `service.Description`
- Append the resulting series to the original row

#### Info on dataframes
```Python
>>> om.info() 
<class 'pandas.core.frame.DataFrame'>
MultiIndex: 23565 entries, (4198, Timestamp('2018-09-10 00:00:00')) to (69793, Timestamp('2021-04-15 00:00:00'))
Data columns (total 23 columns):
 #   Column                    Non-Null Count  Dtype
---  ------                    --------------  -----
 0   Childcare                 18770 non-null  float64
 1   Childcare_d               7715 non-null   float64
 2   Education                 22010 non-null  float64
 3   Education_d               9468 non-null   float64
 ..  .....                     ..........      ......
 n   Other Domain Columns      n non-null     float64
 ..  .....                     ..........      ......
 20  Program Collecting Score  23565 non-null  object
 21  LastScoreDate             10423 non-null  datetime64[ns]
 22  ScoreDate                 23565 non-null  datetime64[ns]
dtypes: datetime64[ns](2), float64(20), object(1)
memory usage: 4.9+ MB

>>> services.info()
<class 'pandas.core.frame.DataFrame'>
DatetimeIndex: 398966 entries, 2013-04-19 00:00:00 to 2020-07-10 00:00:00
Data columns (total 7 columns):
 #   Column           Non-Null Count   Dtype
---  ------           --------------   -----
 0   EntityID         398966 non-null  int64
 1   Description      398966 non-null  object
 2   Units            398966 non-null  float64
 3   Value            398966 non-null  float64
 4   Unit of Measure  398966 non-null  object
 5   Total            398966 non-null  float64
 6   Program          398966 non-null  object
dtypes: float64(3), int64(1), object(3)
memory usage: 24.4+ MB
```

#### Code Sample

```Python
import pandas as pd

# This function processes a csv and returns a dataframe with service data
services = import_service_data()

# services is passed in as a default parameter, since every function call relies on data from services
def pivot_services(row, services = services):
    print(row.name[0]) # This is the EntityID portion of the row index
	try:
		# Filter services by EntityID matching row index
		client_services = services[services.EntityID == row.name[0]]
		
		# Filter services by date range
		time_frame = client_services[(client_services.index >= row.LastScoreDate) & (client_services.index < row.ScoreDate)]
		
		# Calculate sum service totals by service type
		# This returns a pd.Series
		sums = time_frame.groupby('Description')['Total'].agg(sum) by service type
		
		# Since row is also a pd.Series, they can just be stuck together
		with_totals = pd.concat([row,sums])
		
		# Rename the new series to match the original row name
		with_totals.name = row.name
	
	except IndexError:
		# IndexError is thrown when a client received no services in the date range
		# In this case there is nothing to add to the row, so it just returns the row
		return row
	
	return with_totals
	
# This function processes a csv and returns a dataframe with om data
om = import_final_om()
merged = om.apply(pivot_services, axis = 1)

# Output
Traceback (most recent call last):
  File "C:\CaseWorthy-Documentation\Projects\OM\data_processing.py", line 131, in <module>
    merged = om.apply(pivot_services, axis = 1)
  File "D:\Anaconda3\envs\om\lib\site-packages\pandas\core\frame.py", line 7768, in apply
    return op.get_result()
  File "D:\Anaconda3\envs\om\lib\site-packages\pandas\core\apply.py", line 185, in get_result
    return self.apply_standard()
  File "D:\Anaconda3\envs\om\lib\site-packages\pandas\core\apply.py", line 279, in apply_standard
    return self.wrap_results(results, res_index)
  File "D:\Anaconda3\envs\om\lib\site-packages\pandas\core\apply.py", line 303, in wrap_results
    return self.wrap_results_for_axis(results, res_index)
  File "D:\Anaconda3\envs\om\lib\site-packages\pandas\core\apply.py", line 440, in wrap_results_for_axis
    result = self.infer_to_same_shape(results, res_index)
  File "D:\Anaconda3\envs\om\lib\site-packages\pandas\core\apply.py", line 446, in infer_to_same_shape
    result = self.obj._constructor(data=results)
  File "D:\Anaconda3\envs\om\lib\site-packages\pandas\core\frame.py", line 529, in __init__
    mgr = init_dict(data, index, columns, dtype=dtype)
  File "D:\Anaconda3\envs\om\lib\site-packages\pandas\core\internals\construction.py", line 287, in init_dict
    return arrays_to_mgr(arrays, data_names, index, columns, dtype=dtype)
  File "D:\Anaconda3\envs\om\lib\site-packages\pandas\core\internals\construction.py", line 85, in arrays_to_mgr
    arrays = _homogenize(arrays, index, dtype)
  File "D:\Anaconda3\envs\om\lib\site-packages\pandas\core\internals\construction.py", line 344, in _homogenize
    val = val.reindex(index, copy=False)
  File "D:\Anaconda3\envs\om\lib\site-packages\pandas\core\series.py", line 4345, in reindex
    return super().reindex(index=index, **kwargs)
  File "D:\Anaconda3\envs\om\lib\site-packages\pandas\core\generic.py", line 4811, in reindex
    return self._reindex_axes(
  File "D:\Anaconda3\envs\om\lib\site-packages\pandas\core\generic.py", line 4832, in _reindex_axes
    obj = obj._reindex_with_indexers(
  File "D:\Anaconda3\envs\om\lib\site-packages\pandas\core\generic.py", line 4877, in _reindex_with_indexers
    new_data = new_data.reindex_indexer(
  File "D:\Anaconda3\envs\om\lib\site-packages\pandas\core\internals\managers.py", line 1301, in reindex_indexer
    self.axes[axis]._can_reindex(indexer)
  File "D:\Anaconda3\envs\om\lib\site-packages\pandas\core\indexes\base.py", line 3476, in _can_reindex
    raise ValueError("cannot reindex from a duplicate axis")
ValueError: cannot reindex from a duplicate axis
```



Once all the new rows are created I want them to be put into a single dataframe that has all the same original columns as `om` but with extra columns for the service totals. The new column names will refer to the different `service.Description` values, and the row values will be the totals. There will be `NaNs` when a service of that type was not provided to a given client in the time period. 

I am able to generate this dataframe with subsets of my data, but when I try to apply it to the whole `om` dataframe I get an exception `ValueError: cannot reindex from a duplicate axis`

What I want to end up with looks something like this

```Python
>>> merged.info()
<class 'pandas.core.frame.DataFrame'>
Int64Index: 100 entries, 0 to 99
Data columns (total 49 columns):
 #   Column                                       Non-Null Count  Dtype
---  ------                                       --------------  -----
 0   Childcare                                    79 non-null     float64
 1   Childcare_d                                  37 non-null     float64
 2   Education                                    97 non-null     float64
 3   Education_d                                  85 non-null     float64
 ..  .....                                        ..........      ......
 n   Other Domain Columns                         n non-null      float64
 ..  .....                                        ..........      ......
 20  Program Collecting Score                     100 non-null    object
 21  LastScoreDate                                53 non-null     datetime64[ns]
 22  ScoreDate                                    100 non-null    datetime64[ns]
 ########################################################################
 ##                 The columns below are the new ones                 ##
 ##         The final dataset will have over 100 extra columns         ##
 ########################################################################
 23  Additional Child Tax Credit                  1 non-null      float64
 24  Annual Certification                         1 non-null      float64
 25  Annual Certification and Inspection          4 non-null      float64
 26  Case Management                              1 non-null      float64
 ..  .....                                        ..........      ......
 n   Other Service Type Columns                   n non-null      float64
 ..  .....                                        ..........      ......
 47  Utility Payment                              2 non-null      float64
 48  Voucher Issuance                             2 non-null      float64
dtypes: datetime64[ns](2), float64(46), object(1)
memory usage: 39.1+ KB
```

