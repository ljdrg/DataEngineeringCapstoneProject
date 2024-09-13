# Data Dictionary 

### fact_im

This is the fact table in the data model. Its data source is the I94 Immigration data set.

#### Columns & Descirptions
- immigration_id: identifier
- year: year of immigration
- month: month of immigration
- city_code: City code of arrival city
- arrival_date: date when immigrant arrived in the US
- departure_date: date when immigrant left the US
- transportation_mode: mode of transportation of immigrant to get to the US
- visa: type of visa immigrant has
- country: country in which immigrant enters
- state_code: state code of arrival location of immigrant in the US


### dim_im_person

This is a dimension table derived from the I94 Immigration data set.

#### Columns & Descriptions
- immigration_id: identifier
- citizen_country: country of citizenship of immigrant
- residence_country: country of residence of immigrant
- birt_year: year of birth of immigrant
- gender: gender of immigrant


### dim_im_airline

This is a dimension table derived from the I94 Immigration data set.

#### Columns & Descriptions
- immigration_id: identifier
- airline: airline with which the immigrant arrived
- flight_number: flight number of the flight with which the immigrant arrived
- visa_type: visa_type immigrant needs to be allowed to enter country by air


### dim_temp

This is a dimension table derived from the Global Temperature data set.


#### Columns & Descriptions
- date: date and composite identifier
- city: city name and composite identifier
- avg_temperature: average temperature on date and city
- avg_temp_uncertainty: average uncertainty regarding the temperature
- Country: country for temperature data (here only US)
- Longitude: coordinates
- Latitiude: coodinates
- year: year of temperature measurement
- month: month of temperature measurement

### city_code

This is a dimension table derrived from the labels of the I94 Immigration data set.

#### Columns & Descriptions
- city_code: identifier
- city: name of city 


### country_code

This is a dimension table derrived from the labels of the I94 Immigration data set.

#### Columns & Descriptions
- country_code: identifier
- country: name of country 

### state_code

This is a dimension table derrived from the labels of the I94 Immigration data set.

#### Columns & Descriptions
- state_code: identifier
- state: name of state 















