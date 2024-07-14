
/***********************************************
** File: Assignmend1_ DDLDML.sql
** Desc: Insights based on USpopulation dataset
**       and zillow chicago housing dataset
** Auth: Husein Adenwala
** Date: 1/21/2022
************************************************/

-- Zillow chicago housing data scripts
-- Find mean price for each zipcode inthe zillow housing data  

SELECT zipcode, AVG(price_usd) AS mean_price
  FROM   zillowdata.zillowchicagodata 
    GROUP  BY zipcode;
  
 -- Find house that have greater than 1000 sq feet
SELECT street , city, zipcode, price_usd , floorsize 
  FROM  zillowdata.zillowchicagodata
	WHERE floorsize > 1000;
   
-- Calculate price per square foot WHERE square foot is not =0 AND price not 0
	
SELECT  street , city, zipcode, price_usd , floorsize, price_usd/floorsize AS price_sqft
	FROM  zillowdata.zillowchicagodata 
	  WHERE floorsize > 0 AND price_usd > 0;
	  
--Find the median house price in chicago

SELECT percentile_cont(0.5) WITHIN GROUP(ORDER BY price_usd)
  FROM  zillowdata.zillowchicagodata;
  
-- find median house price GROUP BY zip code 
  
SELECT zipcode, percentile_cont(0.5) WITHIN GROUP(ORDER BY price_usd) 
  FROM  zillowdata.zillowchicagodata 
  GROUP BY zipcode;

 
 -- megering US pupulation data and zillow chicago housing data 
--- join the USdata pouluation AND zillow data to find the mediam price AND poluation of the sqlstate 
 
 SELECT z.state, u.state , median_price, u.population 
   FROM  (SELECT state, percentile_cont(0.5) WITHIN GROUP(ORDER BY price_usd) AS median_price
	  FROM  zillowdata.zillowchicagodata 
	    GROUP BY state ) AS z
 join usdata.us_populationdata AS u ON z.state = UPPER(LEFT(u.state,2));
 
 
 -- US state wise population data scripts
 
 -- find the sate with minimum poulation in US
 
 SELECT state , idyear , population 
   FROM  usdata.us_populationdata 
     ORDER BY population ASC 
       LIMIT 1;
     
 --find the stat with maximum population in usage 
 
  SELECT state , idyear , population 
   FROM  usdata.us_populationdata 
     ORDER BY population DESC 
       LIMIT 1;
       
 -- rank sates by most to least population 
  
 SELECT state, idyear, population , RANK() OVER(ORDER BY population DESC) population_rank
   FROM usdata.us_populationdata;
    