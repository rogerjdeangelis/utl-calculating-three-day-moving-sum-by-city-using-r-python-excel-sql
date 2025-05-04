%let pgm=utl-calculating-three-day-moving-sum-by-city-using-r-python-excel-sql;

%stop_submission;

Calculating three day moving sum by city using r python excel sql

peplexity AI
how do i calculate the moving average using sqlite by group and set incomplete averages to null using sqllite

  CONTENTS

    1 r sql
    2 python sql
    3 excel sql
      see https://tinyurl.com/4e6yaap8
    4 related repos

github
https://tinyurl.com/5dyvncfn
https://github.com/rogerjdeangelis/utl-calculating-three-day-moving-sum-by-city-using-r-python-excel-sql

related to
https://tinyurl.com/5y37zhnk
https://communities.sas.com/t5/SAS-Programming/Data-Step-Consecutive-Sum/m-p/811581#M320128

SOAPBOX ON
  SQL doe not have a sense of order.
  Algorithmic flexibility is often enhanced when you table has a primary key
  Many relational databases require a primary key?
  I added a primary key, primkey, to enable sequential processing in sqlite
SOAPBOX OFF


/**************************************************************************************************************************/
/*          INPUT        |                 PROCESS                                |        OUTPUT                         */
/*          =====        |                 =======                                |       =======                         */
/*                       |                                                        |                                       */
/* PRIMKEY  CITY SALES   | Set first 2 values to missing.                         | PRIMKEY CITY SALES  THREESUM          */
/*                       | Sum rolling sums of three by city.                     |                                       */
/*     1    NYC    11    |                                                        |       1  NYC    11     NA             */
/*     2    NYC     0    |                                                        |       2  NYC     0     NA             */
/*     3    NYC    12    |                                                        |       3  NYC    12     23  11+0+12    */
/*     4    NYC    13    |                                                        |       4  NYC    13     25  0+12+13    */
/*     5    NYC    14    |                                                        |       5  NYC    14     39  12+13+14   */
/*     6    NYC    15    |                                                        |       6  NYC    15     42  13+14+15   */
/*     7    LA      0    |                                                        |                                       */
/*     8    LA     21    |                                                        |       7   LA     0     NA             */
/*     9    LA     22    |                                                        |       8   LA    21     NA             */
/*    10    LA     23    |                                                        |      19   LA    22     43             */
/*    11    LA     24    |                                                        |      10   LA    23     66             */
/*                       |                                                        |       1   LA    24     69             */
/*------------------------------------------------------------------------------------------------------------------------*/
/* options               |  2 R SQL                                               | R                                     */
/*  validvarname=upcase; |  ======                                                |                                       */
/* libname sd1 "d:/sd1"; |                                                        | PRIMKEY CITY SALES THREESUM           */
/* data sd1.have;        |  proc datasets lib=sd1 nolist nodetails;               |                                       */
/*  input                |   delete want;                                         |       1  NYC    11       NA           */
/*   primkey city $      |  run;quit;                                             |       2  NYC     0       NA           */
/*   sales;              |                                                        |       3  NYC    12       23           */
/* cards;                |  %utl_rbeginx;                                         |       4  NYC    13       25           */
/* 1 NYC 11              |  parmcards4;                                           |       5  NYC    14       39           */
/* 2 NYC 0               |  library(haven)                                        |       6  NYC    15       42           */
/* 3 NYC 12              |  library(sqldf)                                        |       7   LA     0       NA           */
/* 4 NYC 13              |  source("c:/oto/fn_tosas9x.R")                         |       8   LA    21       NA           */
/* 5 NYC 14              |  options(sqldf.dll = "d:/dll/sqlean.dll")              |       9   LA    22       43           */
/* 6 NYC 15              |  have<-read_sas("d:/sd1/have.sas7bdat")                |      10   LA    23       66           */
/* 7 LA 0                |  print(have)                                           |      11   LA    24       69           */
/* 8 LA 21               |  want<-sqldf('                                         |                                       */
/* 9 LA 22               |  select                                                |                                       */
/* 10 LA 23              |    primkey                                             | SAS                                   */
/* 11 LA 24              |   ,city                                                |                                       */
/* ;;;;                  |   ,sales                                               | PRIMKEY  CITY  SALES THREESUM         */
/* run;quit;             |   ,case                                                |                                       */
/*                       |     when count(sales) over (                           |     7    LA       0      .            */
/*                       |       partition by city                                |     8    LA      21      .            */
/*                       |       order by primkey                                 |     9    LA      22     43            */
/*                       |       rows between 2 preceding and current row         |    10    LA      23     66            */
/*                       |     ) = 3                                              |    11    LA      24     69            */
/*                       |     then sum(sales) over (                             |     1    NYC     11      .            */
/*                       |       partition by city                                |     2    NYC      0      .            */
/*                       |       order by primkey                                 |     3    NYC     12     23            */
/*                       |       rows between 2 preceding and current row         |     4    NYC     13     25            */
/*                       |     )                                                  |     5    NYC     14     39            */
/*                       |     else null                                          |     6    NYC     15     42            */
/*                       |    end as threesum                                     |                                       */
/*                       |  from                                                  |                                       */
/*                       |    have                                                |                                       */
/*                       |  order                                                 |                                       */
/*                       |    by primkey                                          |                                       */
/*                       |    ')                                                  |                                       */
/*                       |  want;                                                 |                                       */
/*                       |  fn_tosas9x(                                           |                                       */
/*                       |        inp    = want                                   |                                       */
/*                       |       ,outlib ="d:/sd1/"                               |                                       */
/*                       |       ,outdsn ="want"                                  |                                       */
/*                       |       )                                                |                                       */
/*                       |  ;;;;                                                  |                                       */
/*                       |  %utl_rendx;                                           |                                       */
/*                       |                                                        |                                       */
/*                       |  proc print data=sd1.want;                             |                                       */
/*                       |  run;quit;                                             |                                       */
/*                       |------------------------------------------------------------------------------------------------*/
/*                       | 2 PYTHON SQL                                           | PYTHON                                */
/*                       | ============                                           |                                       */
/*                       |                                                        |     PRIMKEY CITY  SALES  THREESUM     */
/*                       |  proc datasets lib=sd1 nolist nodetails;               |                                       */
/*                       |   delete pywant;                                       |  0      7.0   LA    0.0       NaN     */
/*                       |  run;quit;                                             |  1      8.0   LA   21.0       NaN     */
/*                       |                                                        |  2      9.0   LA   22.0      43.0     */
/*                       |  %utl_pybeginx;                                        |  3     10.0   LA   23.0      66.0     */
/*                       |  parmcards4;                                           |  4     11.0   LA   24.0      69.0     */
/*                       |  exec(open('c:/oto/fn_pythonx.py').read());            |  5      1.0  NYC   11.0       NaN     */
/*                       |  have,meta = ps.read_sas7bdat('d:/sd1/have.sas7bdat'); |  6      2.0  NYC    0.0       NaN     */
/*                       |  want=pdsql('''                                        |  7      3.0  NYC   12.0      23.0     */
/*                       |  select                                                |  8      4.0  NYC   13.0      25.0     */
/*                       |    primkey                                             |  9      5.0  NYC   14.0      39.0     */
/*                       |   ,city                                                |  10     6.0  NYC   15.0      42.0     */
/*                       |   ,sales                                               |                                       */
/*                       |   ,case                                                |                                       */
/*                       |      when count(sales) over (                          | SAS                                   */
/*                       |        partition by city                               |                                       */
/*                       |        order by primkey                                | PRIMKEY  CITY SALES THREESUM          */
/*                       |        rows between 2 preceding and current row        |                                       */
/*                       |      ) = 3                                             |     7    LA      0      .             */
/*                       |      then sum(sales) over (                            |     8    LA     21      .             */
/*                       |        partition by city                               |     9    LA     22     43             */
/*                       |        order by primkey                                |    10    LA     23     66             */
/*                       |        rows between 2 preceding and current row        |    11    LA     24     69             */
/*                       |      )                                                 |     1    NYC    11      .             */
/*                       |      else null                                         |     2    NYC     0      .             */
/*                       |    end as threesum                                     |     3    NYC    12     23             */
/*                       |  from                                                  |     4    NYC    13     25             */
/*                       |    have                                                |     5    NYC    14     39             */
/*                       |  order                                                 |     6    NYC    15     42             */
/*                       |    by primkey                                          |                                       */
/*                       |     ''');                                              |                                       */
/*                       |  print(want);                                          |                                       */
/*                       |  fn_tosas9x(want,outlib='d:/sd1/',outdsn='pywant');    |                                       */
/*                       |  ;;;;                                                  |                                       */
/*                       |  %utl_pyendx;                                          |                                       */
/*                       |                                                        |                                       */
/*                       |  proc print data=sd1.pywant;                           |                                       */
/*                       |  run;quit;                                             |                                       */
/**************************************************************************************************************************/

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

options
 validvarname=upcase;
libname sd1 "d:/sd1";
data sd1.have;
 input
  primkey city $
  sales;
cards;
1 NYC 11
2 NYC 0
3 NYC 12
4 NYC 13
5 NYC 14
6 NYC 15
7 LA 0
8 LA 21
9 LA 22
10 LA 23
11 LA 24
;;;;
run;quit;

/**************************************************************************************************************************/
/* PRIMKEY    CITY    SALES                                                                                               */
/*                                                                                                                        */
/*     1      NYC       11                                                                                                */
/*     2      NYC        0                                                                                                */
/*     3      NYC       12                                                                                                */
/*     4      NYC       13                                                                                                */
/*     5      NYC       14                                                                                                */
/*     6      NYC       15                                                                                                */
/*     7      LA         0                                                                                                */
/*     8      LA        21                                                                                                */
/*     9      LA        22                                                                                                */
/*    10      LA        23                                                                                                */
/*    11      LA        24                                                                                                */
/**************************************************************************************************************************/

/*                    _
/ |  _ __   ___  __ _| |
| | | `__| / __|/ _` | |
| | | |    \__ \ (_| | |
|_| |_|    |___/\__, |_|
                   |_|
*/

proc datasets lib=sd1 nolist nodetails;
 delete want;
run;quit;

%utl_rbeginx;
parmcards4;
library(haven)
library(sqldf)
source("c:/oto/fn_tosas9x.R")
options(sqldf.dll = "d:/dll/sqlean.dll")
have<-read_sas("d:/sd1/have.sas7bdat")
print(have)
want<-sqldf('
select
  primkey
 ,city
 ,sales
 ,case
   when count(sales) over (
     partition by city
     order by primkey
     rows between 2 preceding and current row
   ) = 3
   then sum(sales) over (
     partition by city
     order by primkey
     rows between 2 preceding and current row
   )
   else null
  end as threesum
from
  have
order
  by primkey
  ')
want;
fn_tosas9x(
      inp    = want
     ,outlib ="d:/sd1/"
     ,outdsn ="want"
     )
;;;;
%utl_rendx;

proc print data=sd1.want;
run;quit;

/**************************************************************************************************************************/
/* R                              |    SAS                                                                                */
/* PRIMKEY CITY SALES THREESUM    |    ROWNAMES    PRIMKEY    CITY    SALES    THREESUM                                   */
/*                                |                                                                                       */
/*       1  NYC    11       NA    |        1           1      NYC       11         .                                      */
/*       2  NYC     0       NA    |        2           2      NYC        0         .                                      */
/*       3  NYC    12       23    |        3           3      NYC       12        23                                      */
/*       4  NYC    13       25    |        4           4      NYC       13        25                                      */
/*       5  NYC    14       39    |        5           5      NYC       14        39                                      */
/*       6  NYC    15       42    |        6           6      NYC       15        42                                      */
/*       7   LA     0       NA    |        7           7      LA         0         .                                      */
/*       8   LA    21       NA    |        8           8      LA        21         .                                      */
/*       9   LA    22       43    |        9           9      LA        22        43                                      */
/*      10   LA    23       66    |       10          10      LA        23        66                                      */
/**************************************************************************************************************************/

/*___                _   _                             _
|___ \   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
  __) | | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
 / __/  | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
|_____| | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
        |_|    |___/                                |_|
*/

proc datasets lib=sd1 nolist nodetails;
 delete pywant;
run;quit;

%utl_pybeginx;
parmcards4;
exec(open('c:/oto/fn_pythonx.py').read());
have,meta = ps.read_sas7bdat('d:/sd1/have.sas7bdat');
want=pdsql('''
select
  primkey
 ,city
 ,sales
 ,case
    when count(sales) over (
      partition by city
      order by primkey
      rows between 2 preceding and current row
    ) = 3
    then sum(sales) over (
      partition by city
      order by primkey
      rows between 2 preceding and current row
    )
    else null
  end as threesum
from
  have
order
  by primkey
   ''');
print(want);
fn_tosas9x(want,outlib='d:/sd1/',outdsn='pywant');
;;;;
%utl_pyendx;

proc print data=sd1.pywant;
run;quit;

/**************************************************************************************************************************/
/* R                                  |   SAS                                                                             */
/*     PRIMKEY CITY  SALES  tHREESUM  |   PRIMKEY    CITY    SALES    THREESUM                                            */
/*                                    |                                                                                   */
/* 0       1.0  NYC   11.0       NaN  |       1      NYC       11         .                                               */
/* 1       2.0  NYC    0.0       NaN  |       2      NYC        0         .                                               */
/* 2       3.0  NYC   12.0      23.0  |       3      NYC       12        23                                               */
/* 3       4.0  NYC   13.0      25.0  |       4      NYC       13        25                                               */
/* 4       5.0  NYC   14.0      39.0  |       5      NYC       14        39                                               */
/* 5       6.0  NYC   15.0      42.0  |       6      NYC       15        42                                               */
/* 6       7.0   LA    0.0       NaN  |       7      LA         0         .                                               */
/* 7       8.0   LA   21.0       NaN  |       8      LA        21         .                                               */
/* 8       9.0   LA   22.0      43.0  |       9      LA        22        43                                               */
/* 9      10.0   LA   23.0      66.0  |      10      LA        23        66                                               */
/* 10     11.0   LA   24.0      69.0  |      11      LA        24        69                                               */
/**************************************************************************************************************************/

/*  _              _       _           _
| || |    _ __ ___| | __ _| |_ ___  __| |  _ __ ___ _ __   ___  ___
| || |_  | `__/ _ \ |/ _` | __/ _ \/ _` | | `__/ _ \ `_ \ / _ \/ __|
|__   _| | | |  __/ | (_| | ||  __/ (_| | | | |  __/ |_) | (_) \__ \
   |_|   |_|  \___|_|\__,_|\__\___|\__,_| |_|  \___| .__/ \___/|___/
                                                   |_|
*/

REPO
-----------------------------------------------------------------------------------------------------------------------------------------
https://github.com/rogerjdeangelis/utl-betas-for-rolling-regressions
https://github.com/rogerjdeangelis/utl-calculating-a-weighted-or-moving-sum-for-a-window-of-size-three
https://github.com/rogerjdeangelis/utl-calculating-three-year-rolling-moving-weekly-and-annual-daily-standard-deviation
https://github.com/rogerjdeangelis/utl-compute-the-partial-and-total-rolling-sums-for-window-of-size-of-three
https://github.com/rogerjdeangelis/utl-creating-rolling-sets-of-monthly-tables
https://github.com/rogerjdeangelis/utl-forecast-the-next-four-months-using-a-moving-average-time-series
https://github.com/rogerjdeangelis/utl-forecast-the-next-seven-days-using-a--moving-average-model-in-R
https://github.com/rogerjdeangelis/utl-how-to-compare-price-observations-in-rolling-time-intervals
https://github.com/rogerjdeangelis/utl-moving-ten-month-average-by-group
https://github.com/rogerjdeangelis/utl-nearest-sales-date-on-or-before-a-commercial-date-using-r-roll-join-and-wps-r-and-python-sql
https://github.com/rogerjdeangelis/utl-parallell-processing-a-rolling-moving-three-month-ninety-day-skewness-for-five-thousand-variable
https://github.com/rogerjdeangelis/utl-proof-of-concept-using-dosubl-to-create-a-fcmp-like-function-for-a-rolling-sum-of-size-three
https://github.com/rogerjdeangelis/utl-python-r-compute-the-slope-e-of-rolling-window-ofe-size-seven-based-for-a-sine-curve
https://github.com/rogerjdeangelis/utl-roll-up-multiple-values-for-the-same-name-and-date-to-form-unique-name-date-key
https://github.com/rogerjdeangelis/utl-rolling-moving-sum-and-count-over-3-day-window-by-id
https://github.com/rogerjdeangelis/utl-rolling-sum_of-six-months-by-group
https://github.com/rogerjdeangelis/utl-timeseries-rolling-three-day-averages-by-county
https://github.com/rogerjdeangelis/utl-tumbling-goups-of-ten-temperatures-similar-like-rolling-and-moving-means-wps-r-python
https://github.com/rogerjdeangelis/utl-weight-loss-over-thirty-day-rolling-moving-windows-using-weekly-values
https://github.com/rogerjdeangelis/utl-weighted-moving-sum-for-several-variables
https://github.com/rogerjdeangelis/utl_calculate-moving-rolling-average-with-gaps-in-years

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
