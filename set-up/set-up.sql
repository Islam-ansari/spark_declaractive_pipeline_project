create catalog if not exists circuitbox;
use catalog circuitbox;
create schema if not exists landing;
create schema if not exists bronze;
create schema if not exists silver;
create schema if not exists gold;

use schema landing;

create volume circuitbox.landing.operational_data;

%python
dbutils.fs.ls('/Volumes/circuitbox/landing/operational_data/');
