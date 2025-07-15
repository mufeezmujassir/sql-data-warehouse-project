--Create the database and Schemas

USE master;



--Drop and recreate the DataWarehouse database

IF EXISTS (SELECT 1 from sys.databases where name='DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse
END;
GO
  


--create the DataWarehouse database

create DATABASE DataWarehouse;


USE DataWarehouse


-- craete the schemas

create schema bronze;
GO
create schema silver;
GO
create schema gold;
GO

