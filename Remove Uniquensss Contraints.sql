-- Remove the uniqueness constraint ***THIRD
ALTER TABLE test.countries
DROP INDEX CountryShort;

-- Add a regular index (not unique)
ALTER TABLE test.countries
ADD INDEX CountryShort (CountryShort);

-- Remove the uniqueness constraint ****SECOND
ALTER TABLE test.institution
DROP FOREIGN KEY FK_institution_countries;

ALTER TABLE institution
ADD CONSTRAINT FK_institution_countries
FOREIGN KEY (InstituteCountry)
REFERENCES countries (CountryShort)
ON UPDATE CASCADE;

-- Drop the foreign key constraint (replace 'FK_samples_countries' with the actual constraint name) **RUN THIS FIRST IF STARTING OVER
ALTER TABLE test.samples
DROP FOREIGN KEY FK_samples_countries;

ALTER TABLE samples
ADD CONSTRAINT FK_samples_countries
FOREIGN KEY (Country)
REFERENCES countries (CountryShort)
ON UPDATE CASCADE;

-- Remove the uniqueness constraint 
ALTER TABLE test.methods
DROP INDEX Method_name;

-- Add a regular index (not unique)
ALTER TABLE test.methods
ADD INDEX Method_name (Method_name);

-- Drop the foreign key constraint (replace 'FK_samples_countries' with the actual constraint name) 
ALTER TABLE test.methods
DROP FOREIGN KEY FK_methods_method_category;

-- Remove the uniqueness constraint ***THIRD
ALTER TABLE test.institution
DROP INDEX InstituteAcronym;

-- Add a regular index (not unique)
ALTER TABLE test.institution
ADD INDEX InstituteAcronym (InstituteAcronym);




