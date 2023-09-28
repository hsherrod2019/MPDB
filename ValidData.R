# Load the libraries
library(RMariaDB)
library(DBI)
library(RMySQL)
library(dplyr)

# Create a database connection
con <- dbConnect(
  MariaDB(),
  dbname = dbname,
  host = host,
  port = port,
  user = user,
  password = pass
)

# Enter data
data <- read.csv("Copy of Copy of Image Data Sharing (Responses) - Form Responses.csv", stringsAsFactors = FALSE)

# Replace empty or NA values with "NA"
data <- data %>%
  mutate_all(~ ifelse(is.na(.x) | .x == "", "NA", .x))

### Inserting data into mysql ###

# Read in "colors" table
colors <- data %>%
  select(Color) %>%
  mutate(IDColor = row_number())
colors <- head(colors, n = 26)

dbWriteTable(con, name = "colors", value = colors, append = TRUE, row.names = FALSE)

# Read in "contributors" table
contributors <- data %>%
  select(Fisrt_name, Last_name) %>%
  mutate(IDContributor = row_number())
contributors <- head(contributors, n = 26)

dbWriteTable(con, name = "contributor", value = contributors, append = TRUE, row.names = FALSE)

# Read in "size_fraction" table
size_fraction <- data %>%
  select(Size_fraction, Size_range_from_μm, Size_range_to_μm) %>%
  mutate(IDSize = row_number())

size_fraction <- size_fraction %>%
  rename(Size_category = Size_fraction)
size_fraction <- head(size_fraction, n = 26)

dbWriteTable(con, name = "size_fraction", value = size_fraction, append = TRUE, row.names = FALSE)

# Read in "shape" table
shape <- data %>%
  select(Shape) %>%
  mutate(IDShape = row_number())
shape <- head(shape, n = 26)

dbWriteTable(con, name = "shape", value = shape, append = TRUE, row.names = FALSE)

# Read in "method_category" table
method_category <- data %>%
  select(Category) %>%
  mutate(IDMethodCategory = row_number())
method_category <- head(method_category, n = 26)

dbWriteTable(con, name = "method_category", value = method_category, append = TRUE, row.names = FALSE)

# Read in "methods" table
methods <- data %>%
  select(Method_name, Category, Images) %>%
  mutate(IDMethod = row_number())
methods <- head(methods, n = 26)

dbWriteTable(con, name = "methods", value = methods, append = TRUE, row.names = FALSE)

# Read in "projects" table
projects <- data %>%
  select(Project) %>%
  mutate(IDProject = row_number(),
         Project = paste(Project, row_number(), sep = "_"))

projects <- projects %>%
  rename(Acronym = Project)
projects <- head(projects, n = 26)

dbWriteTable(con, name = "projects", value = projects, append = TRUE, row.names = FALSE)

# Read in "countries" table
countries <- data %>%
  select(Country) %>%
  mutate(IDCountry = row_number())

countries <- countries %>%
  rename(CountryShort = Country)
countries <- head(countries, n = 26)

dbWriteTable(con, name = "countries", value = countries, append = TRUE, row.names = FALSE)

# Read in "institution" table
institution <- data %>%
  select(Affiliation, Country, Institute_Acronym) %>%
  mutate(IDInstitute = row_number())

institution <- institution %>%
  rename(Institute_Name = Affiliation,
         InstituteCountry = Country)
institution <- head(institution, n = 26)

dbWriteTable(con, name = "institution", value = institution, append = TRUE, row.names = FALSE)

# Read in "sampling_compartment" table
sampling_compartment <- data %>%
  select(Compartment) %>%
  mutate(IDCompartment = row_number(),
         Compartment = "unknown")
sampling_compartment <- head(sampling_compartment, n = 26)

dbWriteTable(con, name = "sampling_compartment", value = sampling_compartment, append = TRUE, row.names = FALSE)

# Read in "samples" table
contributor_values <- contributors$IDContributor[1:nrow(data)]
project_values <- projects$Acronym[1:nrow(data)]
compartment <- sampling_compartment$Compartment[1:nrow(data)]

samples <- data %>%
  select(Country, Analysis_date, Project, Compartment) %>%
  mutate(IDSample = row_number(),
         Sample_name = paste("Sample", 1:nrow(data)),
         Contributor = contributor_values,
         Project = project_values,
         Site_name = "unknown",
         Compartment = compartment,
         Time = 00:00:00,
         GPS_LON = 99.99,
         GPS_LAT = 99.99)

samples <- samples %>%
  rename(Date = Analysis_date)
samples <- head(samples, n = 26)

dbWriteTable(con, name = "samples", value = samples, append = TRUE, row.names = FALSE)

# Read in "polymer_category" table
polymer_category <- data %>%
  select(Categorised_result) %>%
  mutate(IDPolymer = row_number())

polymer_category <- polymer_category %>%
  rename(Polymer_category = Categorised_result)
polymer_category <- head(polymer_category, n = 26)

dbWriteTable(con, name = "polymer_category", value = polymer_category, append = TRUE, row.names = FALSE)

# Read in "particles" table
preferred_method_values <- methods$IDMethod[1:nrow(data)]
analyst_values <- contributors$IDContributor[1:nrow(data)]

particles <- data %>%
  select(Shape, Size_fraction, Categorised_result, Analysis_date, Color) %>%
  mutate(IDParticles = row_number(),
         Preferred_method = preferred_method_values,
         Analyst = analyst_values,
         Amount = 1,
         Sample = paste("Sample", 1:nrow(data)))

particles <- particles %>%
  rename(Colour = Color)
particles <- head(particles, n = 26)

dbWriteTable(con, name = "particles", value = particles, append = TRUE, row.names = FALSE)

# Read in "equipment" table
equipment <- data %>%
  select(Eq_name, Eq_specification) %>%
  mutate(IDEquipment = row_number())

equipment <- equipment %>%
  rename(Eq_Name = Eq_name,
         Eq_Specification = Eq_specification)
equipment <- head(equipment, n = 26)

dbWriteTable(con, name = "equipment", value = equipment, append = TRUE, row.names = FALSE)

# Disconnect
dbDisconnect(con)
