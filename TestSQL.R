# Load the libraries
library(RMariaDB)
library(DBI)
library(RMySQL)
library(dplyr)

# LOADING NEW DATA
# Create a database connection
con <- dbConnect(
  MariaDB(),
  dbname = dbname,
  host = host,
  port = port,
  user = user,
  password = pass
)


# Making new data (replaced with real data later on)
new_data <- read.csv("/Users/nick_leong/Downloads/mpdb_colors - Sheet1.csv", stringsAsFactors = FALSE)

### Color Table
existing_colors <- dbReadTable(con, "colors")

# Load new data from the CSV file
new_colors <- new_data %>%
  select(Color)

# Identify new records based on a unique identifier
color_identifier <- "Color"
color_new_records <- anti_join(new_colors, existing_colors, by = color_identifier)

# Assign unique identifiers ("IDColor") to the new records
# Find the maximum existing IDColor and increment from there
color_max_existing_id <- max(existing_colors$IDColor, na.rm = TRUE)
color_new_records$IDColor <- color_max_existing_id + 1 + seq_len(nrow(color_new_records))

# Filter to keep only the first occurrence of each new record
color_first_occurrences <- color_new_records%>%
  group_by(across(all_of(color_identifier))) %>%
  slice(1)

# Insert new records into the "colors" table if they don't already exist
for (i in 1:nrow(color_first_occurrences)) {
  color_record <- color_first_occurrences[i, ]
  # Check if the record already exists in the "colors" table
  if (!any(existing_colors$Color %in% color_record$Color)) {
    dbWriteTable(con, name = "colors", value = color_record, append = TRUE, row.names = FALSE)
  }
}


### Contributor Table
# Load existing data from the "contributor" table in the database
existing_contributors <- dbReadTable(con, "contributor")

# Load new data from the CSV file
new_contributors <- new_data %>%
  select(Fisrt_Name, Last_Name)

# Identify new records based on a unique identifier (combination of First_name and Last_name)
contributor_identifier <- c("Fisrt_Name", "Last_Name")
contributor_new_records <- anti_join(new_contributors, existing_contributors, by = contributor_identifier)

# Assign unique identifiers ("IDContributor") to the new records
# Find the maximum existing IDContributor and increment from there
contributor_max_existing_id <- max(existing_contributors$IDContributor, na.rm = TRUE)
contributor_new_records$IDContributor <- contributor_max_existing_id + 1 + seq_len(nrow(contributor_new_records))


# Filter to keep only the first occurrence of each new record
contributor_first_occurrences <- contributor_new_records %>%
  group_by(across(all_of(contributor_identifier))) %>%
  slice(1)


# Insert new records into the "contributor" table if they don't already exist
for (i in 1:nrow(contributor_first_occurrences)) {
  contributor_record <- contributor_first_occurrences[i, ]
  # Check if the record already exists in the "contributor" table
  if (!any(
    duplicated(existing_contributors %>%
               select(all_of(contributor_identifier))) ==
    contributor_record %>%
    select(all_of(contributor_identifier))
  )) {
    dbWriteTable(con, name = "contributor", value = contributor_record, append = TRUE, row.names = FALSE)
  }
}

### Size Fraction Table
existing_size_fraction <- dbReadTable(con, "size_fraction")

new_size_fraction <- new_data %>%
  select(Size_fraction, Size_range_from_μm, Size_range_to_μm)

# Identify new records based on a unique identifier
size_fraction_identifier <- "Size_fraction"
size_fraction_new_records <- anti_join(new_size_fraction, existing_size_fraction, by = size_fraction_identifier)

# Assign unique identifiers ("IDSize") to the new records
size_max_existing_id <- max(existing_size_fraction$IDSize, na.rm = TRUE)
size_fraction_new_records$IDSize <- size_max_existing_id + 1 + seq_len(nrow(size_fraction_new_records))

# Filter to keep only the first occurrence of each new record
size_fraction_first_occurrences <- size_fraction_new_records %>%
  group_by(across(all_of(size_fraction_identifier))) %>%
  slice(1)

# Insert new records into the "size_fraction" table if they don't already exist
for (i in 1:nrow(size_fraction_first_occurrences)) {
  size_fraction_record <- size_fraction_first_occurrences[i, ]
  # Check if the record already exists in the "size_fraction" table
  if (!any(existing_size_fraction$Size_fraction %in% size_fraction_record$Size_fraction)) {
    dbWriteTable(con, name = "size_fraction", value = size_fraction_record, append = TRUE, row.names = FALSE)
  }
}

### Shape Table
existing_shape <- dbReadTable(con, "shape")

new_shape <- new_data %>%
  select(Shape)

# Identify new records based on a unique identifier
shape_identifier <- "Shape"
shape_new_records <- anti_join(new_shape, existing_shape, by = shape_identifier)

# Assign unique identifiers ("IDShape") to the new records
shape_max_existing_id <- max(existing_shape$IDShape, na.rm = TRUE)
shape_new_records$IDShape <- shape_max_existing_id + 1 + seq_len(nrow(shape_new_records))

# Filter to keep only the first occurrence of each new record
shape_first_occurrences <- shape_new_records %>%
  group_by(across(all_of(shape_identifier))) %>%
  slice(1)

# Insert new records into the "shape" table if they don't already exist
for (i in 1:nrow(shape_first_occurrences)) {
  shape_record <- shape_first_occurrences[i, ]
  # Check if the record already exists in the "shape" table
  if (!any(existing_shape$Shape %in% shape_record$Shape)) {
    dbWriteTable(con, name = "shape", value = shape_record, append = TRUE, row.names = FALSE)
  }
}

### Method Category Table
existing_method_category <- dbReadTable(con, "method_category")

new_method_category <- new_data %>%
  select(Category)

# Identify new records based on a unique identifier
method_category_identifier <- "Category"
method_category_new_records <- anti_join(new_method_category, existing_method_category, by = method_category_identifier)

# Assign unique identifiers ("IDMethodCategory") to the new records
method_category_max_existing_id <- max(existing_method_category$IDMethodCategory, na.rm = TRUE)
method_category_new_records$IDMethodCategory <- method_category_max_existing_id + 1 + seq_len(nrow(method_category_new_records))

# Filter to keep only the first occurrence of each new record
method_category_first_occurrences <- method_category_new_records %>%
  group_by(across(all_of(method_category_identifier))) %>%
  slice(1)

# Insert new records into the "method_category" table if they don't already exist
for (i in 1:nrow(method_category_first_occurrences)) {
  method_category_record <- method_category_first_occurrences[i, ]
  # Check if the record already exists in the "method_category" table
  if (!any(existing_method_category$Category %in% method_category_record$Category)) {
    dbWriteTable(con, name = "method_category", value = method_category_record, append = TRUE, row.names = FALSE)
  }
}

### Methods Table
existing_methods <- dbReadTable(con, "methods")

new_methods <- new_data %>%
  select(Method_name, Category, Images)

# Identify new records based on a unique identifier
methods_identifier <- "Method_name"
methods_new_records <- anti_join(new_methods, existing_methods, by = methods_identifier)

# Assign unique identifiers ("IDMethod") to the new records
methods_max_existing_id <- max(existing_methods$IDMethod, na.rm = TRUE)
methods_new_records$IDMethod <- methods_max_existing_id + 1 + seq_len(nrow(methods_new_records))

# Filter to keep only the first occurrence of each new record
methods_first_occurrences <- methods_new_records %>%
  group_by(across(all_of(methods_identifier))) %>%
  slice(1)

# Insert new records into the "methods" table if they don't already exist
for (i in 1:nrow(methods_first_occurrences)) {
  methods_record <- methods_first_occurrences[i, ]
  # Check if the record already exists in the "methods" table
  if (!any(existing_methods$Method_name %in% methods_record$Method_name)) {
    dbWriteTable(con, name = "methods", value = methods_record, append = TRUE, row.names = FALSE)
  }
}

### Projects Table
existing_projects <- dbReadTable(con, "projects")

new_projects <- new_data %>%
  select(Project)

# Identify new records based on a unique identifier
projects_identifier <- "Project"
projects_new_records <- anti_join(new_projects, existing_projects, by = projects_identifier)

# Assign unique identifiers ("IDProject") to the new records
projects_max_existing_id <- max(existing_projects$IDProject, na.rm = TRUE)
projects_new_records$IDProject <- projects_max_existing_id + 1 + seq_len(nrow(projects_new_records))

# Filter to keep only the first occurrence of each new record
projects_first_occurrences <- projects_new_records %>%
  group_by(across(all_of(projects_identifier))) %>%
  slice(1)

# Insert new records into the "projects" table if they don't already exist
for (i in 1:nrow(projects_first_occurrences)) {
  projects_record <- projects_first_occurrences[i, ]
  # Check if the record already exists in the "projects" table
  if (!any(existing_projects$Project %in% projects_record$Project)) {
    dbWriteTable(con, name = "projects", value = projects_record, append = TRUE, row.names = FALSE)
  }
}

### Countries Table
existing_countries <- dbReadTable(con, "countries")

new_countries <- new_data %>%
  select(Country)

# Identify new records based on a unique identifier
countries_identifier <- "Country"
countries_new_records <- anti_join(new_countries, existing_countries, by = countries_identifier)

# Assign unique identifiers ("IDCountry") to the new records
countries_max_existing_id <- max(existing_countries$IDCountry, na.rm = TRUE)
countries_new_records$IDCountry <- countries_max_existing_id + 1 + seq_len(nrow(countries_new_records))

# Filter to keep only the first occurrence of each new record
countries_first_occurrences <- countries_new_records %>%
  group_by(across(all_of(countries_identifier))) %>%
  slice(1)

# Insert new records into the "countries" table if they don't already exist
for (i in 1:nrow(countries_first_occurrences)) {
  countries_record <- countries_first_occurrences[i, ]
  # Check if the record already exists in the "countries" table
  if (!any(existing_countries$Country %in% countries_record$Country)) {
    dbWriteTable(con, name = "countries", value = countries_record, append = TRUE, row.names = FALSE)
  }
}

### Institution Table
existing_institution <- dbReadTable(con, "institution")

new_institution <- new_data %>%
  select(Affiliation, Country, Institute_Acronym)

# Identify new records based on a unique identifier
institution_identifier <- c("Affiliation", "Country", "Institute_Acronym")
institution_new_records <- anti_join(new_institution, existing_institution, by = institution_identifier)

# Assign unique identifiers ("IDInstitute") to the new records
institution_max_existing_id <- max(existing_institution$IDInstitute, na.rm = TRUE)
institution_new_records$IDInstitute <- institution_max_existing_id + 1 + seq_len(nrow(institution_new_records))

# Filter to keep only the first occurrence of each new record
institution_first_occurrences <- institution_new_records %>%
  group_by(across(all_of(institution_identifier))) %>%
  slice(1)

# Insert new records into the "institution" table if they don't already exist
for (i in 1:nrow(institution_first_occurrences)) {
  institution_record <- institution_first_occurrences[i, ]
  # Check if the record already exists in the "institution" table
  if (!any(
    duplicated(existing_institution %>%
               select(all_of(institution_identifier))) ==
    institution_record %>%
    select(all_of(institution_identifier))
  )) {
    dbWriteTable(con, name = "institution", value = institution_record, append = TRUE, row.names = FALSE)
  }
}

### Sampling Compartment Table
existing_sampling_compartment <- dbReadTable(con, "sampling_compartment")

new_sampling_compartment <- new_data %>%
  select(Compartment)

# Identify new records based on a unique identifier
sampling_compartment_identifier <- "Compartment"
sampling_compartment_new_records <- anti_join(new_sampling_compartment, existing_sampling_compartment, by = sampling_compartment_identifier)

# Assign unique identifiers ("IDCompartment") to the new records
sampling_compartment_max_existing_id <- max(existing_sampling_compartment$IDCompartment, na.rm = TRUE)
sampling_compartment_new_records$IDCompartment <- sampling_compartment_max_existing_id + 1 + seq_len(nrow(sampling_compartment_new_records))

# Filter to keep only the first occurrence of each new record
sampling_compartment_first_occurrences <- sampling_compartment_new_records %>%
  group_by(across(all_of(sampling_compartment_identifier))) %>%
  slice(1)

# Insert new records into the "sampling_compartment" table if they don't already exist
for (i in 1:nrow(sampling_compartment_first_occurrences)) {
  sampling_compartment_record <- sampling_compartment_first_occurrences[i, ]
  # Check if the record already exists in the "sampling_compartment" table
  if (!any(existing_sampling_compartment$Compartment %in% sampling_compartment_record$Compartment)) {
    dbWriteTable(con, name = "sampling_compartment", value = sampling_compartment_record, append = TRUE, row.names = FALSE)
  }
}

### Samples Table
existing_samples <- dbReadTable(con, "samples")

contributor_values <- contributors$IDContributor[1:nrow(new_data)]
project_values <- projects$Acronym[1:nrow(new_data)]
compartment_values <- sampling_compartment$Compartment[1:nrow(new_data)]

new_samples <- new_data %>%
  select(Country, Analysis_date, Project, Compartment) %>%
  mutate(IDSample = row_number(),
         Sample_name = paste("Sample", 1:nrow(new_data)),
         Contributor = contributor_values,
         Project = project_values,
         Site_name = "unknown",
         Compartment = compartment_values,
         Time = 00:00:00,
         GPS_LON = 99.99,
         GPS_LAT = 99.99)

# Filter to keep only the first occurrence of each new record
samples_first_occurrences <- new_samples %>%
  group_by(across(all_of(c("Country", "Analysis_date", "Project", "Compartment")))) %>%
  slice(1)

# Insert new records into the "samples" table if they don't already exist
for (i in 1:nrow(samples_first_occurrences)) {
  samples_record <- samples_first_occurrences[i, ]
  # Check if the record already exists in the "samples" table
  if (!any(
    duplicated(existing_samples %>%
               select(all_of(c("Country", "Analysis_date", "Project", "Compartment")))) ==
    samples_record %>%
    select(all_of(c("Country", "Analysis_date", "Project", "Compartment")))
  )) {
    dbWriteTable(con, name = "samples", value = samples_record, append = TRUE, row.names = FALSE)
  }
}

### Polymer Category Table
existing_polymer_category <- dbReadTable(con, "polymer_category")

new_polymer_category <- new_data %>%
  select(Categorised_result)

# Identify new records based on a unique identifier
polymer_category_identifier <- "Categorised_result"
polymer_category_new_records <- anti_join(new_polymer_category, existing_polymer_category, by = polymer_category_identifier)

# Assign unique identifiers ("IDPolymer") to the new records
polymer_category_max_existing_id <- max(existing_polymer_category$IDPolymer, na.rm = TRUE)
polymer_category_new_records$IDPolymer <- polymer_category_max_existing_id + 1 + seq_len(nrow(polymer_category_new_records))

# Filter to keep only the first occurrence of each new record
polymer_category_first_occurrences <- polymer_category_new_records %>%
  group_by(across(all_of(polymer_category_identifier))) %>%
  slice(1)

# Insert new records into the "polymer_category" table if they don't already exist
for (i in 1:nrow(polymer_category_first_occurrences)) {
  polymer_category_record <- polymer_category_first_occurrences[i, ]
  # Check if the record already exists in the "polymer_category" table
  if (!any(existing_polymer_category$Categorised_result %in% polymer_category_record$Categorised_result)) {
    dbWriteTable(con, name = "polymer_category", value = polymer_category_record, append = TRUE, row.names = FALSE)
  }
}

### Particles Table
existing_particles <- dbReadTable(con, "particles")

preferred_method_values <- methods$IDMethod[1:nrow(new_data)]
analyst_values <- contributors$IDContributor[1:nrow(new_data)]

new_particles <- new_data %>%
  select(Shape, Size_fraction, Categorised_result, Analysis_date, Color) %>%
  mutate(IDParticles = row_number(),
         Preferred_method = preferred_method_values,
         Analyst = analyst_values,
         Amount = 1,
         Sample = paste("Sample", 1:nrow(new_data)))

# Filter to keep only the first occurrence of each new record
particles_first_occurrences <- new_particles %>%
  group_by(across(all_of(c("Shape", "Size_fraction", "Categorised_result", "Analysis_date", "Color")))) %>%
  slice(1)

# Insert new records into the "particles" table if they don't already exist
for (i in 1:nrow(particles_first_occurrences)) {
  particles_record <- particles_first_occurrences[i, ]
  # Check if the record already exists in the "particles" table
  if (!any(
    duplicated(existing_particles %>%
               select(all_of(c("Shape", "Size_fraction", "Categorised_result", "Analysis_date", "Color")))) ==
    particles_record %>%
    select(all_of(c("Shape", "Size_fraction", "Categorised_result", "Analysis_date", "Color")))
  )) {
    dbWriteTable(con, name = "particles", value = particles_record, append = TRUE, row.names = FALSE)
  }
}

### Equipment Table
existing_equipment <- dbReadTable(con, "equipment")

new_equipment <- new_data %>%
  select(Eq_name, Eq_specification)

# Identify new records based on a unique identifier
equipment_identifier <- c("Eq_name", "Eq_specification")
equipment_new_records <- anti_join(new_equipment, existing_equipment, by = equipment_identifier)

# Assign unique identifiers ("IDEquipment") to the new records
equipment_max_existing_id <- max(existing_equipment$IDEquipment, na.rm = TRUE)
equipment_new_records$IDEquipment <- equipment_max_existing_id + 1 + seq_len(nrow(equipment_new_records))

# Filter to keep only the first occurrence of each new record
equipment_first_occurrences <- equipment_new_records %>%
  group_by(across(all_of(equipment_identifier))) %>%
  slice(1)

# Insert new records into the "equipment" table if they don't already exist
for (i in 1:nrow(equipment_first_occurrences)) {
  equipment_record <- equipment_first_occurrences[i, ]
  # Check if the record already exists in the "equipment" table
  if (!any(
    duplicated(existing_equipment %>%
               select(all_of(equipment_identifier))) ==
    equipment_record %>%
    select(all_of(equipment_identifier))
  )) {
    dbWriteTable(con, name = "equipment", value = equipment_record, append = TRUE, row.names = FALSE)
  }
}

# Disconnect
dbDisconnect(con)