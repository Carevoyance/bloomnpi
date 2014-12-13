DROP TABLE IF EXISTS bloom.npi_licenses;
DROP TABLE IF EXISTS bloom.npi_locations;
DROP TABLE IF EXISTS bloom.npi_organization_officials;
DROP TABLE IF EXISTS bloom.npi_other_identifiers;
DROP TABLE IF EXISTS bloom.npi_parent_orgs;
DROP TABLE IF EXISTS bloom.npi_taxonomy_groups;
DROP TABLE IF EXISTS bloom.npis;
DROP TABLE IF EXISTS bloom.npi_files;

DELETE FROM bloom.data_sources WHERE source = 'NPI';