CREATE INDEX ON bloom.npis (npi);

CREATE INDEX ON bloom.npi_licenses (npi_id);
CREATE INDEX ON bloom.npis (business_location_id);
CREATE INDEX ON bloom.npis (practice_location_id);
CREATE INDEX ON bloom.npis (organization_official_id);
CREATE INDEX ON bloom.npi_other_identifiers (npi_id);
CREATE INDEX ON bloom.npis (parent_orgs_id);
CREATE INDEX ON bloom.npi_taxonomy_groups (npi_id);
CREATE INDEX ON bloom.npis (file_id);