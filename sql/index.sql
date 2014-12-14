CREATE INDEX ON bloom.npis (npi);
CREATE UNIQUE INDEX npi_and_revision ON bloom.npis(npi,revision);
