DROP TABLE IF EXISTS firme;
CREATE TABLE firme (
    denumire TEXT NOT NULL,
    cui TEXT,
    cod_inmatriculare TEXT,
    data_inmatriculare TEXT,
    euid TEXT,
    forma_juridica TEXT,
    judet TEXT,
    localitate TEXT,
    adresa TEXT,
    cod_postal TEXT,
    sector TEXT,
    web TEXT
);
CREATE INDEX idx_firme_denumire ON firme(denumire);
CREATE INDEX idx_firme_cui ON firme(cui);
CREATE INDEX idx_firme_cod ON firme(cod_inmatriculare);
