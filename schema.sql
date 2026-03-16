DROP TABLE IF EXISTS firme;
DROP TABLE IF EXISTS stare_firma;
DROP TABLE IF EXISTS caen_autorizat;
DROP TABLE IF EXISTS n_stare;
DROP TABLE IF EXISTS n_caen;

CREATE TABLE firme (
    denumire TEXT NOT NULL,
    cui TEXT,
    cod_inmatriculare TEXT PRIMARY KEY,
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

CREATE TABLE stare_firma (
    cod_inmatriculare TEXT,
    cod INTEGER
);
CREATE INDEX idx_stare_cod ON stare_firma(cod_inmatriculare);

CREATE TABLE caen_autorizat (
    cod_inmatriculare TEXT,
    cod_caen TEXT
);
CREATE INDEX idx_caen_cod ON caen_autorizat(cod_inmatriculare);

CREATE TABLE n_stare (
    cod INTEGER PRIMARY KEY,
    denumire TEXT
);

CREATE TABLE n_caen (
    clasa TEXT PRIMARY KEY,
    denumire TEXT,
    sectiunea TEXT
);
