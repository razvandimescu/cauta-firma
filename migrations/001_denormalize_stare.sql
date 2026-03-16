-- Add stare column to firme (denormalized from stare_firma + n_stare)
ALTER TABLE firme ADD COLUMN stare TEXT;

-- Populate from the join
UPDATE firme SET stare = (
  SELECT ns.denumire
  FROM stare_firma sf
  JOIN n_stare ns ON ns.cod = sf.cod
  WHERE sf.cod_inmatriculare = firme.cod_inmatriculare
);
