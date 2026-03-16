export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === '/api/search') return handleSearch(url, env);
    if (url.pathname === '/api/company') return handleCompany(url, env);
    if (url.pathname === '/api/bilant') return handleBilant(url);
    if (url.pathname === '/api/similar') return handleSimilar(url, env);

    return env.ASSETS.fetch(request);
  }
};

async function handleSearch(url, env) {
  const q = (url.searchParams.get('q') || '').trim();
  if (q.length < 2) return json({ results: [] });

  const isNumeric = /^\d+$/.test(q);

  if (isNumeric) {
    // CUI lookup — exact match on index, fast
    const results = await env.DB.prepare(
      `SELECT denumire, cui, cod_inmatriculare, forma_juridica, judet, localitate
       FROM firme WHERE cui = ?1 LIMIT 10`
    ).bind(q).all();
    return json({ results: results.results });
  }

  // Uppercase for index-friendly comparison (ONRC data is uppercase)
  const upper = q.toUpperCase();

  // Try prefix search first (uses B-tree index on denumire)
  let results = await env.DB.prepare(
    `SELECT denumire, cui, cod_inmatriculare, forma_juridica, judet, localitate
     FROM firme WHERE denumire LIKE ?1 ORDER BY denumire LIMIT 10`
  ).bind(`${upper}%`).all();

  // Fall back to contains search only if prefix returns too few
  if (results.results.length < 5) {
    results = await env.DB.prepare(
      `SELECT denumire, cui, cod_inmatriculare, forma_juridica, judet, localitate
       FROM firme WHERE denumire LIKE ?1 ORDER BY denumire LIMIT 10`
    ).bind(`%${upper}%`).all();
  }

  return json({ results: results.results });
}

async function handleCompany(url, env) {
  const cui = url.searchParams.get('cui');
  if (!cui) return json({ error: 'Missing cui' }, 400);

  const company = await env.DB.prepare(
    `SELECT f.*, ns.denumire as stare
     FROM firme f
     LEFT JOIN stare_firma sf ON sf.cod_inmatriculare = f.cod_inmatriculare
     LEFT JOIN n_stare ns ON ns.cod = sf.cod
     WHERE f.cui = ?1
     LIMIT 1`
  ).bind(cui).first();

  if (!company) return json({ error: 'Not found' }, 404);

  const caenCodes = await env.DB.prepare(
    `SELECT ca.cod_caen, nc.denumire, nc.sectiunea
     FROM caen_autorizat ca
     LEFT JOIN n_caen nc ON nc.clasa = ca.cod_caen
     WHERE ca.cod_inmatriculare = ?1`
  ).bind(company.cod_inmatriculare).all();

  return json({ company, caen_codes: caenCodes.results });
}

async function handleSimilar(url, env) {
  const cui = url.searchParams.get('cui');
  const caen = url.searchParams.get('caen');
  const judet = url.searchParams.get('judet');
  if (!caen) return json({ results: [] });

  // Drop ORDER BY RANDOM() — just take first 8 matching rows
  const results = await env.DB.prepare(
    `SELECT f.denumire, f.cui, f.judet, f.localitate, f.forma_juridica
     FROM firme f
     JOIN caen_autorizat ca ON ca.cod_inmatriculare = f.cod_inmatriculare
     LEFT JOIN stare_firma sf ON sf.cod_inmatriculare = f.cod_inmatriculare
     WHERE ca.cod_caen = ?1
       AND f.cui != ?2
       AND (sf.cod IS NULL OR sf.cod = 1048)
       ${judet ? 'AND f.judet = ?3' : ''}
     LIMIT 8`
  ).bind(...(judet ? [caen, cui || '', judet] : [caen, cui || ''])).all();

  return json({ results: results.results });
}

async function handleBilant(url) {
  const cui = url.searchParams.get('cui');
  const an = url.searchParams.get('an');
  if (!cui || !an) return json({ error: 'Missing cui or an' }, 400);

  const target = `https://webservicesp.anaf.ro/bilant?an=${an}&cui=${cui}`;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const resp = await fetch(target, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
          'Accept': 'application/json, text/plain, */*',
          'Accept-Language': 'ro-RO,ro;q=0.9,en;q=0.8',
        },
      });
      if (resp.status === 520 || resp.status === 502) {
        await new Promise(r => setTimeout(r, 500 * (attempt + 1)));
        continue;
      }
      const data = await resp.text();
      return new Response(data, {
        headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
      });
    } catch (e) {
      if (attempt === 2) return json({ error: e.message }, 502);
      await new Promise(r => setTimeout(r, 500 * (attempt + 1)));
    }
  }
  return json({ error: 'ANAF unreachable after 3 attempts' }, 502);
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
  });
}
