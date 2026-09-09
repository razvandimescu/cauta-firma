export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const handler = routes[url.pathname];
    if (!handler) return env.ASSETS.fetch(request);
    return cached(request, ctx, () => handler(url, env));
  }
};

const routes = {
  '/api/search': handleSearch,
  '/api/company': handleCompany,
  '/api/bilant': handleBilant,
  '/api/anaf': handleAnaf,
  '/api/similar': handleSimilar,
};

// Edge cache keyed on the full URL; repeated and crawler queries never reach D1
async function cached(request, ctx, produce) {
  const cache = caches.default;
  const hit = await cache.match(request);
  if (hit) return hit;

  const response = await produce();
  if (response.status === 200) {
    if (!response.headers.has('Cache-Control')) response.headers.set('Cache-Control', 'public, max-age=3600');
    ctx.waitUntil(cache.put(request, response.clone()));
  }
  return response;
}

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

  // Prefix as a range: LIKE is case-insensitive and cannot use the binary-collated index
  const results = await env.DB.prepare(
    `SELECT denumire, cui, cod_inmatriculare, forma_juridica, judet, localitate
     FROM firme WHERE denumire >= ?1 AND denumire < ?2 ORDER BY denumire LIMIT 10`
  ).bind(upper, upper + '\uffff').all();

  return json({ results: results.results });
}

async function handleCompany(url, env) {
  const cui = url.searchParams.get('cui');
  if (!cui) return json({ error: 'Missing cui' }, 400);

  const company = await env.DB.prepare(
    `SELECT * FROM firme WHERE cui = ?1 LIMIT 1`
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
     WHERE ca.cod_caen = ?1
       AND f.cui != ?2
       AND (f.stare IS NULL OR f.stare = 'funcțiune')
       ${judet ? 'AND f.judet = ?3' : ''}
     LIMIT 8`
  ).bind(...(judet ? [caen, cui || '', judet] : [caen, cui || ''])).all();

  return json({ results: results.results });
}

async function handleAnaf(url) {
  const cui = url.searchParams.get('cui');
  if (!cui) return json({ error: 'Missing cui' }, 400);

  const today = new Date().toISOString().slice(0, 10);
  const target = 'https://webservicesp.anaf.ro/api/PlatitorTvaRest/v9/tva';
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const resp = await fetch(target, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        },
        body: JSON.stringify([{ cui: parseInt(cui), data: today }]),
      });
      if (resp.status === 520 || resp.status === 502 || resp.status === 404) {
        await new Promise(r => setTimeout(r, 500 * (attempt + 1)));
        continue;
      }
      const data = await resp.json();
      const found = data.found?.[0] || null;
      return json({ data: found });
    } catch (e) {
      if (attempt === 2) return json({ error: e.message }, 502);
      await new Promise(r => setTimeout(r, 500 * (attempt + 1)));
    }
  }
  return json({ error: 'ANAF unreachable' }, 502);
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
      const data = await resp.json();
      // Filed statements never change; unfiled ones keep the default short TTL
      const filed = data.i?.length > 0;
      return json(data, 200, filed ? { 'Cache-Control': 'public, max-age=2592000' } : {});
    } catch (e) {
      if (attempt === 2) return json({ error: e.message }, 502);
      await new Promise(r => setTimeout(r, 500 * (attempt + 1)));
    }
  }
  return json({ error: 'ANAF unreachable after 3 attempts' }, 502);
}

function json(data, status = 200, headers = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', ...headers }
  });
}
