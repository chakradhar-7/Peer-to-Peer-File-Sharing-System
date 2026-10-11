// The tracker's read-only dashboard (served at http://127.0.0.1:<web_port>/).
// Plain HTML, CSS and JavaScript, no external files. It polls GET /api/state every second:
// users and who is online, groups, files with their seeders and partial peers, and the
// state of the sync link to the other tracker.
#pragma once

static const char *DASHBOARD_PAGE = R"PAGE(<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Tracker Dashboard</title>
<style>
:root{--bg:#f5f6f8;--card:#fff;--text:#1c2027;--muted:#6b7280;--line:#e3e6ea;--ok:#15803d;--ok-bg:#dcfce7;--warn:#b45309;--warn-bg:#fef3c7;--bad:#b91c1c;--bad-bg:#fee2e2;--chip:#eef0f3}
@media (prefers-color-scheme:dark){:root{--bg:#0f1115;--card:#171a20;--text:#e5e7eb;--muted:#9aa3af;--line:#2a2f38;--ok:#4ade80;--ok-bg:#14301f;--warn:#fbbf24;--warn-bg:#3a2e0e;--bad:#f87171;--bad-bg:#3b1717;--chip:#232831}}
*{box-sizing:border-box}
body{margin:0;font:14px/1.45 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;background:var(--bg);color:var(--text)}
header{display:flex;align-items:center;gap:12px;flex-wrap:wrap;padding:12px 16px;border-bottom:1px solid var(--line);background:var(--card);position:sticky;top:0}
h1{font-size:16px;margin:0}
h2{font-size:14px;margin:0 0 10px}
.muted{color:var(--muted)}
.spacer{flex:1}
main{max-width:1200px;margin:0 auto;padding:16px;display:grid;gap:16px}
.tiles{display:grid;gap:12px;grid-template-columns:repeat(auto-fit,minmax(170px,1fr))}
.tile,.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px 14px;min-width:0}
.tile .v{font-size:22px;font-weight:600;margin-top:2px}
.tile .s{font-size:12px;color:var(--muted)}
.table{overflow-x:auto}
table{border-collapse:collapse;width:100%}
th,td{text-align:left;padding:7px 8px;border-top:1px solid var(--line);vertical-align:top;overflow-wrap:anywhere}
th{color:var(--muted);font-weight:600;font-size:12px;border-top:0}
.pill{display:inline-block;padding:1px 9px;border-radius:999px;font-size:12px;white-space:nowrap}
.on{background:var(--ok-bg);color:var(--ok)}
.via{background:var(--warn-bg);color:var(--warn)}
.off{background:var(--chip);color:var(--muted)}
.down{background:var(--bad-bg);color:var(--bad)}
.chip{display:inline-block;padding:1px 8px;border-radius:999px;font-size:12px;margin:2px 4px 2px 0;background:var(--chip);color:var(--muted)}
.chip.live{background:var(--ok-bg);color:var(--ok)}
.empty{color:var(--muted);padding:8px}
</style>
</head>
<body>
<header>
  <h1 id="title">Tracker</h1>
  <span class="muted" id="addr"></span>
  <span class="spacer"></span>
  <span class="muted" id="updated">connecting…</span>
</header>
<main>
  <section class="tiles" id="tiles"></section>
  <section class="card"><h2>Users</h2><div class="table" id="users"></div></section>
  <section class="card"><h2>Groups</h2><div class="table" id="groups"></div></section>
  <section class="card"><h2>Files</h2><div class="table" id="files"></div></section>
</main>
<script>
const $ = id => document.getElementById(id);
// names come from users of the system: always escape them
const esc = s => String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function size(n) {
  const u = ['B', 'KB', 'MB', 'GB'];
  let i = 0;
  while (n >= 1000 && i < u.length - 1) { n /= 1000; i++; }
  return (i ? n.toFixed(1) : n) + ' ' + u[i];
}
function table(head, rows, empty) {
  if (!rows.length) return `<div class="empty">${empty}</div>`;
  return `<table><thead><tr>${head.map(h => `<th>${h}</th>`).join('')}</tr></thead><tbody>` +
    rows.map(r => `<tr>${r.map(c => `<td>${c}</td>`).join('')}</tr>`).join('') + '</tbody></table>';
}
function tile(label, value, sub) {
  return `<div class="tile"><div class="s">${label}</div><div class="v">${value}</div><div class="s">${sub || '&nbsp;'}</div></div>`;
}
function link(up, name) { return `<span class="pill ${up ? 'on' : 'down'}">${name} ${up ? 'up' : 'down'}</span>`; }

function render(s) {
  $('title').textContent = 'Tracker ' + s.tracker;
  $('addr').textContent = s.addr + (s.peer ? ' · other tracker ' + s.peer : ' · no other tracker configured');
  $('updated').textContent = 'updated ' + new Date().toLocaleTimeString();
  const online = s.users.filter(u => u.online).length;
  const sync = !s.peer ? '<span class="pill off">not configured</span>'
    : link(s.sending, 'sending') + ' ' + link(s.receiving, 'receiving');
  $('tiles').innerHTML =
    tile('Users online', `${online} <span class="muted" style="font-size:14px">of ${s.users.length}</span>`) +
    tile('Groups', s.groups.length) +
    tile('Shared files', s.files.length) +
    tile('Open connections', s.connections, 'clients and the other tracker') +
    tile('Sync with other tracker', `<span style="font-size:14px">${sync}</span>`,
         s.receiving || !s.peer ? '' : s.link_down_for ? `receiving link down for ${s.link_down_for} s` : '') +
    tile('Changes made here', s.changes, `${s.unconfirmed} not yet confirmed · ${s.peer_applied} received from the other tracker`);
  $('users').innerHTML = table(['User', 'Status', 'Sessions here', 'Serves pieces on'],
    s.users.map(u => [esc(u.name),
      u.here ? '<span class="pill on">online here</span>' : u.online ? '<span class="pill via">online via other tracker</span>' : '<span class="pill off">offline</span>',
      u.sessions, u.online ? esc(u.addr) : '<span class="muted">-</span>']),
    'No users yet');
  $('groups').innerHTML = table(['Group', 'Owner', 'Members', 'Join requests'],
    s.groups.map(g => [esc(g.id), esc(g.owner),
      g.members.map(m => `<span class="chip">${esc(m)}</span>`).join(''),
      g.requests.length ? g.requests.map(r => `<span class="chip">${esc(r)}</span>`).join('') : '<span class="muted">none</span>']),
    'No groups yet');
  const peers = list => list.length
    ? list.map(p => `<span class="chip${p.online ? ' live' : ''}" title="${p.online ? 'online' : 'offline'}">${esc(p.user)}</span>`).join('')
    : '<span class="muted">none</span>';
  $('files').innerHTML = table(['Group', 'File', 'Size', 'Pieces', 'Seeders (whole file)', 'Downloading (partial)'],
    s.files.map(f => [esc(f.group), esc(f.name), size(f.size), f.pieces, peers(f.seeders), peers(f.partial)]),
    'No files shared yet');
}

async function refresh() {
  try {
    const r = await fetch('/api/state');
    render(await r.json());
  } catch (e) { $('updated').textContent = 'tracker not reachable'; }
}
refresh();
setInterval(refresh, 1000);
</script>
</body>
</html>
)PAGE";
