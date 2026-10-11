// The client's web page (served at http://127.0.0.1:<web_port>/?t=<token>).
// Plain HTML, CSS and JavaScript, no external files. It talks to the client through:
//   GET  /api/state       who is logged in, downloads with per-peer progress, shared files, groups
//   GET  /api/group?g=..  files and (for the owner) join requests of one group
//   POST /api/cmd         one command line, exactly as typed in the terminal
// Every API call carries the token from the page's URL in the X-P2P-Token header.
#pragma once

static const char *WEB_PAGE = R"PAGE(<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>P2P Client</title>
<style>
:root{--bg:#f5f6f8;--card:#fff;--text:#1c2027;--muted:#6b7280;--line:#e3e6ea;--accent:#2563eb;--accent-text:#fff;--ok:#15803d;--warn:#b45309;--bad:#b91c1c;--track:#e6e9ee}
@media (prefers-color-scheme:dark){:root{--bg:#0f1115;--card:#171a20;--text:#e5e7eb;--muted:#9aa3af;--line:#2a2f38;--accent:#3b82f6;--accent-text:#fff;--ok:#4ade80;--warn:#fbbf24;--bad:#f87171;--track:#262b34}}
*{box-sizing:border-box}
body{margin:0;font:14px/1.45 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;background:var(--bg);color:var(--text)}
header{display:flex;align-items:center;gap:12px;flex-wrap:wrap;padding:12px 16px;border-bottom:1px solid var(--line);background:var(--card);position:sticky;top:0;z-index:1}
h1{font-size:16px;margin:0}
h2{font-size:14px;margin:0}
h3{font-size:13px;margin:14px 0 6px;color:var(--muted);font-weight:600}
.muted{color:var(--muted)}
.spacer{flex:1}
main{max-width:1120px;margin:0 auto;padding:16px;display:grid;gap:16px;grid-template-columns:minmax(0,1fr) minmax(0,1fr);align-items:start}
@media (max-width:820px){main{grid-template-columns:minmax(0,1fr)}}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px;min-width:0}
.full{grid-column:1/-1}
.row{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin:8px 0}
.row input{flex:1 1 160px}
input{font:inherit;padding:7px 9px;border:1px solid var(--line);border-radius:6px;background:var(--bg);color:var(--text);min-width:0}
button{font:inherit;padding:6px 11px;border-radius:6px;border:1px solid var(--line);background:var(--card);color:var(--text);cursor:pointer}
button:hover{border-color:var(--accent)}
button.primary{background:var(--accent);border-color:var(--accent);color:var(--accent-text)}
button.link{border:0;background:none;padding:2px 0;color:var(--text);text-align:left}
button.link.sel{color:var(--accent);font-weight:600}
ul.list{list-style:none;margin:0;padding:0}
ul.list li{display:flex;justify-content:space-between;align-items:center;gap:8px;padding:7px 0;border-top:1px solid var(--line);overflow-wrap:anywhere}
ul.list li:first-child{border-top:0}
.badge{padding:3px 10px;border-radius:999px;background:var(--track);font-size:12px}
.dl{padding:10px 0;border-top:1px solid var(--line)}
.dl:first-child{border-top:0;padding-top:0}
.bar{height:8px;background:var(--track);border-radius:4px;overflow:hidden;margin:6px 0}
.bar>div{height:100%;background:var(--accent);transition:width .3s}
.state-C{color:var(--ok)}
.state-E{color:var(--warn)}
.chip{display:inline-block;padding:1px 8px;border-radius:999px;border:1px solid var(--line);font-size:12px;margin:3px 4px 0 0;color:var(--muted)}
.chip.bad{color:var(--bad);border-color:var(--bad)}
pre{margin:0;max-height:260px;overflow:auto;white-space:pre-wrap;overflow-wrap:anywhere;font:12px/1.45 ui-monospace,Menlo,Consolas,monospace}
</style>
</head>
<body>
<header>
  <h1>P2P client</h1>
  <span class="muted" id="peer"></span>
  <span class="spacer"></span>
  <span class="badge" id="who">connecting…</span>
  <button id="logoutBtn" hidden>Log out</button>
</header>
<main>
  <section class="card full" id="authCard" hidden>
    <h2>Log in</h2>
    <div class="row">
      <input id="u" placeholder="User id" autocomplete="username">
      <input id="p" type="password" placeholder="Password" autocomplete="current-password">
    </div>
    <div class="row"><button class="primary" id="loginBtn">Log in</button><button id="createBtn">Create account</button></div>
  </section>

  <section class="card" id="groupsCard" hidden>
    <h2>Groups</h2>
    <ul class="list" id="groups"></ul>
    <div class="row"><input id="newGroup" placeholder="New group id"><button id="createGroupBtn">Create group</button></div>
  </section>

  <section class="card" id="groupCard" hidden>
    <div class="row" style="margin-top:0"><h2 id="gTitle">Group</h2><span class="spacer"></span><button id="leaveBtn" hidden>Leave group</button></div>
    <p class="muted" id="gNote"></p>
    <div id="gJoin" hidden><button class="primary" id="joinBtn">Request to join</button></div>
    <div id="gMember" hidden>
      <h3>Files</h3>
      <ul class="list" id="files"></ul>
      <div class="row"><span class="muted">Save downloads to</span><input id="dest"></div>
      <h3>Share a file from this computer</h3>
      <div class="row"><input id="upPath" placeholder="/full/path/to/file"><button id="upBtn">Share</button></div>
      <div id="reqBox" hidden><h3>Join requests</h3><ul class="list" id="reqs"></ul></div>
    </div>
  </section>

  <section class="card" id="dlCard" hidden><h2>Downloads</h2><div id="downloads" style="margin-top:10px"></div></section>
  <section class="card" id="shareCard" hidden><h2>Shared from this client</h2><ul class="list" id="shared" style="margin-top:6px"></ul></section>

  <section class="card full">
    <h2>Console</h2>
    <div class="row"><input id="cmdIn" placeholder="Any client command, e.g. show_downloads"><button id="cmdBtn">Run</button></div>
    <pre id="log"></pre>
  </section>
</main>
<script>
const token = new URLSearchParams(location.search).get('t') || '';
const $ = id => document.getElementById(id);
// everything shown comes from the tracker or other users: always escape it
const esc = s => String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
// the selected group lives in the link (…#family), so it survives a reload and can be bookmarked
let state = null, group = decodeURIComponent(location.hash.slice(1)) || null, ginfo = null;
function selectGroup(g) {
  group = g;
  ginfo = null;
  history.replaceState(null, '', location.pathname + location.search + '#' + encodeURIComponent(g));
}

async function api(path, opts = {}) {
  const r = await fetch(path, {...opts, headers: {'X-P2P-Token': token}});
  const j = await r.json();
  if (!r.ok) throw new Error(j.error || r.statusText);
  return j;
}
function log(text) { $('log').textContent = text + '\n' + $('log').textContent; }
// replace a list only when it changed, so a button isn't swapped out under the mouse
function setHTML(el, html) { if (el._html !== html) { el.innerHTML = html; el._html = html; } }
// commands are space separated, so ids and paths must be single words
function word(v, what) {
  v = v.trim();
  if (!v || /\s/.test(v)) { alert(what + ' must be one word, without spaces'); return null; }
  return v;
}
function size(n) {
  const u = ['B', 'KB', 'MB', 'GB'];
  let i = 0;
  while (n >= 1000 && i < u.length - 1) { n /= 1000; i++; }
  return (i ? n.toFixed(1) : n) + ' ' + u[i];
}
async function run(cmd, shown) {
  try {
    const r = await api('/api/cmd', {method: 'POST', body: cmd});
    const out = r.output.trim();
    log('$ ' + (shown || cmd) + (out ? '\n' + out : ''));
  } catch (e) { log('! ' + e.message); }
  await refresh();
}
async function refresh() {
  try { state = await api('/api/state'); }
  catch (e) { $('who').textContent = token ? e.message : 'Open the link the client printed'; return; }
  if (state.logged_in && group) {
    try { ginfo = await api('/api/group?g=' + encodeURIComponent(group)); } catch (e) { ginfo = null; }
  }
  render();
}

function render() {
  const s = state;
  $('peer').textContent = 'serving pieces on ' + s.peer + ' · tracker ' + s.tracker;
  $('who').textContent = s.logged_in ? 'logged in as ' + s.user : 'not logged in';
  $('logoutBtn').hidden = !s.logged_in;
  $('authCard').hidden = s.logged_in;
  for (const id of ['groupsCard', 'groupCard', 'dlCard', 'shareCard']) $(id).hidden = !s.logged_in;
  if (!s.logged_in) return;
  if (!$('dest').value) $('dest').value = s.cwd;
  setHTML($('groups'), s.groups.length
    ? s.groups.map(g => `<li><button class="link${g === group ? ' sel' : ''}" data-g="${esc(g)}">${esc(g)}</button></li>`).join('')
    : '<li class="muted">No groups yet</li>');
  renderGroup();
  renderDownloads();
  renderShared();
}

function renderGroup() {
  const g = ginfo && ginfo.group === group ? ginfo : null;
  $('gTitle').textContent = group ? 'Group ' + group : 'Group';
  $('gNote').textContent = !group ? 'Pick a group, or create one.'
    : !g ? 'Loading…'
    : g.error ? g.error
    : g.member ? (g.owner ? 'You own this group.' : 'You are a member.')
    : 'You are not a member of this group.';
  $('leaveBtn').hidden = !(g && g.member);
  $('gMember').hidden = !(g && g.member);
  $('gJoin').hidden = !(g && !g.member && !g.error);
  if (!g || !g.member) return;
  setHTML($('files'), g.files.length
    ? g.files.map(f => `<li><span>${esc(f)}</span><button data-dl="${esc(f)}">Download</button></li>`).join('')
    : '<li class="muted">No files shared yet</li>');
  $('reqBox').hidden = !g.owner;
  setHTML($('reqs'), g.requests.length
    ? g.requests.map(u => `<li><span>${esc(u)}</span><button data-acc="${esc(u)}">Accept</button></li>`).join('')
    : '<li class="muted">No pending requests</li>');
}

function renderDownloads() {
  const d = state.downloads;
  setHTML($('downloads'), d.length ? d.map(x => {
    const total = x.total || 1, done = x.state === 'C' ? total : (x.done || 0);
    const label = x.state === 'C' ? 'Complete'
      : x.state === 'E' ? x.status.replace(/^\[E\]\s+\S+\s+\S+\s*/, '')
      : x.total ? `${done} / ${x.total} pieces` : x.status;
    const peers = (x.peers || []).map(p =>
      `<span class="chip${p.banned ? ' bad' : ''}" title="${p.complete ? 'has the whole file' : 'still downloading itself'}">` +
      `${esc(p.user)} · ${esc(p.addr)} · ${p.pieces} pieces${p.banned ? ' · dropped (bad data)' : ''}</span>`).join('');
    return `<div class="dl"><div class="row" style="margin:0"><b>${esc(x.file)}</b><span class="muted">in ${esc(x.group)}</span>` +
      `<span class="spacer"></span><span class="state-${esc(x.state)}">${esc(label)}</span></div>` +
      `<div class="bar"><div style="width:${Math.round(100 * done / total)}%"></div></div>${peers}</div>`;
  }).join('') : '<p class="muted" style="margin:0">No downloads yet</p>');
}

function renderShared() {
  const f = state.shared;
  setHTML($('shared'), f.length ? f.map(x =>
    `<li><span><b>${esc(x.file)}</b> <span class="muted">in ${esc(x.group)} · ${size(x.size)}` +
    `${x.partial ? ' · still downloading, its finished pieces are shared' : ''}</span></span>` +
    (x.partial ? '' : `<button data-stop="${esc(x.group)}" data-file="${esc(x.file)}">Stop sharing</button>`) + '</li>').join('')
    : '<li class="muted">Nothing shared yet</li>');
}

function account(kind) {
  const u = word($('u').value, 'User id'), p = word($('p').value, 'Password');
  if (u && p) run(`${kind} ${u} ${p}`, `${kind} ${u} ****`);
}
$('loginBtn').onclick = () => account('login');
$('createBtn').onclick = () => account('create_user');
$('p').onkeydown = e => { if (e.key === 'Enter') account('login'); };
$('logoutBtn').onclick = () => run('logout');
$('createGroupBtn').onclick = () => {
  const g = word($('newGroup').value, 'Group id');
  if (g) { selectGroup(g); $('newGroup').value = ''; run(`create_group ${g}`); }
};
$('groups').onclick = e => {
  const b = e.target.closest('[data-g]');
  if (b) { selectGroup(b.dataset.g); render(); refresh(); }
};
$('joinBtn').onclick = () => run(`join_group ${group}`);
$('leaveBtn').onclick = () => { if (confirm(`Leave ${group}? Your files in it stop being shared.`)) run(`leave_group ${group}`); };
$('files').onclick = e => {
  const b = e.target.closest('[data-dl]');
  const dest = b && word($('dest').value, 'Destination');
  if (dest) run(`download_file ${group} ${b.dataset.dl} ${dest}`);
};
$('reqs').onclick = e => { const b = e.target.closest('[data-acc]'); if (b) run(`accept_request ${group} ${b.dataset.acc}`); };
$('upBtn').onclick = () => {
  const p = word($('upPath').value, 'File path');
  if (p) { $('upPath').value = ''; run(`upload_file ${group} ${p}`); }
};
$('shared').onclick = e => {
  const b = e.target.closest('[data-stop]');
  if (b) run(`stop_share ${b.dataset.stop} ${b.dataset.file}`);
};
function console_run() {
  const c = $('cmdIn').value.trim();
  if (!c) return;
  $('cmdIn').value = '';
  const w = c.split(/\s+/);
  run(c, (w[0] === 'login' || w[0] === 'create_user') && w.length > 2 ? `${w[0]} ${w[1]} ****` : c);
}
$('cmdBtn').onclick = console_run;
$('cmdIn').onkeydown = e => { if (e.key === 'Enter') console_run(); };

refresh();
setInterval(refresh, 1000);
</script>
</body>
</html>
)PAGE";
