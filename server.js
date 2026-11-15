// Мессенджер: node server.js  →  http://localhost:3000  (без зависимостей, Node 16+)
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto'), zlib = require('zlib');
const PORT = process.env.PORT || 3000, UP = path.join(__dirname, 'uploads'), DBF = path.join(__dirname, 'db.json');
fs.mkdirSync(UP, { recursive: true });

let db = { users: {}, chats: {}, msgs: {}, tokens: {} };
try { db = JSON.parse(fs.readFileSync(DBF)); } catch {}
db.stories = db.stories || [];
db.tickets = db.tickets || [];
// операторы техподдержки: ники через запятую, напр.  SUPPORT_ADMINS=admin,vasya node server.js
const STAFF = new Set((process.env.SUPPORT_ADMINS || '').split(',').map(x => x.trim().toLowerCase()).filter(Boolean));
// запись БД: не чаще раза в 400 мс (было — на каждое сообщение, сериализация всей базы блокировала сервер), атомарно через временный файл
let saveT = null, saving = false, dirty = false;
const flush = () => {
  saveT = null;
  if (saving) { dirty = true; return; }
  saving = true;
  const tmp = DBF + '.tmp';
  fs.writeFile(tmp, JSON.stringify(db), err => {
    const done = () => { saving = false; if (dirty) { dirty = false; save(); } };
    if (err) return done();
    fs.rename(tmp, DBF, done);
  });
};
const save = () => { if (!saveT) saveT = setTimeout(flush, 400); };
const flushSync = () => { try { if (saveT) clearTimeout(saveT); fs.writeFileSync(DBF, JSON.stringify(db)); } catch {} };
process.on('SIGINT', () => { flushSync(); process.exit(0); });
process.on('SIGTERM', () => { flushSync(); process.exit(0); });

const EXT = { 'image/png': 'png', 'image/jpeg': 'jpg', 'image/gif': 'gif', 'image/webp': 'webp', 'video/webm': 'webm', 'audio/mp3': 'mp3', 'audio/mpeg': 'mp3', 'audio/ogg': 'ogg', 'audio/x-m4a': 'm4a', 'audio/mp4': 'm4a', 'video/mp4': 'mp4' };
const MIME = Object.fromEntries(Object.entries(EXT).map(([m, e]) => [e, m]));
const REACTS = ['❤️', '👍', '👎', '😂', '‼️', '❓'];
const clients = new Map(); // ник -> Set(res) для SSE
const id = () => crypto.randomBytes(8).toString('hex');
const hash = (p, s) => crypto.scryptSync(p, s, 32).toString('hex');
const send = (res, code, obj) => {
  const body = Buffer.from(JSON.stringify(obj)), ae = (res.req && res.req.headers['accept-encoding']) || '';
  if (body.length > 1400 && /\bgzip\b/.test(ae)) {
    return zlib.gzip(body, { level: 4 }, (e, z) => {
      if (e) { res.writeHead(code, { 'Content-Type': 'application/json' }); return res.end(body); }
      res.writeHead(code, { 'Content-Type': 'application/json', 'Content-Encoding': 'gzip', 'Vary': 'Accept-Encoding' }); res.end(z);
    });
  }
  res.writeHead(code, { 'Content-Type': 'application/json' }); res.end(body);
};
const body = req => new Promise(r => {
  let b = '';
  req.on('data', c => { b += c; if (b.length > 1e6) req.destroy(); });
  req.on('end', () => { try { r(JSON.parse(b || '{}')); } catch { r({}); } });
});
// сквозное шифрование: сервер хранит только публичные ключи и шифртекст {v, iv, ct} (AES-GCM, base64), прочитать его не может
const encValid = e => e && typeof e === 'object' && e.v === 1 && typeof e.iv === 'string' && typeof e.ct === 'string' && /^[A-Za-z0-9+/]{16}$/.test(e.iv) && e.ct.length >= 24 && e.ct.length <= 20000 && /^[A-Za-z0-9+/]+={0,2}$/.test(e.ct) ? { v: 1, iv: e.iv, ct: e.ct } : null;
const push = (nicks, ev) => nicks.forEach(n => (clients.get(n) || []).forEach(r => r.write('data:' + JSON.stringify(ev) + '\n\n')));
const pubRaw = n => {
  const u = db.users[n] || {}, c = db.chats[u.channel]; // канал и музыка, показываемые в профиле
  return { nick: n, name: u.name || n, username: u.username || '', bio: u.bio || '', avatar: u.avatar || null, banner: u.banner || null, e2e: u.e2e || null,
    channel: c && c.type === 'channel' && !c.private ? { id: c.id, name: c.name, avatar: c.avatar || null, count: c.members.length } : null, music: u.music || null, ...(u.bot ? { online: true, seen: Date.now(), hid: false } : presence(u, (clients.get(n) || { size: 0 }).size > 0)) };
};
const pub = (n, full) => { // full=true только для своего профиля: себе показываем всё, остальным — с учётом настроек приватности
  const o = pubRaw(n), p = (db.users[n] || {}).priv || {};
  if (full) return { ...o, priv: p };
  if (p.bio) o.bio = ''; if (p.avatar) o.avatar = null; if (p.banner) o.banner = null; if (p.username) o.username = '';
  if (p.extras) { o.channel = null; o.music = null; }
  if (p.login) o.hidLogin = true;
  return o;
};
const out = m => { const x = pub(m.from), o = { ...m, name: x.name, avatar: x.avatar, username: x.username }; if (m.reply) o.reply = { ...m.reply, name: pub(m.reply.from).name }; return o; };
// ---------- истории (живут 24 часа) ----------
const TTL = 24 * 3600e3, MAX_STORIES = 30;
const live = () => { // отдаёт актуальные истории, просроченные удаляет
  const t = Date.now() - TTL;
  if (db.stories.some(x => x.ts < t)) { db.stories = db.stories.filter(x => x.ts >= t); save(); }
  return db.stories;
};
setInterval(live, 10 * 60e3).unref();
const sview = (st, viewer) => {
  const o = { id: st.id, from: st.from, media: st.media, mtype: st.mtype, ts: st.ts, exp: st.ts + TTL, my: (st.react || {})[viewer] || null };
  if (st.from === viewer) o.react = Object.entries(st.react || {}).map(([n, e]) => ({ nick: n, name: pub(n).name, emoji: e })); // кто и чем отреагировал — видит только автор
  return o;
};
const storyRef = st => ({ id: st.id, owner: st.from, media: st.media, mtype: st.mtype });
// ---------- блокировка: пользователь может заблокировать любого другого ----------
const blockedBy = (a, b) => ((db.users[a] || {}).blocked || []).includes(b); // a заблокировал b
const blockErr = (a, b) => blockedBy(a, b) ? 'Вы заблокировали этого пользователя. Разблокируйте его, чтобы писать' : blockedBy(b, a) ? 'Пользователь ограничил получение сообщений от вас' : null; // a пишет b
const getDm = (a, b) => {
  const cid = 'dm_' + crypto.createHash('md5').update([a, b].sort().join('|')).digest('hex').slice(0, 12);
  if (!db.chats[cid]) {
    db.chats[cid] = { id: cid, type: 'dm', name: '', owner: a, members: [a, b], reads: { [a]: Date.now(), [b]: 0 } }; db.msgs[cid] = []; save();
    push([a, b], { t: 'chat', chat: view(db.chats[cid]) });
  }
  return db.chats[cid];
};
const tview = (t, viewer) => ({ id: t.id, subject: t.subject, status: t.status, ts: t.ts, upd: t.upd, name: pub(t.from).name, mine: t.from === viewer,
  msgs: t.msgs.map(x => ({ id: x.id, staff: !!x.staff, mine: x.from === viewer, name: x.staff ? 'Техподдержка' : pub(x.from).name, text: x.text, ts: x.ts })) });
const TTLS = [0, 86400, 604800, 2592000]; // автоудаление: выкл., 1 день, 1 неделя, 1 месяц
const alive = c => { const a = db.msgs[c.id] || []; return c.ttl ? a.filter(x => Date.now() - x.ts < c.ttl * 1000) : a; };
const addMsg = (c, msg) => { db.msgs[c.id].push(msg); save(); const o = out(msg); push(c.members, { t: 'msg', chat: c.id, msg: o }); return o; };

// ---------- «прочитано»: у каждого участника своя отметка времени, до которой он прочитал чат ----------
const markRead = (c, who, ts) => {
  const r = c.reads = c.reads || {};
  if ((r[who] || 0) >= ts) return false;
  r[who] = ts; save();
  if (c.type !== 'channel') push(c.members.filter(n => n !== who), { t: 'read', chat: c.id, nick: who, ts }); // в каналах отметку храним, но не рассылаем
  return true;
};
const countUnread = (c, who) => { // сколько сообщений от других пришло после того, как человек последний раз открывал чат
  const r = c.reads = c.reads || {};
  if (r[who] === undefined) { r[who] = Date.now(); save(); return 0; } // старый чат без отметки: всё прежнее считаем прочитанным
  let n = 0; alive(c).forEach(x => { if (x.from !== who && x.ts > r[who]) n++; });
  return Math.min(n, 999);
};
// последнее сообщение для списка чатов (текст ограничен, медиа только как признак)
const lastOf = c => {
  const a = alive(c), m = a[a.length - 1]; if (!m) return null;
  return { id: m.id, from: m.from, name: pub(m.from).name, ts: m.ts, text: String(m.text || '').slice(0, 100), sticker: m.sticker || undefined, mtype: m.mtype || undefined, fname: m.fname || undefined, media: !!m.media || undefined, story: !!m.story || undefined };
};
const view = c => ({
  id: c.id, type: c.type, name: c.name, owner: c.owner, count: c.members.length, avatar: c.avatar || null, admins: c.admins || [], discuss: c.discuss || null, parent: c.parent || null, ttl: c.ttl || 0, private: !!c.private, history: c.history || 'visible', reactions: c.reactions || 'all', greeting: c.greeting || null, perms: c.perms || null, color: c.color || '', desc: c.desc || '', pinned: c.pinned || null,
  members: c.type === 'dm' ? c.members : undefined,
  reads: c.type === 'channel' ? undefined : (c.reads || {}),
  last: lastOf(c),
  peers: c.type === 'dm' ? Object.fromEntries(c.members.map(n => [n, pub(n)])) : undefined
});

// ---------- расширенные настройки чата (общий код сервера и демо) ----------
const inviteOk = (c, code) => { if (!code) return false; if (code === c.invite) return true; const i = (c.invites || []).find(x => x.code === code && !x.revoked); if (i) i.uses = (i.uses || 0) + 1; return !!i; };
const reactOk = (c, e) => !c.reactions || c.reactions === 'all' || (c.reactions === 'basic' && ['❤️', '👍', '👎'].includes(e));
const histOk = (c, who, m) => c.history !== 'hidden' || !c.joined || !c.joined[who] || m.ts >= c.joined[who];
const permErr = (c, who, p) => {
  const pr = c.perms; if (!pr || c.owner === who || (c.admins || []).includes(who)) return null;
  if (p.sticker || p.mtype === 'sticker' || p.mtype === 'gif') return pr.stickers === false ? 'В этом чате запрещены стикеры и GIF' : null;
  if (p.media) return p.mtype === 'file' ? (pr.files === false ? 'В этом чате запрещены файлы' : null) : (pr.media === false ? 'В этом чате запрещены фото и видео' : null);
  return p.text && pr.text === false ? 'В этом чате запрещено писать сообщения' : null;
};
const chatLog = (c, by, text) => { (c.log = c.log || []).push({ ts: Date.now(), by, text }); c.log = c.log.slice(-100); };
const LOGN = { edit: 'Изменён чат', admin: 'Изменены админы', kick: 'Удалён участник', comments: 'Включены комментарии' };
const onJoin = (c, who, H) => {
  (c.joined = c.joined || {})[who] = Date.now();
  if (c.greeting && c.greeting.on && c.greeting.text) H.addMsg(c, { id: H.id(), from: c.owner, text: c.greeting.text.replace(/\{name\}/g, H.pub(who).name), ts: Date.now() });
};
function extra(c, me, a, m, B, H) { // [код, ответ]
  const owner = c.owner === me, adm = owner || (c.admins || []).includes(me), E = (k, e) => [k, { error: e }];
  if (a === 'log') return adm ? [200, (c.log || []).slice().reverse().map(x => ({ ...x, name: H.pub(x.by).name }))] : E(403, 'Нет доступа');
  if (!adm && a !== 'settings') return E(403, 'Нет доступа');
  if (a === 'stats') {
    const ms = H.db.msgs[c.id] || [], now = Date.now(), cnt = {}, days = Array(7).fill(0);
    ms.forEach(x => { cnt[x.from] = (cnt[x.from] || 0) + 1; const k = Math.floor((now - x.ts) / 864e5); if (k < 7) days[6 - k]++; });
    return [200, { members: c.members.length, total: ms.length, day: days[6], week: days.reduce((p, q) => p + q, 0), days, top: Object.entries(cnt).sort((p, q) => q[1] - p[1]).slice(0, 5).map(([n, v]) => ({ name: H.pub(n).name, count: v })) }];
  }
  if (a === 'links') {
    c.invites = c.invites || [];
    if (m === 'POST') {
      if (B.revoke) { const i = c.invites.find(x => x.code === B.revoke); if (i) i.revoked = 1; else if (B.revoke === c.invite) c.invite = H.id(); chatLog(c, me, 'Отозвана ссылка-приглашение'); }
      else { if (c.invites.filter(x => !x.revoked).length >= 10) return E(400, 'Не больше 10 ссылок'); c.invites.push({ code: H.id(), name: String(B.name || '').trim().slice(0, 30) || 'Ссылка', ts: Date.now(), uses: 0 }); chatLog(c, me, 'Создана ссылка-приглашение'); }
      H.save();
    }
    return [200, [...(c.invite ? [{ code: c.invite, name: 'Основная', main: 1 }] : []), ...c.invites.filter(x => !x.revoked)]];
  }
  if (a === 'settings') {
    if (!owner) return E(403, 'Только владелец');
    if (B.history !== undefined) { if (!['visible', 'hidden'].includes(B.history)) return E(400, 'Нельзя'); c.history = B.history; }
    if (B.reactions !== undefined) { if (!['all', 'basic', 'none'].includes(B.reactions)) return E(400, 'Нельзя'); c.reactions = B.reactions; }
    if (B.greeting !== undefined) { const g = B.greeting || {}; c.greeting = { on: !!g.on, text: String(g.text || '').trim().slice(0, 300) }; }
    if (B.perms !== undefined) { const p = B.perms || {}; c.perms = {}; ['text', 'media', 'stickers', 'files'].forEach(k => { c.perms[k] = p[k] !== false; }); }
    if (B.color !== undefined) { if (B.color && !/^#[0-9a-f]{6}$/i.test(B.color)) return E(400, 'Нельзя'); c.color = B.color || ''; }
    if (B.desc !== undefined) c.desc = String(B.desc).trim().slice(0, 200);
    const N = { history: 'история', reactions: 'реакции', greeting: 'приветствие', perms: 'права', color: 'цвет', desc: 'описание' };
    chatLog(c, me, 'Изменены настройки: ' + Object.keys(B).map(k => N[k] || k).join(', '));
    H.save(); H.push(c.members, { t: 'chat', chat: H.view(c) }); return [200, H.view(c)];
  }
  return E(404, 'Не найдено');
}
// ---------- «в сети» и время последнего захода ----------
const presence = (u, live) => {
  if (u.hideSeen) return { online: false, seen: 0, hid: true };
  const on = live || (Date.now() - (u.seen || 0) < 40000 && !(u.off && u.off >= u.seen));
  return { online: on, seen: on ? Date.now() : (u.seen || 0), hid: false };
};
const touch = n => { const u = db.users[n]; if (u && Date.now() - (u.seen || 0) > 15000) { u.seen = Date.now(); save(); } };
// ---------- стикер-паки: свои и готовые (общий код сервера и демо) ----------
const BUILTIN = [
  ['b_faces', 'Эмоции', '😀 😂 🤣 😍 🥰 😎 🤩 🥳 😭 😡 🥺 🤔 😴 🤯 😱 🙄 😏 🤗 😇 🤪'],
  ['b_animals', 'Зверята', '🐱 🐶 🦊 🐻 🐼 🐨 🐯 🦁 🐸 🐵 🐧 🦄 🐰 🐷 🐮 🐙'],
  ['b_food', 'Вкусняшки', '🍕 🍔 🍟 🌭 🍣 🍜 🍩 🍪 🎂 🍫 🍿 🍓 🍉 🥑 ☕ 🍺'],
  ['b_love', 'Любовь', '❤️ 💔 💕 💖 💘 😘 💋 🌹 💌 💞 😻 💝'],
  ['b_party', 'Праздник', '🎉 🥳 🎂 🎁 🎈 🍾 🎊 🔥 💯 ✨ 🎶 🏆'],
  ['b_space', 'Космос', '🚀 🌍 🌙 ⭐ ☄️ 👽 🛸 🪐 🌌 🔭']
].map(([id, name, it]) => ({ id, name, items: it.split(' ') }));
function packx(u, me, m, q, B, H) { // [код, ответ]
  const E = (k, e) => [k, { error: e }], P = u.packs = u.packs || [], IMG = /^\/uploads\/\w+\.(png|jpg|gif|webp)$/;
  if (m === 'GET') {
    if (!q.get('cat')) return [200, P];
    const shared = []; Object.entries(H.db.users).forEach(([n, x]) => { if (n !== me) (x.packs || []).forEach(p => { if (p.public && !p.src) shared.push({ id: p.id, name: p.name, items: p.items, by: H.pub(n).name }); }); });
    return [200, { builtin: BUILTIN, shared: shared.slice(0, 60) }];
  }
  const A = B.act, p = P.find(x => x.id === B.pack);
  if (A === 'create') {
    const nm = String(B.name || '').trim().slice(0, 30); if (!nm) return E(400, 'Введите название пака'); if (P.length >= 20) return E(400, 'Не больше 20 паков');
    P.push({ id: H.id(), name: nm, items: [] }); H.save(); return [200, P];
  }
  if (A === 'get') {
    let s = BUILTIN.find(x => x.id === B.id); if (!s) Object.values(H.db.users).forEach(x => (x.packs || []).forEach(k => { if (k.id === B.id && k.public && !k.src) s = k; }));
    if (!s) return E(404, 'Пак не найден'); if (P.some(x => x.src === s.id)) return E(400, 'Пак уже добавлен'); if (P.length >= 20) return E(400, 'Не больше 20 паков');
    P.push({ id: H.id(), name: s.name, items: s.items.slice(), src: s.id }); H.save(); return [200, P];
  }
  if (!p) return E(404, 'Пак не найден');
  if (A === 'del') { u.packs = P.filter(x => x !== p); H.save(); return [200, u.packs]; }
  if (A === 'rm') p.items = p.items.filter(x => x !== B.url);
  else if (A === 'rename') { const nm = String(B.name || '').trim().slice(0, 30); if (!nm) return E(400, 'Введите название пака'); p.name = nm; }
  else if (A === 'pub') { if (p.src) return E(400, 'Чужой пак нельзя опубликовать'); p.public = !!B.on; }
  else if (A === 'add') {
    if (p.src) return E(400, 'Пак из каталога нельзя дополнять'); if (!IMG.test(B.url || '')) return E(400, 'Нужна картинка'); if (p.items.length >= 60) return E(400, 'Максимум 60 стикеров в паке');
    if (!p.items.includes(B.url)) p.items.push(B.url);
  } else return E(400, 'Нельзя');
  H.save(); return [200, P];
}
// ---------- действия над сообщениями: изменить, удалить, переслать, закрепить ----------
function msgx(c, me, a, B, H) {
  const E = (k, e) => [k, { error: e }], ms = H.db.msgs[c.id] || [], m = ms.find(x => x.id === B.msg), adm = c.type !== 'dm' && (c.owner === me || (c.admins || []).includes(me));
  if (a === 'fwd') {
    const t = H.db.chats[B.to]; if (!t || !t.members.includes(me)) return E(403, 'Нет доступа к чату');
    if (t.type === 'channel' && t.owner !== me && !(t.admins || []).includes(me)) return E(403, 'В канале пишут только владелец и админы');
    const res = [];
    for (const id of (Array.isArray(B.msgs) ? B.msgs : []).slice(0, 20)) {
      const s = ms.find(x => x.id === id); if (!s) continue;
      if (s.enc) return E(400, 'Зашифрованные сообщения нельзя пересылать');
      const pe = permErr(t, me, { text: s.text, media: !!s.media, mtype: s.mtype, sticker: s.sticker }); if (pe) return E(403, pe);
      const n = { id: H.id(), from: me, text: s.text || '', ts: Date.now(), fwd: { from: s.fwd ? s.fwd.from : s.from, name: s.fwd ? s.fwd.name : H.pub(s.from).name } };
      if (s.sticker) n.sticker = s.sticker; else if (s.media) { n.media = s.media; n.mtype = s.mtype; if (s.fname) n.fname = s.fname; }
      res.push(H.addMsg(t, n));
    }
    return res.length ? [200, res] : E(404, 'Сообщение не найдено');
  }
  if (a === 'pin') {
    if (c.type !== 'dm' && !adm) return E(403, 'Закреплять могут владелец и админы');
    if (B.msg && !m) return E(404, 'Сообщение не найдено');
    c.pinned = B.msg || null; H.save(); H.push(c.members, { t: 'chat', chat: H.view(c) }); return [200, H.view(c)];
  }
  if (!m) return E(404, 'Сообщение не найдено');
  if (a === 'msgedit') {
    if (m.from !== me) return E(403, 'Можно изменять только свои сообщения'); if (m.sticker || m.mtype === 'sticker' || m.mtype === 'gif') return E(400, 'Нельзя изменить');
    const eo = B.enc === undefined ? null : encValid(B.enc);
    if (B.enc !== undefined && !eo) return E(400, 'Некорректное шифрованное сообщение');
    if (!!m.enc !== !!eo) return E(400, m.enc ? 'Изменённый текст нужно зашифровать' : 'Это сообщение не зашифровано');
    const t = eo ? '' : String(B.text || '').trim().slice(0, 4000); if (!t && !m.media && !eo) return E(400, 'Пустое сообщение');
    if (eo) m.enc = eo;
    m.text = t; m.edited = m.ets = Date.now(); H.save(); const o = H.out(m); H.push(c.members, { t: 'edit', chat: c.id, msg: o }); return [200, o];
  }
  if (a === 'msgdel') {
    if (m.from !== me && !adm) return E(403, 'Нельзя удалить чужое сообщение');
    const wasLast = ms[ms.length - 1] === m;
    H.db.msgs[c.id] = ms.filter(x => x !== m); c.dels = (c.dels || []).concat({ id: m.id, ts: Date.now() }).slice(-50);
    if (c.pinned === m.id) { c.pinned = null; H.push(c.members, { t: 'chat', chat: H.view(c) }); }
    H.save(); H.push(c.members, { t: 'del', chat: c.id, ids: [m.id] });
    if (wasLast) H.push(c.members, { t: 'chat', chat: H.view(c) }); // в списке чатов появится предыдущее сообщение
    return [200, { ok: true }];
  }
  return E(404, 'Не найдено');
}
const HX = { get db() { return db; }, id, pub, addMsg, view, push, save, out };

// страница: сжатие gzip один раз на версию файла + ETag (повторные заходы с телефона отдают 304 без передачи 600 КБ)
const pageCache = new Map();
function servePage(req, res, f) {
  const st = fs.statSync(f); let c = pageCache.get(f);
  if (!c || c.mtime !== st.mtimeMs) {
    const raw = fs.readFileSync(f);
    c = { mtime: st.mtimeMs, raw, gz: zlib.gzipSync(raw, { level: 9 }), etag: '"' + crypto.createHash('md5').update(raw).digest('hex') + '"' };
    pageCache.set(f, c);
  }
  const H = { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache', 'ETag': c.etag, 'Vary': 'Accept-Encoding' };
  if (req.headers['if-none-match'] === c.etag) { res.writeHead(304, H); return res.end(); }
  if (/\bgzip\b/.test(req.headers['accept-encoding'] || '')) { res.writeHead(200, { ...H, 'Content-Encoding': 'gzip' }); return res.end(c.gz); }
  res.writeHead(200, H); res.end(c.raw);
}

async function handle(req, res) {
  const u = new URL(req.url, 'http://x'), p = u.pathname, m = req.method;

  if (p === '/old') { // старый интерфейс: отдельная страница (old.html), тот же сервер и те же аккаунты
    const f = path.join(__dirname, 'old.html');
    if (!fs.existsSync(f)) { res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }); return res.end('Не найден old.html рядом с server.js'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' }); return fs.createReadStream(f).pipe(res);
  }
  if (p === '/') {
    const f = path.join(__dirname, 'index.html');
    if (!fs.existsSync(f)) { res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' }); return res.end('Не найден index.html рядом с server.js'); }
    return servePage(req, res, f);
  }

  if (p.startsWith('/uploads/')) { // файлы с поддержкой Range (перемотка видео)
    const f = path.join(UP, path.basename(p));
    if (!fs.existsSync(f)) { res.writeHead(404); return res.end(); }
    const size = fs.statSync(f).size, type = MIME[path.extname(f).slice(1)] || 'application/octet-stream';
    const h = { 'Content-Type': type, 'Accept-Ranges': 'bytes', 'X-Content-Type-Options': 'nosniff' };
    if (!MIME[path.extname(f).slice(1)]) h['Content-Disposition'] = 'attachment'; // любые другие файлы только скачиваются
    const r = /bytes=(\d*)-(\d*)/.exec(req.headers.range || '');
    if (r) {
      const s = +r[1] || 0, e = +r[2] || size - 1;
      res.writeHead(206, { ...h, 'Content-Range': `bytes ${s}-${e}/${size}`, 'Content-Length': e - s + 1 });
      return fs.createReadStream(f, { start: s, end: e }).pipe(res);
    }
    res.writeHead(200, { ...h, 'Content-Length': size });
    return fs.createReadStream(f).pipe(res);
  }

  if (p === '/api/auth' && m === 'POST') {
    const { mode, login, password } = await body(req);
    if (!/^[\p{L}\d_]{3,20}$/u.test(login || '') || (password || '').length < 4)
      return send(res, 400, { error: 'Логин: 3–20 символов (буквы, цифры, _). Пароль: от 4 символов.' });
    const nick = login.toLowerCase();
    if (mode === 'register') {
      if (db.users[nick]) return send(res, 409, { error: 'Этот логин уже занят' });
      const salt = id();
      db.users[nick] = { salt, hash: hash(password, salt), avatar: null, name: login, username: '', bio: '' };
    } else {
      const usr = db.users[nick];
      if (!usr || usr.bot || usr.hash !== hash(password, usr.salt)) return send(res, 401, { error: 'Неверный логин или пароль' });
    }
    const token = id() + id();
    db.tokens[token] = nick; save();
    return send(res, 200, { token });
  }

  const me = db.tokens[req.headers['x-token'] || u.searchParams.get('token')];
  if (!me) return send(res, 401, { error: 'auth' });
  touch(me);

  if (p === '/events') {
    res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive', 'X-Accel-Buffering': 'no' });
    res.write(':\n\n');
    if (!clients.has(me)) clients.set(me, new Set());
    clients.get(me).add(res);
    const ka = setInterval(() => res.write(':\n\n'), 15000);
    return req.on('close', () => { clearInterval(ka); clients.get(me).delete(res); const u2 = db.users[me]; if (u2 && !clients.get(me).size) { u2.seen = u2.off = Date.now(); save(); } });
  }

  if (p === '/api/poll') { // запасной канал: клиент опрашивает сервер, если SSE буферизуется туннелем
    const now = Date.now(), since = +u.searchParams.get('since') || 0;
    const mine = Object.values(db.chats).filter(c => c.members.includes(me) && !((db.users[me].hidden) || []).includes(c.id)), msgs = [], reacts = [];
    mine.forEach(c => { const a = alive(c); for (let i = a.length - 1; i >= 0 && a[i].ts >= since; i--) msgs.push({ chat: c.id, msg: out(a[i]) }); });
    mine.forEach(c => db.msgs[c.id].slice(-100).forEach(x => { if (x.rts >= since) reacts.push({ chat: c.id, id: x.id, react: x.react || {} }); }));
    const edits = [], dels = []; mine.forEach(c => { (db.msgs[c.id] || []).slice(-100).forEach(x => { if (x.ets >= since) edits.push({ chat: c.id, msg: out(x) }); }); (c.dels || []).forEach(d => { if (d.ts >= since) dels.push({ chat: c.id, id: d.id }); }); });
    msgs.sort((x, y) => x.msg.ts - y.msg.ts);
    return send(res, 200, { now, chats: mine.map(view), msgs, reacts, edits, dels, blocked: db.users[me].blocked || [] });
  }

  if (p === '/api/me') {
    annaHello(me);
    const chats = Object.values(db.chats).filter(c => c.members.includes(me) && !((db.users[me].hidden) || []).includes(c.id)).map(view);
    const unread = {}; chats.forEach(v => { const n = countUnread(db.chats[v.id], me); if (n) unread[v.id] = n; });
    return send(res, 200, { ...pub(me, true), chats, unread, archived: db.users[me].archived || [], pinned: db.users[me].pinned || [], blocked: db.users[me].blocked || [], now: Date.now() });
  }

  if (p === '/api/e2e/key' && m === 'POST') { // публичный ключ ECDH P-256 (65 байт, base64); приватный ключ остаётся на устройстве
    const { key } = await body(req), raw = typeof key === 'string' && /^[A-Za-z0-9+/]{87}=$/.test(key) ? Buffer.from(key, 'base64') : null;
    if (!raw || raw.length !== 65 || raw[0] !== 4) return send(res, 400, { error: 'Некорректный ключ' });
    if (db.users[me].e2e !== key) {
      db.users[me].e2e = key; save();
      Object.values(db.chats).filter(c => c.type === 'dm' && c.members.includes(me)).forEach(c => push(c.members.filter(n => n !== me), { t: 'chat', chat: view(c) }));
    }
    return send(res, 200, { ok: true });
  }
  if (p === '/api/stickers') { // личные стикеры пользователя (картинки, загруженные заранее)
    const usr = db.users[me]; usr.stickers = usr.stickers || [];
    if (m === 'POST') {
      const { url } = await body(req);
      if (!/^\/uploads\/\w+\.(png|jpg|gif|webp)$/.test(url || '')) return send(res, 400, { error: 'Нужна картинка' });
      if (usr.stickers.length >= 60) return send(res, 400, { error: 'Максимум 60 стикеров' });
      if (!usr.stickers.includes(url)) { usr.stickers.push(url); save(); }
    }
    if (m === 'DELETE') { usr.stickers = usr.stickers.filter(x => x !== u.searchParams.get('url')); save(); }
    return send(res, 200, usr.stickers);
  }

  if (p === '/api/packs') { const r = packx(db.users[me], me, m, u.searchParams, m === 'POST' ? await body(req) : {}, HX); return send(res, r[0], r[1]); }

  if (p === '/api/gifs') { // личные GIF пользователя: gif, картинки и короткие видео
    const usr = db.users[me]; usr.gifs = usr.gifs || [];
    if (m === 'POST') {
      const { url } = await body(req);
      if (!/^\/uploads\/\w+\.(png|jpg|gif|webp|mp4|webm)$/.test(url || '')) return send(res, 400, { error: 'Нужно видео или .gif' });
      if (usr.gifs.length >= 60) return send(res, 400, { error: 'Максимум 60 стикеров' });
      if (!usr.gifs.includes(url)) { usr.gifs.push(url); save(); }
    }
    if (m === 'DELETE') { usr.gifs = usr.gifs.filter(x => x !== u.searchParams.get('url')); save(); }
    return send(res, 200, usr.gifs);
  }

  if (p === '/api/search') {
    const q = (u.searchParams.get('q') || '').toLowerCase().trim();
    if (!q) return send(res, 200, { users: [], chats: [] });
    return send(res, 200, {
      users: Object.keys(db.users).filter(n => { const x = pub(n), k = q.replace(/^@/, ''); return n !== me && [(db.users[n].priv || {}).login && n !== k ? '' : n, x.name.toLowerCase(), x.username.toLowerCase()].some(v => v.includes(k)); }).slice(0, 20).map(n => pub(n)),
      chats: Object.values(db.chats).filter(c => c.type !== 'dm' && !c.private && c.name.toLowerCase().includes(q)).slice(0, 20).map(c => ({ ...view(c), last: undefined })) // тем, кто ещё не вступил, последнее сообщение не показываем
    });
  }

  if (p === '/api/upload' && m === 'POST') {
    const ext = EXT[req.headers['content-type']] || 'bin'; // не-медиа сохраняем как .bin
    const name = id() + '.' + ext, file = path.join(UP, name), ws = fs.createWriteStream(file);
    let n = 0, failed = false;
    req.on('data', c => {
      if ((n += c.length) > 5e7 && !failed) {
        failed = true; req.unpipe(ws); ws.destroy(); fs.unlink(file, () => {});
        send(res, 413, { error: 'Файл больше 50 МБ' });
      }
    });
    req.pipe(ws);
    ws.on('finish', () => { if (!failed) send(res, 200, { url: '/uploads/' + name, mtype: /^(mp3|ogg|m4a)$/.test(ext) ? 'audio' : /mp4|webm/.test(ext) ? 'video' : ext === 'bin' ? 'file' : 'image' }); });
    return;
  }

  if (p === '/api/user') {
    const n = (u.searchParams.get('nick') || '').toLowerCase();
    return db.users[n] ? send(res, 200, { ...pub(n, n === me), ...(n === me ? {} : { blocked: blockedBy(me, n) }) }) : send(res, 404, { error: 'Пользователь не найден' });
  }

  if (p === '/api/profile' && m === 'POST') {
    const { name, username, bio, channel, music, hideSeen, priv } = await body(req), nm = String(name || '').trim(), un = String(username || '').trim().replace(/^@/, '');
    if (!nm || nm.length > 30) return send(res, 400, { error: 'Ник: от 1 до 30 символов' });
    if (un && !/^[a-z][a-z0-9_]{2,19}$/i.test(un)) return send(res, 400, { error: 'Юзернейм: латиница, цифры, _; 3–20 символов, с буквы' });
    if (un && Object.entries(db.users).some(([k, x]) => k !== me && (x.username || '').toLowerCase() === un.toLowerCase())) return send(res, 409, { error: 'Этот юзернейм занят' });
    Object.assign(db.users[me], { name: nm, username: un, bio: String(bio || '').trim().slice(0, 120) });
    if (hideSeen !== undefined) db.users[me].hideSeen = !!hideSeen;
    if (priv && typeof priv === 'object') db.users[me].priv = Object.fromEntries(['bio', 'avatar', 'banner', 'username', 'login', 'extras'].map(k => [k, !!priv[k]]));
    if (channel !== undefined) { const c = db.chats[channel]; db.users[me].channel = c && c.type === 'channel' && c.owner === me && !c.private ? c.id : ''; } // только свой канал
    if (music !== undefined) {
      const mu = music || {}, t = String(mu.title || '').trim().slice(0, 60), url = String(mu.url || '');
      db.users[me].music = t && /^(|\/uploads\/\w+\.(mp3|ogg|m4a))$/.test(url) ? { title: t, artist: String(mu.artist || '').trim().slice(0, 60), url } : null;
    }
    save();
    return send(res, 200, pub(me, true));
  }

  if (p === '/api/banner' && m === 'POST') { // баннер профиля: фото или видео (пустой url — убрать)
    const { url } = await body(req);
    if (!url) { delete db.users[me].banner; save(); return send(res, 200, pub(me, true)); }
    const mt = /^\/uploads\/\w+\.(png|jpg|gif|webp|mp4|webm)$/.exec(url);
    if (!mt || !fs.existsSync(path.join(UP, path.basename(url)))) return send(res, 400, { error: 'Нужно фото или видео' });
    db.users[me].banner = { url, mtype: /mp4|webm/.test(mt[1]) ? 'video' : 'image' }; save();
    return send(res, 200, pub(me, true));
  }

  if (p === '/api/avatar' && m === 'POST') {
    const { url } = await body(req);
    if (!/^\/uploads\/\w+\.(png|jpg|gif|webp)$/.test(url || '')) return send(res, 400, { error: 'Нужна картинка' });
    db.users[me].avatar = url; save();
    return send(res, 200, { ok: true });
  }

  if (p === '/api/chats' && m === 'POST') {
    const { type, name, avatar, members, ttl, private: priv } = await body(req);
    if (!['group', 'channel'].includes(type) || !name || !name.trim() || name.length > 40) return send(res, 400, { error: 'Введите название до 40 символов' });
    if (avatar && !/^\/uploads\/\w+\.(png|jpg|gif|webp)$/.test(avatar)) return send(res, 400, { error: 'Нужна картинка' });
    const c = { id: id(), type, name: name.trim(), owner: me, members: [me] };
    if (avatar) c.avatar = avatar;
    if (priv) { c.private = true; c.invite = id(); } // приватный: не виден в поиске, вход только по ссылке-приглашению
    if (TTLS.includes(+ttl) && +ttl) c.ttl = +ttl;
    (Array.isArray(members) ? members : []).slice(0, 200).forEach(n => { n = String(n).toLowerCase(); if (db.users[n] && !c.members.includes(n) && !blockedBy(n, me)) c.members.push(n); }); // участники, выбранные при создании
    c.reads = Object.fromEntries(c.members.map(n => [n, n === me ? Date.now() : 0])); // у приглашённых всё новое непрочитано
    db.chats[c.id] = c; db.msgs[c.id] = []; save();
    push(c.members, { t: 'chat', chat: view(c) });
    return send(res, 200, view(c));
  }

  if (p === '/api/block' && m === 'POST') { // заблокировать / разблокировать пользователя
    const { nick, on } = await body(req), t = String(nick || '').toLowerCase(), usr = db.users[me];
    if (!db.users[t] || t === me) return send(res, 404, { error: 'Пользователь не найден' });
    usr.blocked = (usr.blocked || []).filter(x => x !== t); if (on) usr.blocked.push(t); save();
    return send(res, 200, { blocked: usr.blocked });
  }

  if (p === '/api/dm' && m === 'POST') {
    const { nick } = await body(req), to = (nick || '').toLowerCase();
    if (!db.users[to] || to === me) return send(res, 404, { error: 'Пользователь не найден' });
    return send(res, 200, view(getDm(me, to)));
  }

  // ---------- истории ----------
  if (p === '/api/stories') {
    if (m === 'GET') {
      const n = (u.searchParams.get('nick') || '').toLowerCase();
      if (!db.users[n]) return send(res, 404, { error: 'Пользователь не найден' });
      return send(res, 200, { user: pub(n, n === me), stories: live().filter(x => x.from === n).sort((a, b) => a.ts - b.ts).map(x => sview(x, me)) });
    }
    if (m === 'POST') {
      const { media } = await body(req);
      if (!/^\/uploads\/\w+\.(png|jpg|gif|webp|mp4|webm)$/.test(media || '')) return send(res, 400, { error: 'Нужно фото или видео' });
      if (live().filter(x => x.from === me).length >= MAX_STORIES) return send(res, 400, { error: 'Максимум ' + MAX_STORIES + ' историй за сутки' });
      const st = { id: id(), from: me, media, mtype: /\.(mp4|webm)$/.test(media) ? 'video' : 'image', ts: Date.now(), react: {} };
      db.stories.push(st); save();
      return send(res, 200, sview(st, me));
    }
    if (m === 'DELETE') {
      const st = db.stories.find(x => x.id === u.searchParams.get('id'));
      if (!st || st.from !== me) return send(res, 404, { error: 'История не найдена' });
      db.stories = db.stories.filter(x => x !== st); save();
      return send(res, 200, { ok: true });
    }
  }

  if (p === '/api/stories/react' && m === 'POST') {
    const { id: sid, emoji } = await body(req), st = live().find(x => x.id === sid);
    if (!st) return send(res, 404, { error: 'История больше недоступна' });
    if (st.from === me) return send(res, 400, { error: 'Нельзя реагировать на свою историю' });
    if (!REACTS.includes(emoji)) return send(res, 400, { error: 'Нельзя поставить такую реакцию' });
    { const be = blockErr(me, st.from); if (be) return send(res, 403, { error: be }); }
    st.react = st.react || {};
    const same = st.react[me] === emoji;
    if (same) delete st.react[me]; else st.react[me] = emoji;
    save();
    const r = { my: same ? null : emoji };
    if (!same) { // автору историй приходит сообщение в личку
      const c = getDm(me, st.from);
      r.chat = view(c);
      r.msg = addMsg(c, { id: id(), from: me, avatar: db.users[me].avatar, text: emoji + ' Реакция на историю', story: storyRef(st), ts: Date.now() });
    }
    return send(res, 200, r);
  }

  if (p === '/api/stories/reply' && m === 'POST') {
    const { id: sid, text } = await body(req), st = live().find(x => x.id === sid), t = String(text || '').trim().slice(0, 4000);
    if (!st) return send(res, 404, { error: 'История больше недоступна' });
    if (st.from === me) return send(res, 400, { error: 'Нельзя отвечать на свою историю' });
    if (!t) return send(res, 400, { error: 'Пустое сообщение' });
    { const be = blockErr(me, st.from); if (be) return send(res, 403, { error: be }); }
    const c = getDm(me, st.from);
    return send(res, 200, { chat: view(c), msg: addMsg(c, { id: id(), from: me, avatar: db.users[me].avatar, text: t, story: storyRef(st), ts: Date.now() }) });
  }

  // ---------- техподдержка ----------
  if (p === '/api/support') {
    const staff = STAFF.has(me);
    if (m === 'POST') {
      const { subject, text } = await body(req), sj = String(subject || '').trim().slice(0, 80), t = String(text || '').trim().slice(0, 2000);
      if (!sj || !t) return send(res, 400, { error: 'Укажите тему и опишите проблему' });
      if (db.tickets.filter(x => x.from === me && x.status !== 'closed').length >= 5) return send(res, 400, { error: 'Слишком много открытых обращений: дождитесь ответа или закройте старые' });
      const now = Date.now(), tk = { id: id(), from: me, subject: sj, status: 'open', ts: now, upd: now, msgs: [{ id: id(), from: me, text: t, ts: now }] };
      db.tickets.push(tk); save();
      return send(res, 200, tview(tk, me));
    }
    return send(res, 200, { staff, tickets: db.tickets.filter(x => staff || x.from === me).sort((a, b) => b.upd - a.upd).map(x => tview(x, me)) });
  }
  const sm = p.match(/^\/api\/support\/(\w+)\/(msg|close)$/);
  if (sm && m === 'POST') {
    const tk = db.tickets.find(x => x.id === sm[1]), staff = STAFF.has(me);
    if (!tk || (tk.from !== me && !staff)) return send(res, 404, { error: 'Обращение не найдено' });
    const { text, on } = await body(req);
    if (sm[2] === 'close') { tk.status = on === false ? 'open' : 'closed'; tk.upd = Date.now(); save(); return send(res, 200, tview(tk, me)); }
    const t = String(text || '').trim().slice(0, 2000);
    if (!t) return send(res, 400, { error: 'Пустое сообщение' });
    if (tk.status === 'closed') return send(res, 400, { error: 'Обращение закрыто' });
    if (tk.msgs.length >= 200) return send(res, 400, { error: 'В обращении слишком много сообщений' });
    const isStaff = staff && tk.from !== me;
    tk.msgs.push({ id: id(), from: me, staff: isStaff, text: t, ts: Date.now() }); tk.status = isStaff ? 'answered' : 'open'; tk.upd = Date.now(); save();
    return send(res, 200, tview(tk, me));
  }

  const cm = p.match(/^\/api\/chats\/(\w+)\/(join|msgs|msg|react|read|archive|members|edit|admin|kick|leave|comments|invite|log|stats|links|settings|fwd|pin|msgedit|msgdel|chatpin|chatdel)$/);
  if (cm) {
    const c = db.chats[cm[1]];
    if (!c) return send(res, 404, { error: 'Чат не найден' });
    if (cm[2] === 'join') {
      if (c.type === 'dm') return send(res, 403, { error: 'Нельзя' });
      if (!c.members.includes(me)) {
        const { code } = await body(req), par = c.parent && db.chats[c.parent]; // комментарии приватного канала открыты его участникам
        if (c.private && !inviteOk(c, code) && !(par && par.members.includes(me))) return send(res, 403, { error: 'Чат приватный: вступить можно только по ссылке-приглашению' });
        c.members.push(me); (c.reads = c.reads || {})[me] = Date.now(); onJoin(c, me, HX); save(); // историю до вступления прочитанной считаем
      }
      push([me], { t: 'chat', chat: view(c) });
      return send(res, 200, view(c));
    }
    if (cm[2] === 'members') {
      if ((c.type === 'dm' || c.private) && !c.members.includes(me)) return send(res, 403, { error: 'Нет доступа' });
      return send(res, 200, c.members.map(n => ({ ...pub(n, n === me), role: n === c.owner ? 'owner' : (c.admins || []).includes(n) ? 'admin' : 'member' })));
    }
    if (!c.members.includes(me)) return send(res, 403, { error: 'Вы не участник чата' });
    if (cm[2] === 'read' && m === 'POST') { markRead(c, me, Date.now()); return send(res, 200, { ok: true }); } // собеседник открыл чат: всё до этого момента прочитано
    if (cm[2] === 'msgs') return send(res, 200, alive(c).filter(x => histOk(c, me, x)).slice(-200).map(out));
    if (cm[2] === 'react' && m === 'POST') { // реакции как в iMessage: у каждого участника не больше одной на сообщение
      const { msg: mid, emoji } = await body(req), mm = db.msgs[c.id].find(x => x.id === mid);
      if (!mm || !REACTS.includes(emoji)) return send(res, 400, { error: 'Нельзя поставить такую реакцию' });
      if (!reactOk(c, emoji)) return send(res, 400, { error: 'В этом чате реакции ограничены' });
      mm.react = mm.react || {};
      if (mm.react[me] === emoji) delete mm.react[me]; else mm.react[me] = emoji;
      if (!Object.keys(mm.react).length) delete mm.react;
      mm.rts = Date.now(); save();
      push(c.members, { t: 'react', chat: c.id, id: mm.id, react: mm.react || {} });
      return send(res, 200, { id: mm.id, react: mm.react || {} });
    }
    if (cm[2] === 'chatpin' && m === 'POST') { // закрепить чат наверху списка: личный для каждого пользователя
      const { on } = await body(req), usr = db.users[me];
      usr.pinned = (usr.pinned || []).filter(x => x !== c.id); if (on) usr.pinned.unshift(c.id); save();
      return send(res, 200, { pinned: usr.pinned });
    }
    if (cm[2] === 'chatdel' && m === 'POST') { // удалить чат: «для себя» (из списка, закрепов, архива; в группе и канале — выход) или «для всех»
      const { both } = await body(req);
      if (both) { // навсегда, у всех: в личке — у обоих собеседников, в группе и канале — только владелец
        if (c.type !== 'dm' && c.owner !== me) return send(res, 403, { error: 'Только владелец' });
        const gone = [c]; if (c.type === 'channel' && c.discuss && db.chats[c.discuss]) gone.push(db.chats[c.discuss]);
        const mem = new Set(); gone.forEach(g => { g.members.forEach(n => mem.add(n)); delete db.chats[g.id]; delete db.msgs[g.id]; });
        const ids = gone.map(g => g.id);
        Object.values(db.users).forEach(u => { ['pinned', 'archived', 'hidden'].forEach(k => { if (u[k]) u[k] = u[k].filter(x => !ids.includes(x)); }); if (ids.includes(u.channel)) u.channel = ''; });
        if (c.parent && db.chats[c.parent]) delete db.chats[c.parent].discuss;
        save(); ids.forEach(i => push([...mem], { t: 'chatgone', chat: i }));
        return send(res, 200, { ok: true });
      }
      if (c.type !== 'dm' && c.owner === me) return send(res, 400, { error: 'Владелец не может удалить свой чат' });
      const usr = db.users[me];
      usr.hidden = [...new Set([...(usr.hidden || []), c.id])];
      usr.pinned = (usr.pinned || []).filter(x => x !== c.id);
      usr.archived = (usr.archived || []).filter(x => x !== c.id);
      if (c.type !== 'dm') { c.members = c.members.filter(n => n !== me); c.admins = (c.admins || []).filter(n => n !== me); }
      save(); return send(res, 200, { ok: true });
    }
    if (cm[2] === 'archive' && m === 'POST') { // архив чатов: личный для каждого пользователя
      const { on } = await body(req), usr = db.users[me];
      usr.archived = (usr.archived || []).filter(x => x !== c.id); if (on) usr.archived.push(c.id); save();
      return send(res, 200, { archived: usr.archived });
    }
    if (['fwd', 'pin', 'msgedit', 'msgdel'].includes(cm[2]) && m === 'POST') {
      const B = await body(req);
      if (cm[2] === 'fwd') { const t = db.chats[B.to]; if (t && t.type === 'dm') { const be = blockErr(me, t.members.find(n => n !== me)); if (be) return send(res, 403, { error: be }); } }
      const r = msgx(c, me, cm[2], B, HX); return send(res, r[0], r[1]);
    }
    if (cm[2] !== 'msg' && c.type !== 'dm') { // управление группой/каналом
      const owner = c.owner === me, isAdmin = owner || (c.admins || []).includes(me);
      const B = await body(req), { name, avatar, nick, on, ttl, private: priv } = B, t = String(nick || '').toLowerCase();
      const upd = () => { chatLog(c, me, (LOGN[cm[2]] || cm[2]) + (t ? ': ' + t : '')); save(); push(c.members, { t: 'chat', chat: view(c) }); return send(res, 200, view(c)); };
      if (['log', 'stats', 'links', 'settings'].includes(cm[2])) { const r = extra(c, me, cm[2], m, B, HX); return send(res, r[0], r[1]); }
      if (cm[2] === 'leave') {
        if (owner) return send(res, 400, { error: 'Владелец не может выйти из своего чата' });
        c.members = c.members.filter(n => n !== me); c.admins = (c.admins || []).filter(n => n !== me); save();
        return send(res, 200, { ok: true });
      }
      if (cm[2] === 'edit') {
        if (!owner) return send(res, 403, { error: 'Только владелец' });
        if (name !== undefined) { if (!String(name).trim() || String(name).length > 40) return send(res, 400, { error: 'Название: до 40 символов' }); c.name = String(name).trim(); }
        if (avatar !== undefined) { if (!/^\/uploads\/\w+\.(png|jpg|gif|webp)$/.test(avatar || '')) return send(res, 400, { error: 'Нужна картинка' }); c.avatar = avatar; }
        if (ttl !== undefined) { if (!TTLS.includes(+ttl)) return send(res, 400, { error: 'Нельзя' }); c.ttl = +ttl; }
        if (priv !== undefined && !c.parent) { // приватность меняет только владелец; чат комментариев наследует её от канала
          const was = !!c.private; c.private = !!priv;
          if (c.private && (!was || !c.invite)) c.invite = id(); // при каждом включении — новая ссылка
          if (c.discuss && db.chats[c.discuss]) db.chats[c.discuss].private = c.private;
        }
        return upd();
      }
      if (cm[2] === 'invite') {
        if (!isAdmin || !c.private || c.parent) return send(res, 403, { error: 'Нет доступа' });
        if (m === 'POST') { c.invite = id(); save(); } // сбросить: старая ссылка перестаёт работать
        return send(res, 200, { invite: c.invite });
      }
      if (cm[2] === 'admin') {
        if (!owner) return send(res, 403, { error: 'Только владелец' });
        if (!c.members.includes(t) || t === c.owner) return send(res, 400, { error: 'Нельзя' });
        c.admins = (c.admins || []).filter(n => n !== t); if (on) c.admins.push(t);
        return upd();
      }
      if (cm[2] === 'kick') {
        if (!isAdmin || !c.members.includes(t) || t === c.owner || (!owner && (c.admins || []).includes(t))) return send(res, 403, { error: 'Нельзя' });
        c.members = c.members.filter(n => n !== t); c.admins = (c.admins || []).filter(n => n !== t);
        return upd();
      }
      if (cm[2] === 'comments') {
        if (!owner || c.type !== 'channel' || c.discuss) return send(res, 400, { error: 'Нельзя' });
        const g = { id: id(), type: 'group', name: c.name + ' · комментарии', owner: me, members: [me], admins: [], parent: c.id, private: !!c.private };
        db.chats[g.id] = g; db.msgs[g.id] = []; c.discuss = g.id;
        push([me], { t: 'chat', chat: view(g) });
        return upd();
      }
    }
    if (cm[2] === 'msg' && m === 'POST') {
      const { text = '', media, mtype, fname, reply, sticker, enc } = await body(req);
      const eo = enc === undefined ? null : encValid(enc);
      if (enc !== undefined && !eo) return send(res, 400, { error: 'Некорректное шифрованное сообщение' });
      if (eo && (c.type !== 'dm' || c.members.some(n => (db.users[n] || {}).bot))) return send(res, 400, { error: 'Шифрование доступно только в личных чатах с людьми' });
      if (c.type === 'channel' && c.owner !== me && !(c.admins || []).includes(me)) return send(res, 403, { error: 'В канале пишут только владелец и админы' });
      if (c.type === 'dm') { const be = blockErr(me, c.members.find(n => n !== me)); if (be) return send(res, 403, { error: be }); }
      const t = eo ? '' : String(text).trim().slice(0, 4000), okm = /^\/uploads\/\w+\.\w+$/.test(media || '');
      const stk = typeof sticker === 'string' && [...sticker].length <= 8 && /^\p{Extended_Pictographic}/u.test(sticker) ? sticker : null; // стикер-эмодзи
      { const pe = permErr(c, me, { text: t, media: okm && media, mtype, sticker: stk }); if (pe) return send(res, 403, { error: pe }); }
      if (!t && !okm && !stk && !eo) return send(res, 400, { error: 'Пустое сообщение' });
      const msg = { id: id(), from: me, avatar: db.users[me].avatar, text: t, ts: Date.now() };
      const rm = reply && db.msgs[c.id].find(x => x.id === reply);
      if (rm) msg.reply = { id: rm.id, from: rm.from, ...(rm.enc ? { enc: true } : {}), text: rm.enc ? '' : (rm.text || (rm.sticker ? rm.sticker + ' Стикер' : rm.mtype === 'sticker' ? '🏷 Стикер' : rm.mtype === 'file' ? '📄 ' + rm.fname : '🖼 Медиа')).slice(0, 100) };
      if (eo) msg.enc = eo;
      if (stk) { msg.sticker = stk; msg.text = ''; }
      else if (okm) { msg.media = media; msg.mtype = ['video', 'file', 'sticker'].includes(mtype) ? mtype : 'image'; if (msg.mtype === 'file') msg.fname = String(fname || 'файл').slice(0, 120); }
      db.msgs[c.id].push(msg); save();
      if (c.type === 'dm') c.members.forEach(n => { const u = db.users[n]; if (u && (u.hidden || []).includes(c.id)) { u.hidden = u.hidden.filter(x => x !== c.id); save(); push([n], { t: 'chat', chat: view(c) }); } });
      push(c.members, { t: 'msg', chat: c.id, msg: out(msg) });
      // ответив в чате, человек автоматически «прочитал» всё до своего сообщения
      if (db.msgs[c.id].some(x => x.from !== me && x.ts > ((c.reads || {})[me] || 0))) markRead(c, me, msg.ts);
      if (c.type === 'dm' && c.members.includes(ANNA) && me !== ANNA) { setTimeout(() => markRead(c, ANNA, Date.now()), 350); annaReply(c, me, msg); } // бот «читает» почти сразу
      return send(res, 200, out(msg));
    }
  }
  send(res, 404, { error: 'Не найдено' });
}


const ANNA = 'anna';
// ---------- бот Анна: официальный помощник, рассказывает о функциях ----------
const ANNA_ALL = '✨ Что умеет Рейс:\n🎨 Темы оформления — Киберпанк, Майнкрафт, Космос, Океан и другие\n😀 Реакции на сообщения\n🎞 Стикеры и GIF\n📌 Закреплённые сообщения\n↪️ Пересылка сообщений\n📁 Папки и 🗂 архив чатов\n🔋 Режим экономии энергии\n\nСпроси меня про любую функцию — например «темы», «реакции» или «папки» 🙂';
const ANNA_HELLO = n => ['Привет, ' + n + '! 👋 Я Анна — официальный помощник Рейс мессенджера. Расскажу, что у нас нового.', ANNA_ALL];
const annaP = s => new RegExp('(^|[^а-яa-z0-9])(' + s + ')');
const ANNA_KB = [
  [annaP('автоудал|исчезающ|самоуничтож'), '⏳ Автоудаление: владелец группы или канала открывает окно информации (нажми на название чата) и выбирает, через сколько удалять сообщения: 1 день, неделя или месяц.'],
  [annaP('тем|оформл|дизайн|киберпанк|майнкрафт|космос|океан|закат|цвет|фон|theme'), '🎨 Темы: Настройки → Чаты → Тема оформления. Есть Киберпанк, Майнкрафт, Космос, Океан, Закат, Золото, Хакер, Джунгли, Япония, Новый год, Осень и другие. А ещё можно собрать свою тему в редакторе («Правка темы») 💜'],
  [annaP('эконом|энерг|батаре|греет|анимац|снег|эффект|тормоз|лагает|battery'), '🔋 Режим экономии: Настройки → Экономия энергии. Он отключает анимации, падающий снег и размытие фона — телефон меньше греется. Эффекты темы можно отключить и отдельно, в настройках чатов.'],
  [annaP('реакц|эмодзи|лайк|сердеч|react'), '😀 Реакции: двойной клик по сообщению ставит ❤️, а правый клик (долгое нажатие) открывает меню с другими: 👍 👎 😂 ‼️ ❓. Нажми на ту же реакцию ещё раз — она уберётся.'],
  [annaP('стикер|гиф|gif|пак|sticker'), '🎞 Стикеры и GIF: кнопка рядом с полем ввода. Есть готовые паки (Эмоции, Зверята, Вкусняшки…), можно добавить свои картинки (до 60) и личные GIF.'],
  [annaP('закреп|pin'), '📌 Закреп: в меню сообщения выбери «Закрепить» — оно появится сверху чата. В группах и каналах закреплять могут владелец и админы.'],
  [annaP('пересл|forward|репост'), '↪️ Пересылка: в меню сообщения выбери «Переслать в…» и укажи чат, куда отправить.'],
  [annaP('ответит|редакт|изменит|удалит|меню|reply|edit|delete'), '💬 Меню сообщения (правый клик или долгое нажатие): ответить, изменить, удалить, переслать, закрепить и поставить реакцию.'],
  [annaP('папк|архив|folder|archive'), '📁 Папки: Настройки → Папки для чатов — над списком появятся вкладки «Все», «Личные», «Группы», «Каналы».\n🗂 Архив: убери чат из основного списка в архив, он личный для каждого — у остальных ничего не поменяется.'],
  [annaP('истори|stories|story'), '📸 Истории живут 24 часа. Реагировать на них можно эмодзи, а автор видит, кто и как отреагировал.'],
  [annaP('групп|канал|комментар|приватн|ссылк|приглаш|админ|владел|участник|статистик|журнал|channel|group'), '👥 Группы и 📢 каналы: в канале пишут только владелец и админы, а для обсуждения можно включить комментарии. Приватный чат не виден в поиске — вступить можно только по ссылке-приглашению. У владельца есть статистика, журнал действий, приветствие новичков и настройка прав.'],
  [annaP('профил|аккаунт|юзернейм|username|аватар|баннер|о себе|скрыть|скрыт|музык|profile'), '👤 Профиль: юзернейм (@), «О себе», аватарка и баннер (фото или видео), музыка и канал. На устройстве можно держать несколько аккаунтов и переключаться между ними. Лишнее скрывается в Настройки → Приватность и безопасность.'],
  [annaP('уведом|звук|notification|sound'), '🔔 Уведомления: Настройки → Уведомления — звук и уведомления в браузере.'],
  [annaP('язык|english|language|українськ|polski|беларуск'), '🌐 Язык: русский, English, українська, беларуская и polski — Настройки → Язык.'],
  [annaP('поиск|найти|search'), '🔍 Поиск по настройкам — значок лупы вверху настроек. Людей, группы и каналы ищи через поиск в списке чатов.'],
  [annaP('поддержк|баг|ошибк|проблем|не работает|жалоб|support|bug'), '🛟 Техподдержка: Настройки → Техподдержка. Там частые вопросы и форма обращения к оператору.'],
  [annaP('функци|умеешь|можешь|возможност|что нового|обновлен|помощь|помоги|команд|help|/start|что ты|что можно'), ANNA_ALL],
  [annaP('спасиб|благодар|thanks|thx'), 'Всегда пожалуйста! ☕ Если что — я рядом.'],
  [annaP('кто ты|как тебя|твое имя|ты бот|who are you'), 'Я Анна 🙂 Официальный помощник Рейс мессенджера: рассказываю про функции и подсказываю, где что найти.'],
  [annaP('привет|здравств|хай|здорово|добрый|добрая|доброе|доброго|hello|hi($|[^a-z])|hey'), 'Привет, {name}! 👋 Я Анна. Напиши «функции» — расскажу, что умеет Рейс. Или спроси про что-то конкретное: темы, реакции, стикеры, папки, истории…']
];
const annaAnswer = (text, name, msg) => {
  const x = String(text || '').toLowerCase().replace(/ё/g, 'е').trim();
  if (!x && msg && (msg.media || msg.sticker)) return 'Классно! 👍 Если хочешь узнать про функции — напиши «функции».';
  const k = ANNA_KB.find(e => e[0].test(x));
  return (k ? k[1] : 'Пока не поняла 🙂 Я умею рассказывать о функциях. Попробуй написать: темы, реакции, стикеры, закрепы, пересылка, папки, архив, профиль, истории, каналы, язык или поддержка. Или просто «функции» — расскажу всё.').replace(/\{name\}/g, name || '');
};

function ensureAnna() { // создаёт бота Анну (в неё нельзя войти по паролю)
  const u = db.users[ANNA];
  if (u) { if (!u.bot) console.warn('Ник «anna» занят обычным пользователем — бот Анна не создан'); return; }
  const salt = id();
  db.users[ANNA] = { salt, hash: hash(id(), salt), name: 'Анна', username: 'anna', bio: 'Официальный помощник · расскажу о функциях ✨', avatar: null, bot: 1 };
  save();
}
function annaHello(nick) { // один раз присылает новому игроку (и уже зарегистрированным) рассказ о функциях
  const u = db.users[nick], a = db.users[ANNA];
  if (!u || u.bot || u.annaHi || !a || !a.bot) return;
  u.annaHi = 1;
  const c = getDm(ANNA, nick), t = Date.now();
  ANNA_HELLO(u.name || nick).forEach((x, i) => addMsg(c, { id: id(), from: ANNA, avatar: null, text: x, ts: t + i }));
}
function annaReply(c, nick, msg) { // отвечает на сообщения в личке
  const a = db.users[ANNA]; if (!a || !a.bot) return;
  setTimeout(() => {
    if (!db.chats[c.id]) return;
    addMsg(c, { id: id(), from: ANNA, avatar: null, text: annaAnswer(msg.text, (db.users[nick] || {}).name, msg), ts: Date.now() });
  }, 700 + Math.random() * 800);
}
ensureAnna();

const os = require('os');
const lan = () => Object.values(os.networkInterfaces()).flat().filter(a => a && a.family === 'IPv4' && !a.internal).map(a => a.address);
const server = http.createServer((req, res) => handle(req, res).catch(() => { try { send(res, 500, { error: 'Ошибка сервера' }); } catch {} }));
server.on('connection', s => s.setNoDelay(true));
server.keepAliveTimeout = 65000;
server.on('error', e => {
  if (e.code === 'EADDRINUSE') console.error('Порт ' + PORT + ' уже занят. Закройте другую копию мессенджера или запустите с другим портом:  set PORT=3001 && node server.js');
  else console.error(e);
  process.exit(1);
});
server.listen(PORT, () => {
  console.log('Мессенджер запущен: http://localhost:' + PORT);
  lan().forEach(ip => console.log('Из локальной сети (телефон, другой ПК): http://' + ip + ':' + PORT));
  console.log('Данные: ' + DBF + '\nФайлы: ' + UP + '\nОстановить: Ctrl+C');
});
