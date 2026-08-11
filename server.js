import express from 'express';
import cors from 'cors';
import fs from 'fs';
import path from 'path';
import { makeWASocket, useMultiFileAuthState, DisconnectReason, fetchLatestBaileysVersion, downloadMediaMessage } from '@whiskeysockets/baileys';
import { Boom } from '@hapi/boom';
import { createClient } from '@supabase/supabase-js';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';

process.on('uncaughtException', e => console.error('[KEEP-ALIVE]', e.message));
process.on('unhandledRejection', e => console.error('[KEEP-ALIVE]', String((e && e.message) || e)));

const PORT = process.env.PORT || 10000;
const TOKEN = process.env.BRIDGE_TOKEN || 'CNX-BRIDGE-2026';
const jidDigits = j => (j||'').split('@')[0].split(':')[0].replace(/\D/g,'');
const normName = s => String(s||'').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'').replace(/[^\p{L}\p{N} ]/gu,'').replace(/\s+/g,' ').trim();

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);
const s3 = new S3Client({ region:'auto', endpoint: process.env.R2_ENDPOINT, credentials:{ accessKeyId: process.env.R2_ACCESS_KEY_ID, secretAccessKey: process.env.R2_SECRET_ACCESS_KEY } });
const R2_BUCKET = process.env.R2_BUCKET || 'crm-nexus-media';
const R2_PUBLIC = process.env.R2_PUBLIC || '';

const DBG=[]; function log(...a){ const s=a.map(x=>(x&&typeof x==='object')?JSON.stringify(x):String(x)).join(' '); DBG.push(s); if(DBG.length>300) DBG.shift(); console.log(s); }
function mkInst(id){ return { id, sock:null, qr:null, status:'disconnected', connecting:false, dir:'./session'+id, presence:{}, lastEvent:0, lastSend:0, pushT:null, own:'', sentIds:new Set() }; }
const INST = { 1: mkInst(1), 2: mkInst(2) };
function reconnect(inst){ log('♻️ reconectando WA'+inst.id); inst.status='disconnected'; inst.connecting=false; connect(inst); }

const extOf = m => (m||'').includes('jpeg')?'jpg':(m||'').includes('webp')?'webp':(m||'').includes('png')?'png':(m||'').includes('ogg')?'ogg':(m||'').includes('mp4')?'mp4':(m||'').includes('pdf')?'pdf':'bin';
async function r2Put(buf, mime, inst){
  const key = `wa${inst}/${Date.now()}_${Math.random().toString(36).slice(2,7)}.${extOf(mime)}`;
  await s3.send(new PutObjectCommand({ Bucket: R2_BUCKET, Key: key, Body: buf, ContentType: mime||'application/octet-stream' }));
  return R2_PUBLIC + '/' + key;
}

async function pullSession(inst){
  try{ const { data } = await sb.storage.from('wa-sessions').list('s'+inst.id, { limit: 1000 });
    if(!data||!data.length) return log('☁️ WA'+inst.id+' sin sesión previa');
    for(const f of data){ const { data: blob, error } = await sb.storage.from('wa-sessions').download(`s${inst.id}/${f.name}`);
      if(!error) fs.writeFileSync(path.join(inst.dir, f.name), Buffer.from(await blob.arrayBuffer())); }
    log('☁️ sesión WA'+inst.id+' restaurada'); }catch(e){ log('pull:', e.message); }
}
async function pushSession(inst){
  try{ const files = fs.readdirSync(inst.dir);
    for(const f of files){ const buf = fs.readFileSync(path.join(inst.dir, f));
      await sb.storage.from('wa-sessions').upload(`s${inst.id}/${f}`, buf, { upsert:true, contentType:'application/octet-stream' }); }
    log('☁️ sesión WA'+inst.id+' respaldada'); }catch(e){ log('push:', e.message); }
}
function schedulePush(inst){ clearTimeout(inst.pushT); inst.pushT=setTimeout(()=>pushSession(inst), 3000); }

async function connect(inst){
  if (inst.connecting) return; inst.connecting = true;
  try{
    fs.mkdirSync(inst.dir, { recursive:true });
    await pullSession(inst);
    const { state, saveCreds } = await useMultiFileAuthState(inst.dir);
    const ver = await fetchLatestBaileysVersion().catch(()=>({ version: undefined }));
    inst.sock = makeWASocket({ version: ver.version, auth: state, printQRInTerminal:false, browser:['CRM Nexus WA'+inst.id,'Chrome','1.0'], syncFullHistory:false, generateHighQualityLinkPreviews:false });
    inst.sock.ev.on('creds.update', (u)=>{ saveCreds(u); schedulePush(inst); });
    inst.sock.ev.on('connection.update', u => {
      try{
        if (u.qr){ inst.qr = u.qr; inst.status = 'waiting'; }
        if (u.connection === 'open'){ inst.status = 'connected'; inst.qr = null; inst.lastEvent=Date.now(); inst.own=jidDigits(inst.sock.user?.id||''); schedulePush(inst); log('✅ WA'+inst.id+' SESIÓN ABIERTA'); }
        if (u.connection === 'close'){
          inst.status = 'disconnected';
          const code = new Boom(u.lastDisconnect?.error)?.output?.statusCode;
          if (code === DisconnectReason.loggedOut){ try{ fs.rmSync(inst.dir,{recursive:true,force:true}); }catch(e){} }
          setTimeout(()=>{ inst.connecting=false; connect(inst); }, 3000);
        }
      }catch(e){ log('[conn]', e.message); }
    });
    inst.sock.ev.on('presence.update', ({ id, presences })=>{ try{ for(const p of Object.values(presences||{})){ if(p?.lastKnownPresence==='composing') inst.presence[id]={ts:Date.now()}; } }catch(e){} });
    inst.sock.ev.on('messages.update', async (updates)=>{
      try{ for(const u of updates){ const st=u.update?.status; if(!u.key?.fromMe||!st||st<3) continue;
        await routeReceipt(inst, jidDigits(u.key.remoteJid), u.key.id, st); } }catch(e){}
    });
    inst.sock.ev.on('messages.upsert', async ({ messages, type }) => {
      try{
        inst.lastEvent=Date.now();
        if (type !== 'notify') return;
        for (const m of messages){
          try{
            const rj = m.key.remoteJid||'';
            let fromDir='in';
            if (m.key.fromMe){
              if (inst.sentIds.has(m.key.id)) continue;
              const isSelf = inst.own && jidDigits(rj)===inst.own && !rj.endsWith('@lid');
              if (isSelf){ log('🪞 mensaje propio WA'+inst.id); }
              else { fromDir='out'; log('📤 enviado desde otro dispositivo WA'+inst.id+' → '+rj); }
            }
            if (rj.endsWith('@broadcast') || rj.endsWith('@g.us')) continue;
            log('📥 msg WA'+inst.id+':', rj, m.pushName||'');
            const payload = { from:fromDir, ts:Date.now(), wamid:m.key.id };
            if (fromDir==='out'){ payload.autor='externo'; payload.autorName='📱 Otro dispositivo'; }
            const im=m.message?.imageMessage, au=m.message?.audioMessage, doc=m.message?.documentMessage, vi=m.message?.videoMessage, stk=m.message?.stickerMessage, rea=m.message?.reactionMessage, loc=m.message?.locationMessage||m.message?.liveLocationMessage;
            if (im||au||doc||vi||stk){
              try{
                const buf=await downloadMediaMessage(m,'buffer',{}, { reuploadRequest: inst.sock.updateMediaMessage });
                const mime=im?.mimetype||au?.mimetype||doc?.mimetype||vi?.mimetype||stk?.mimetype||'application/octet-stream';
                payload.type=im?'image':(au?'audio':(vi?'video':(stk?'image':'file')));
                payload.fileName=doc?.fileName||(au?'audio.ogg':(im?'imagen.jpg':(vi?'video.mp4':(stk?'sticker.webp':'archivo'))));
                payload.size=buf?buf.length:0; payload.text=im?.caption||vi?.caption||(doc?doc.fileName:'');
                if (buf){ payload.url = await r2Put(buf, mime, inst.id); }
              }catch(e){ log('media:', e.message); payload.text='📎 Adjunto no descargable'; }
            } else if (rea){ payload.text='❤️ Reacción: '+(rea.text||''); }
            else if (loc){ payload.text='📍 Ubicación: https://maps.google.com/?q='+(loc.degrees||0)+','+(loc.minutes||0); }
            else { const text=m.message?.conversation||m.message?.extendedTextMessage?.text; if(!text) continue; payload.text=text; }
            let jidRaw = rj; const wasLid = rj.endsWith('@lid'); const lid = wasLid ? rj : '';
            if (lid || jidDigits(jidRaw).length>15){ try{ const pn = await inst.sock.signalRepository.lidMapping.getPNForLID(jidRaw); if(pn) jidRaw = pn; }catch(e){} }
            let push = m.pushName;
            if (!push){ try{ push = (inst.sock.store?.contacts?.[rj]?.name) || (inst.sock.chats?.[rj]?.name) || ''; }catch(e){} }
            await routeToCRM(inst, jidDigits(jidRaw), payload, push, lid, wasLid);
          }catch(e){ log('[msg]', e.message); }
        }
      }catch(e){ log('[upsert]', e.message); }
    });
  }catch(e){ log('[connect]', e.message); setTimeout(()=>{ inst.connecting=false; connect(inst); }, 5000); }
  inst.connecting = false;
}

async function allClients(){ const { data, error } = await sb.from('clients').select('*'); if(error){ log('⛔ allClients:', error.message); return []; } return (data||[]).map(r=>({id:r.id,...(r.data||{})})); }
async function getWaSettings(){ try{ const { data } = await sb.from('config').select('value').eq('key','waSettings').maybeSingle(); return data? data.value : {}; }catch(e){ return {}; } }

async function matchClient(clients, inst, digits, lid, pushName){
  let c=null;
  if (lid) c=clients.find(d=>(d.lid||'')!=='' && d.lid===lid && (d.waInst||1)===Number(inst.id));
  if (!c && !lid && digits && digits.length>=10 && digits.length<=15){
    c=clients.find(d=>(d.telefono||'').replace(/\D/g,'')===digits && (d.waInst||1)===Number(inst.id));
    if(!c) c=clients.find(d=>{ const t=(d.telefono||'').replace(/\D/g,''); return t.length>=7 && (digits.endsWith(t)||t.endsWith(digits)); });
  }
    if (!c && pushName){ const pn=normName(pushName);
    if(pn) c=clients.find(d=>{ const n=normName(d.nombre); return !!n && (d.waInst||1)===Number(inst.id) && (n===pn||n.startsWith(pn+' ')||pn.startsWith(n+' ')); });
  }
  return c;
}
async function routeToCRM(inst, digits, payload, pushName, lid, wasLid){
  try{
    log('🧭 route inicio WA'+inst.id+' digits='+digits);
    const clients = await allClients();
    let c = await matchClient(clients, inst, digits, lid, pushName);
    log('🧭 match:', c?c.id:'(nuevo)');
    if (c && lid){
      const cur=await sb.from('clients').select('*').eq('id',c.id).single();
      if(!cur.error) await sb.from('clients').update({data:{...(cur.data.data||{}), lid}}).eq('id',c.id);
    }
    if (!c){
      const ws=await getWaSettings(); const set=(ws&&ws[inst.id])||{};
      const data={ nombre:pushName||('+'+(digits||'desconocido')), telefono:(!wasLid&&digits&&digits.length>=10&&digits.length<=15)?digits:'', pipeline:set.pipelineInbound||'Sin Contactos', stage:'Nuevo lead', origen:'WhatsApp', waInst:Number(inst.id), unread:1, hasChat:true, lastMsgTs:payload.ts||Date.now(), createdAt:Date.now() };
      if (lid) data.lid=lid;
      const ref=await sb.from('clients').insert({data}).select().single();
      if(ref.error){ log('⛔ insert client:', ref.error.message); return; }
      c={ id:ref.data.id };
    }
    if (payload.wamid){ const dup=await sb.from('client_messages').select('id').filter('data->>wamid','eq',payload.wamid).limit(1); if(dup.data&&dup.data.length){ log('♻️ duplicado omitido'); return; } }
    const ins=await sb.from('client_messages').insert({ client_id:c.id, ts:payload.ts||Date.now(), data:payload });
    if(ins.error){ log('⛔ insert msg:', ins.error.message); return; }
    const cur2=await sb.from('clients').select('*').eq('id',c.id).single();
    if(!cur2.error) await sb.from('clients').update({data:{...(cur2.data.data||{}), unread: payload.from==='in' ? (((cur2.data.data||{}).unread||0)+1) : ((cur2.data.data||{}).unread||0), lastMsgTs:payload.ts||Date.now(), hasChat:true}}).eq('id',c.id);
    log('📥 ruteado WA'+inst.id+' →', c.id);
  }catch(e){ log('route:', e.message); }
}
async function routeReceipt(inst, digits, wamid, st){
  try{
    if(!digits||!wamid) return;
    const clients = await allClients();
    const c = await matchClient(clients, inst, digits, '', '');
    if(!c) return;
    const { data } = await sb.from('client_messages').select('*').filter('data->>wamid','eq',wamid).limit(1);
    if(data&&data[0]){ const merged={...(data[0].data||{}), rc:st}; await sb.from('client_messages').update({data:merged}).eq('id',data[0].id); }
  }catch(e){}
}

const app=express();
app.use(cors());
app.use(express.json({ limit:'15mb' }));
const auth=(req,res,next)=> req.headers['x-token']===TOKEN ? next() : res.status(401).json({ok:false,error:'No autorizado'});
const waOf=req=>{ const v=parseInt(req.query.wa||(req.body&&req.body.wa)||'1',10); return INST[v]||INST[1]; };

app.get('/', (req,res)=> res.send('CRM Nexus WhatsApp Bridge v17 ✅ (Supabase)'));
app.get('/health', (req,res)=> res.json({ ok:true, wa1:INST[1].status, wa2:INST[2].status }));
app.get('/dbg', (req,res)=> res.json({ ok:true, wa1:INST[1].status, wa2:INST[2].status, last: DBG.slice(-60) }));
app.get('/qr', auth, (req,res)=>{ const inst=waOf(req); res.json({ ok:true, status:inst.status, qr:inst.qr }); });
app.get('/presence', auth, (req,res)=>{ const inst=waOf(req); const now=Date.now(); const out={};
  for(const [jid,v] of Object.entries(inst.presence||{})){ if(now-(v.ts||0)<6000) out[jid]=true; }
  res.json({ ok:true, presence:out }); });
app.post('/send', auth, async (req,res)=>{
  try{
    const inst=waOf(req); const { to, text }=req.body;
    if (inst.status!=='connected') return res.json({ ok:false, error:'WhatsApp '+inst.id+' no conectado.' });
    const r=await inst.sock.sendMessage(String(to).replace(/\D/g,'')+'@s.whatsapp.net', { text });
    inst.lastSend=Date.now(); if(r?.key?.id){ inst.sentIds.add(r.key.id); setTimeout(()=>inst.sentIds.delete(r.key.id),60000); } log('📨 send WA'+inst.id+' ok');
    res.json({ ok:true, wamid:r?.key?.id });
  }catch(e){ try{ reconnect(waOf(req)); }catch(_){} res.status(500).json({ ok:false, error:String(e.message||e) }); }
});
app.post('/sendMedia', auth, async (req,res)=>{
  try{
    const inst=waOf(req); const { to, mime, data, fileName, caption }=req.body;
    if (inst.status!=='connected') return res.json({ ok:false, error:'WhatsApp '+inst.id+' no conectado.' });
    const buf=Buffer.from((data||'').split(',')[1]||'', 'base64');
    let url=''; try{ url=await r2Put(buf, mime, inst.id); }catch(e){ log('⛔ r2 sendMedia:', e.message); }
    const jid=String(to).replace(/\D/g,'')+'@s.whatsapp.net';
    let r;
    if ((mime||'').startsWith('image/')) r=await inst.sock.sendMessage(jid, { image: buf, caption: caption||undefined });
    else if ((mime||'').startsWith('audio/')){
      try{ r=await inst.sock.sendMessage(jid, { audio: buf, mimetype:'audio/ogg; codecs=opus' }); }
      catch(e){ r=await inst.sock.sendMessage(jid, { document: buf, mimetype:'audio/mpeg', fileName: fileName||'audio.ogg' }); }
    }
    else r=await inst.sock.sendMessage(jid, { document: buf, mimetype: mime||'application/octet-stream', fileName: fileName||'archivo', caption: caption||undefined });
    inst.lastSend=Date.now(); if(r?.key?.id){ inst.sentIds.add(r.key.id); setTimeout(()=>inst.sentIds.delete(r.key.id),60000); } log('📨 sendMedia WA'+inst.id+' ok → '+url);
    res.json({ ok:true, wamid:r?.key?.id, url });
  }catch(e){ try{ reconnect(waOf(req)); }catch(_){} res.status(500).json({ ok:false, error:String(e.message||e) }); }
});
app.post('/react', auth, async (req,res)=>{
  try{
    const inst=waOf(req); const { to, wamid, emoji, fromMe }=req.body;
    if (inst.status!=='connected') return res.json({ ok:false, error:'No conectado' });
    const jid=String(to).replace(/\D/g,'')+'@s.whatsapp.net';
    await inst.sock.sendMessage(jid, { react: { text: emoji||'', key: { id: wamid, fromMe: !!fromMe, remoteJid: jid } } });
    res.json({ ok:true });
  }catch(e){ res.status(500).json({ ok:false, error:String(e.message||e) }); }
});
app.post('/edit', auth, async (req,res)=>{
  try{
    const inst=waOf(req); const { to, wamid, text }=req.body;
    if (inst.status!=='connected') return res.json({ ok:false, error:'No conectado' });
    const jid=String(to).replace(/\D/g,'')+'@s.whatsapp.net';
    await inst.sock.sendMessage(jid, { text: text||'', edit: { id: wamid, fromMe: true, remoteJid: jid } });
    res.json({ ok:true });
  }catch(e){ res.status(500).json({ ok:false, error:String(e.message||e) }); }
});

app.listen(PORT, ()=>{ log('Bridge v17 listo en puerto', PORT); connect(INST[1]); connect(INST[2]); });
setInterval(async ()=>{ for(const inst of [INST[1],INST[2]]){
  if(inst.status==='connected'){
    try{ await inst.sock.sendPresenceUpdate('available'); }catch(e){ reconnect(inst); continue; }
    if(inst.lastSend && inst.lastSend>inst.lastEvent && Date.now()-inst.lastSend>10*60*1000){ log('⚠️ WA'+inst.id+' sin eventos tras envío → reconectando'); reconnect(inst); }
  }
} }, 60000);
