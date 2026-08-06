import express from 'express';
import cors from 'cors';
import { makeWASocket, useMultiFileAuthState, DisconnectReason, fetchLatestBaileysVersion, downloadMediaMessage } from '@whiskeysockets/baileys';
import { Boom } from '@hapi/boom';
import fs from 'fs';
import Jimp from 'jimp';
import admin from 'firebase-admin';

process.on('uncaughtException', e => console.error('[KEEP-ALIVE]', e.message));
process.on('unhandledRejection', e => console.error('[KEEP-ALIVE]', String((e && e.message) || e)));

const PORT = process.env.PORT || 10000;
const TOKEN = process.env.BRIDGE_TOKEN || 'CNX-BRIDGE-2026';
const jidDigits = j => (j||'').split('@')[0].split(':')[0].replace(/\D/g,'');

admin.initializeApp({ credential: admin.credential.cert(JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)) });
const db = admin.firestore();

function mkInst(id){ return { id, sock:null, qr:null, status:'disconnected', connecting:false, dir:'./session'+id }; }
const INST = { 1: mkInst(1), 2: mkInst(2) };

async function storeMedia(cid, buf, mime, fileName){
  const b64 = buf.toString('base64');
  const CHUNK = 700*1024;
  const attId = Date.now()+'_'+Math.random().toString(36).slice(2,8);
  const parts = []; for(let i=0;i<b64.length;i+=CHUNK) parts.push(b64.slice(i,i+CHUNK));
  const batch = db.batch();
  const attRef = db.collection('clients').doc(cid).collection('attachments').doc(attId);
  batch.set(attRef,{mime,fileName,size:buf.length,n:parts.length,ts:Date.now()});
  parts.forEach((p,i)=>batch.set(attRef.collection('chunks').doc(String(i).padStart(4,'0')),{i,d:p}));
  await batch.commit();
  return attId;
}

async function connect(inst){
  if (inst.connecting) return; inst.connecting = true;
  try{
    const { state, saveCreds } = await useMultiFileAuthState(inst.dir);
    const ver = await fetchLatestBaileysVersion().catch(()=>({ version: undefined }));
    inst.sock = makeWASocket({ version: ver.version, auth: state, printQRInTerminal:false, browser:['CRM Nexus WA'+inst.id,'Chrome','1.0'], syncFullHistory:false, generateHighQualityLinkPreviews:false });
    inst.sock.ev.on('creds.update', saveCreds);
    inst.sock.ev.on('connection.update', u => {
      try{
        if (u.qr){ inst.qr = u.qr; inst.status = 'waiting'; }
        if (u.connection === 'open'){ inst.status = 'connected'; inst.qr = null; console.log('✅ WA'+inst.id+' SESIÓN ABIERTA'); }
        if (u.connection === 'close'){
          inst.status = 'disconnected';
          const code = new Boom(u.lastDisconnect?.error)?.output?.statusCode;
          if (code === DisconnectReason.loggedOut){ try{ fs.rmSync(inst.dir,{recursive:true,force:true}); }catch(e){} }
          setTimeout(()=>{ inst.connecting=false; connect(inst); }, 3000);
        }
      }catch(e){ console.error('[conn]', e.message); }
    });
    inst.sock.ev.on('messages.upsert', async ({ messages, type }) => {
      try{
        if (type !== 'notify') return;
        for (const m of messages){
          try{
            if (m.key.fromMe) continue;
            const payload = { from:'in', ts:Date.now() };
            const im=m.message?.imageMessage, au=m.message?.audioMessage, doc=m.message?.documentMessage;
            if (im||au||doc){
              try{
                const buf=await downloadMediaMessage(m);
                const mime=im?.mimetype||au?.mimetype||doc?.mimetype||'application/octet-stream';
                payload.type=im?'image':(au?'audio':'file');
                payload.fileName=doc?.fileName||(au?'audio.ogg':(im?'imagen.jpg':'archivo'));
                payload.size=buf?buf.length:0;
                payload.text=im?.caption||(doc?doc.fileName:'');
                let inlined=false;
                if (buf && im){
                  try{
                    const img=await Jimp.read(buf);
                    for (const [w,q] of [[1024,0.7],[800,0.55],[640,0.4]]){
                      img.resize(w,Jimp.AUTO);
                      const out=await img.getBufferAsync(Jimp.MIME_JPEG);
                      if (out.length<=650*1024){ payload.url='data:image/jpeg;base64,'+out.toString('base64'); inlined=true; break; }
                    }
                  }catch(e){ console.error('jimp:',e.message); }
                } else if (buf && buf.length<=650*1024){
                  payload.url='data:'+mime+';base64,'+buf.toString('base64'); inlined=true;
                }
                if (!inlined && buf){ payload._buf=buf; payload._mime=mime; }
              }catch(e){ payload.text='📎 Adjunto no descargable'; }
            } else { const text=m.message?.conversation||m.message?.extendedTextMessage?.text; if(!text) continue; payload.text=text; }
            let jidRaw = m.key.remoteJid||'';
            const wasLid = jidRaw.endsWith('@lid');
            const lid = wasLid ? jidRaw : '';
            if (lid || jidDigits(jidRaw).length>15){
              try{ const pn = await inst.sock.signalRepository.lidMapping.getPNForLID(jidRaw); if(pn) jidRaw = pn; }catch(e){}
            }
              await routeToCRM(inst, jidDigits(jidRaw), payload, m.pushName, lid, wasLid);
          }catch(e){ console.error('[msg]', e.message); }
        }
      }catch(e){ console.error('[upsert]', e.message); }
    });
  }catch(e){ console.error('[connect]', e.message); setTimeout(()=>{ inst.connecting=false; connect(inst); }, 5000); }
  inst.connecting = false;
}

let waSettingsCache=null, waSettingsAt=0;
async function getWaSettings(){ const now=Date.now(); if(waSettingsCache&&now-waSettingsAt<60000) return waSettingsCache; try{ const d=await db.collection('config').doc('waSettings').get(); waSettingsCache=d.exists?d.data():{}; }catch(e){ waSettingsCache={}; } waSettingsAt=now; return waSettingsCache; }

async function routeToCRM(inst, digits, payload, pushName, lid, wasLid){
  try{
    const snap=await db.collection('clients').get();
    let c=null;
    if (lid) c=snap.docs.find(d=>(d.data().lid||'')!=='' && (d.data().lid||'')===lid && (d.data().waInst||1)===Number(inst.id));
    if (!c && !wasLid && digits && digits.length>=10 && digits.length<=15){
      c=snap.docs.find(d=>(d.data().telefono||'').replace(/\D/g,'')===digits && (d.data().waInst||1)===Number(inst.id));
      if(!c) c=snap.docs.find(d=>{ const t=(d.data().telefono||'').replace(/\D/g,''); return t.length>=7 && (digits.endsWith(t)||t.endsWith(digits)); });
    }
    if (!c && pushName) c=snap.docs.find(d=>(d.data().nombre||'')===pushName && (d.data().waInst||1)===Number(inst.id));
    if (!c){
      const ws=await getWaSettings(); const set=(ws&&ws[inst.id])||{};
      const data={ nombre:pushName||('+'+(digits||'desconocido')), telefono:(!wasLid&&digits&&digits.length>=10&&digits.length<=15)?digits:'', pipeline:set.pipelineInbound||'Sin Contactos', stage:'Nuevo lead', origen:'WhatsApp', waInst:Number(inst.id), unread:1, lastMsgTs:payload.ts||Date.now(), createdAt:Date.now() };
      if (lid) data.lid=lid;
      const ref=await db.collection('clients').add(data);
      c={ id:ref.id };
    }
    if (payload._buf){
      try{ payload.att=await storeMedia(c.id,payload._buf,payload._mime||'application/octet-stream',payload.fileName||'archivo'); }
      catch(e){ console.error('att:',e.message); payload.text=payload.text||'📎 Adjunto no almacenado'; }
      delete payload._buf; delete payload._mime;
    }
    await db.collection('clients').doc(c.id).collection('whatsapp').add(payload);
    await db.collection('clients').doc(c.id).update({ unread: admin.firestore.FieldValue.increment(1), lastMsgTs: payload.ts||Date.now() });
    console.log('📥 Mensaje ruteado WA'+inst.id+' →', c.id);
  }catch(e){ console.error('route:', e.message); }
}

const app=express();
app.use(cors());
app.use(express.json({ limit:'2mb' }));
const auth=(req,res,next)=> req.headers['x-token']===TOKEN ? next() : res.status(401).json({ok:false,error:'No autorizado'});
const waOf=req=>{ const v=parseInt(req.query.wa||(req.body&&req.body.wa)||'1',10); return INST[v]||INST[1]; };

app.get('/', (req,res)=> res.send('CRM Nexus WhatsApp Bridge v9 ✅'));
app.get('/health', (req,res)=> res.json({ ok:true, wa1:INST[1].status, wa2:INST[2].status }));
app.get('/qr', auth, (req,res)=>{ const inst=waOf(req); res.json({ ok:true, status:inst.status, qr:inst.qr }); });
app.get('/chats', auth, (req,res)=>{
  const inst=waOf(req);
  if (inst.status!=='connected') return res.json({ ok:false, error:'No conectado' });
  const seen={}, list=[];
  for (const c of Object.values(inst.sock.chats||{})){ const d=jidDigits(c.id); if(!d||d.length<10||d.length>15||seen[d]) continue; seen[d]=1; list.push({jid:d,name:c.name||('+'+d)}); }
  res.json({ ok:true, chats:list });
});
app.post('/send', auth, async (req,res)=>{
  try{
    const inst=waOf(req); const { to, text }=req.body;
    if (inst.status!=='connected') return res.json({ ok:false, error:'WhatsApp '+inst.id+' no conectado. Escanea su QR.' });
    await inst.sock.sendMessage(String(to).replace(/\D/g,'')+'@s.whatsapp.net', { text });
    res.json({ ok:true });
  }catch(e){ res.status(500).json({ ok:false, error:String(e.message||e) }); }
});

app.listen(PORT, ()=>{ console.log('Bridge listo en puerto', PORT); connect(INST[1]); connect(INST[2]); });
setInterval(async ()=>{ for(const inst of [INST[1],INST[2]]){ if(inst.status==='connected'){ try{ await inst.sock.sendPresenceUpdate('available'); }catch(e){ console.log('♻️ reconectando WA'+inst.id); inst.status='disconnected'; inst.connecting=false; connect(inst); } } } }, 120000);
