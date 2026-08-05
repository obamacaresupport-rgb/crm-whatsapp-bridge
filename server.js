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
                let data=null; const mime=im?.mimetype||au?.mimetype||doc?.mimetype||'application/octet-stream';
                if (buf && buf.length<=700*1024) data='data:'+mime+';base64,'+buf.toString('base64');
                else if (buf && im){ try{ const img=await Jimp.read(buf); img.resize(1280,Jimp.AUTO); img.quality(72); const out=await img.getBufferAsync(Jimp.MIME_JPEG); if(out.length<=900*1024) data='data:image/jpeg;base64,'+out.toString('base64'); }catch(e){} }
                if (data){ payload.type=im?'image':(au?'audio':'file'); payload.url=data; payload.fileName=doc?.fileName||(au?'audio.ogg':'imagen.jpg'); payload.size=buf.length; payload.text=im?.caption||''; }
                else payload.text=im?'🖼️ Imagen muy pesada':(au?'🎤 Audio recibido':'📄 Archivo recibido');
              }catch(e){ payload.text='📎 Adjunto no descargable'; }
            } else { const text=m.message?.conversation||m.message?.extendedTextMessage?.text; if(!text) continue; payload.text=text; }
            /* --- Resolver número real si WhatsApp mandó ID oculto (LID) --- */
            let jidRaw = m.key.remoteJid||'';
            if (jidRaw.endsWith('@lid') || jidDigits(jidRaw).length>15){
              try{ const pn = await inst.sock.signalRepository.lidMapping.getPNForLID(jidRaw); if(pn) jidRaw = pn; }catch(e){}
            }
            await routeToCRM(inst, jidDigits(jidRaw), payload, m.pushName);
          }catch(e){ console.error('[msg]', e.message); }
        }
      }catch(e){ console.error('[upsert]', e.message); }
    });
  }catch(e){ console.error('[connect]', e.message); setTimeout(()=>{ inst.connecting=false; connect(inst); }, 5000); }
  inst.connecting = false;
}

let waSettingsCache=null, waSettingsAt=0;
async function getWaSettings(){ const now=Date.now(); if(waSettingsCache&&now-waSettingsAt<60000) return waSettingsCache; try{ const d=await db.collection('config').doc('waSettings').get(); waSettingsCache=d.exists?d.data():{}; }catch(e){ waSettingsCache={}; } waSettingsAt=now; return waSettingsCache; }

async function routeToCRM(inst, digits, payload, pushName){
  try{
    const snap=await db.collection('clients').get();
    let c=null;
    if (digits && digits.length>=10 && digits.length<=15){
      c=snap.docs.find(d=>(d.data().telefono||'').replace(/\D/g,'')===digits && (d.data().waInst||1)===Number(inst.id));
      if(!c) c=snap.docs.find(d=>{ const t=(d.data().telefono||'').replace(/\D/g,''); return t.length>=7 && (digits.endsWith(t)||t.endsWith(digits)); });
    }
    if(!c && pushName) c=snap.docs.find(d=>(d.data().nombre||'')===pushName && (d.data().waInst||1)===Number(inst.id));
    if(!c){
      if(!digits || digits.length<10 || digits.length>15) return;
      const ws=await getWaSettings(); const set=(ws&&ws[inst.id])||{};
      const ref=await db.collection('clients').add({ nombre:pushName||('+'+digits), telefono:digits, pipeline:set.pipelineInbound||'Sin Contactos', stage:'Nuevo lead', origen:'WhatsApp', waInst:Number(inst.id), createdAt:Date.now() });
      console.log('🆕 Cliente WA'+inst.id+':', ref.id);
      c={ id: ref.id };
    }
    await db.collection('clients').doc(c.id).collection('whatsapp').add(payload);
  }catch(e){ console.error('route:', e.message); }
}

const app=express();
app.use(cors());
app.use(express.json({ limit:'2mb' }));
const auth=(req,res,next)=> req.headers['x-token']===TOKEN ? next() : res.status(401).json({ok:false,error:'No autorizado'});
const waOf=req=>{ const v=parseInt(req.query.wa||(req.body&&req.body.wa)||'1',10); return INST[v]||INST[1]; };

app.get('/', (req,res)=> res.send('CRM Nexus WhatsApp Bridge (2 líneas) ✅'));
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
