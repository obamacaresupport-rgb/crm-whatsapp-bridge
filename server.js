import express from 'express';
import cors from 'cors';
import { makeWASocket, useMultiFileAuthState, DisconnectReason, fetchLatestBaileysVersion, downloadMediaMessage } from '@whiskeysockets/baileys';
import { Boom } from '@hapi/boom';
import fs from 'fs';
import Jimp from 'jimp';
import admin from 'firebase-admin';

process.on('uncaughtException', e => console.error('[KEEP-ALIVE] uncaughtException:', e.message));
process.on('unhandledRejection', e => console.error('[KEEP-ALIVE] unhandledRejection:', String((e && e.message) || e)));

const PORT = process.env.PORT || 10000;
const TOKEN = process.env.BRIDGE_TOKEN || 'CNX-BRIDGE-2026';
const jidDigits = j => (j||'').split('@')[0].split(':')[0].replace(/\D/g,'');

admin.initializeApp({ credential: admin.credential.cert(JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)) });
const db = admin.firestore();

let sock = null, qrData = null, status = 'disconnected', waUser = null, connecting = false;

async function connect(){
  if (connecting) return; connecting = true;
  try{
    const { state, saveCreds } = await useMultiFileAuthState('./session');
    const ver = await fetchLatestBaileysVersion().catch(()=>({ version: undefined }));
    sock = makeWASocket({ version: ver.version, auth: state, printQRInTerminal: false, browser: ['CRM Nexus','Chrome','1.0'], syncFullHistory: false, generateHighQualityLinkPreviews: false });
    sock.ev.on('creds.update', saveCreds);
    sock.ev.on('connection.update', u => {
      try{
        if (u.qr){ qrData = u.qr; status = 'waiting'; }
        if (u.connection === 'open'){ status = 'connected'; qrData = null; waUser = sock.user?.id || 'ok'; console.log('✅ SESIÓN WHATSAPP ABIERTA'); }
        if (u.connection === 'close'){
          status = 'disconnected';
          const code = new Boom(u.lastDisconnect?.error)?.output?.statusCode;
          if (code === DisconnectReason.loggedOut){ try{ fs.rmSync('./session',{recursive:true,force:true}); }catch(e){} }
          setTimeout(()=>{ connecting = false; connect(); }, 3000);
        }
      }catch(e){ console.error('[conn.update]', e.message); }
    });
    sock.ev.on('messages.upsert', async ({ messages, type }) => {
      try{
        if (type !== 'notify') return;
        for (const m of messages){
          try{
            if (m.key.fromMe) continue;
            const payload = { from:'in', ts: Date.now() };
            const im = m.message?.imageMessage, au = m.message?.audioMessage, doc = m.message?.documentMessage;
            if (im || au || doc){
              try{
                const buf = await downloadMediaMessage(m);
                                let data = null; const mime = im?.mimetype || au?.mimetype || doc?.mimetype || 'application/octet-stream';
                if (buf && buf.length <= 700*1024) data = 'data:'+mime+';base64,'+buf.toString('base64');
                else if (buf && im){
                  try{
                    const img = await Jimp.read(buf);
                    img.resize(1280, Jimp.AUTO); img.quality(72);
                    const out = await img.getBufferAsync(Jimp.MIME_JPEG);
                    if (out.length <= 900*1024) data = 'data:image/jpeg;base64,'+out.toString('base64');
                  }
                  catch(e){ console.error('jimp:', e.message); }
                }
                if (data){
                  payload.type = im ? 'image' : (au ? 'audio' : 'file');
                  payload.url = data;
                  payload.fileName = doc?.fileName || (au ? 'audio.ogg' : 'imagen.jpg');
                  payload.size = buf.length;
                  payload.text = im?.caption || '';
                } else payload.text = im ? '🖼️ Imagen demasiado pesada incluso comprimida' : (au ? '🎤 Audio recibido (pesado)' : '📄 Archivo recibido (pesado)');
              }catch(e){ payload.text = '📎 Adjunto no descargable'; }
            } else {
              const text = m.message?.conversation || m.message?.extendedTextMessage?.text;
              if (!text) continue;
              payload.text = text;
            }
            await routeToCRM(jidDigits(m.key.remoteJid), payload, m.pushName);
          }catch(e){ console.error('[msg]', e.message); }
        }
      }catch(e){ console.error('[upsert]', e.message); }
    });
  }catch(e){
    console.error('[connect]', e.message);
    setTimeout(()=>{ connecting = false; connect(); }, 5000);
  }
  connecting = false;
}

async function routeToCRM(digits, payload, pushName){
  try{
    if (!digits || digits.length < 7 || digits.length > 15) return;
    const snap = await db.collection('clients').get();
    let c = snap.docs.find(d => (d.data().telefono||'').replace(/\D/g,'') === digits);
    if (!c) c = snap.docs.find(d => { const t=(d.data().telefono||'').replace(/\D/g,''); return t.length>=7 && (digits.endsWith(t) || t.endsWith(digits)); });
    let cid;
    if (c) cid = c.id;
    else {
      const ref = await db.collection('clients').add({ nombre: pushName || ('+'+digits), telefono: digits, pipeline: 'Sin Contactos', stage: 'Nuevo lead', origen: 'WhatsApp', createdAt: Date.now() });
      cid = ref.id; console.log('🆕 Cliente creado desde WhatsApp:', cid);
    }
    await db.collection('clients').doc(cid).collection('whatsapp').add(payload);
    console.log('📥 Mensaje ruteado al cliente', cid);
  }catch(e){ console.error('route:', e.message); }
}

const app = express();
app.use(cors());
app.use(express.json({ limit:'2mb' }));
const auth = (req,res,next)=> req.headers['x-token']===TOKEN ? next() : res.status(401).json({ok:false,error:'No autorizado'});

app.get('/', (req,res)=> res.send('CRM Nexus WhatsApp Bridge ✅'));
app.get('/health', (req,res)=> res.json({ ok:true, status, waUser }));
app.get('/chats', auth, (req,res)=>{
  try{
    if (status!=='connected') return res.json({ ok:false, error:'No conectado' });
    const seen={}, list=[];
    for (const c of Object.values(sock.chats||{})){
      const d=jidDigits(c.id);       if(!d || d.length<10 || d.length>15 || seen[d]) continue; seen[d]=1;
      list.push({ jid:d, name:c.name||('+'+d) });
    }
    res.json({ ok:true, chats:list });
  }catch(e){ res.status(500).json({ ok:false, error:String(e.message||e) }); }
});
app.get('/qr', auth, (req,res)=> res.json({ ok:true, status, qr: qrData }));
app.post('/send', auth, async (req,res)=>{
  try{
    const { to, text } = req.body;
    if (status!=='connected') return res.json({ ok:false, error:'WhatsApp no conectado. Escanea el QR.' });
    await sock.sendMessage(String(to).replace(/\D/g,'') + '@s.whatsapp.net', { text });
    res.json({ ok:true });
  }catch(e){ res.status(500).json({ ok:false, error:String(e.message||e) }); }
});

app.listen(PORT, ()=>{ console.log('Bridge listo en puerto', PORT); connect(); });
