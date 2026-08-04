import express from 'express';
import cors from 'cors';
import { makeWASocket, useMultiFileAuthState, DisconnectReason, fetchLatestBaileysVersion } from '@whiskeysockets/baileys';
import { Boom } from '@hapi/boom';
import fs from 'fs';
import admin from 'firebase-admin';

const PORT = process.env.PORT || 10000;
const TOKEN = process.env.BRIDGE_TOKEN || 'CNX-BRIDGE-2026';

admin.initializeApp({ credential: admin.credential.cert(JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)) });
const db = admin.firestore();

let sock = null, qrData = null, status = 'disconnected', waUser = null;

async function connect(){
  const { state, saveCreds } = await useMultiFileAuthState('./session');
  const ver = await fetchLatestBaileysVersion().catch(()=>({ version: undefined }));
  sock = makeWASocket({ version: ver.version, auth: state, printQRInTerminal: false, browser: ['CRM Nexus', 'Chrome', '1.0'] });
  sock.ev.on('creds.update', saveCreds);
  sock.ev.on('connection.update', u => {
    if (u.qr){ qrData = u.qr; status = 'waiting'; }
    if (u.connection === 'open'){ status = 'connected'; qrData = null; waUser = sock.user?.id || 'ok'; }
    if (u.connection === 'close'){
      const code = new Boom(u.lastDisconnect?.error)?.output?.statusCode;
      status = 'disconnected';
      if (code === DisconnectReason.loggedOut){ try{ fs.rmSync('./session',{recursive:true,force:true}); }catch(e){} }
      setTimeout(connect, 3000);
    }
  });
  sock.ev.on('messages.upsert', async ({ messages, type }) => {
    if (type !== 'notify') return;
    for (const m of messages){
      if (m.key.fromMe) continue;
      const text = m.message?.conversation || m.message?.extendedTextMessage?.text || (m.message?.imageMessage ? '🖼️ Imagen recibida' : (m.message?.audioMessage ? '🎤 Audio recibido' : ''));
      if (!text) continue;
      await routeToCRM((m.key.remoteJid||'').split('@')[0], text);
    }
  });
}

async function routeToCRM(phone, text){
  try{
    const digits = phone.replace(/\D/g,'');
    const snap = await db.collection('clients').get();
    let c = snap.docs.find(d => (d.data().telefono||'').replace(/\D/g,'') === digits);
    if (!c) c = snap.docs.find(d => { const t=(d.data().telefono||'').replace(/\D/g,''); return t.length>=7 && (digits.endsWith(t) || t.endsWith(digits)); });
    if (!c) return;
    await db.collection('clients').doc(c.id).collection('whatsapp').add({ from:'in', text, ts: Date.now() });
  }catch(e){ console.error('route:', e.message); }
}

const app = express();
app.use(cors());
app.use(express.json({ limit:'2mb' }));
const auth = (req,res,next)=> req.headers['x-token']===TOKEN ? next() : res.status(401).json({ok:false,error:'No autorizado'});

app.get('/', (req,res)=> res.send('CRM Nexus WhatsApp Bridge ✅'));
app.get('/health', (req,res)=> res.json({ ok:true, status, waUser }));
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
