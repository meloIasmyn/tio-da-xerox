// Tio da Xerox — cota coletiva de impressão da turma
// Demonstra: (1) Exclusão mútua  (2) Comunicação em tempo real  (3) Consistência de dados
const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');

const PORT = process.env.PORT || 3000;
const COTA = 200;             // páginas da turma no mês
const RESERVA_MS = 30_000;    // reserva expira em 30 s se não confirmar
const LATENCIA_BANCO = [150, 400]; // atraso simulado de leitura/escrita no "banco"

const app = express();
app.use(express.static(path.join(__dirname, 'public')));
app.get('/tio', (_req, res) => res.sendFile(path.join(__dirname, 'public', 'tio.html')));
const server = http.createServer(app);
const io = new Server(server);

// ---------------- Estado central (fonte da verdade) ----------------
const estado = {
  saldo: COTA,
  versao: 0,          // nº de versão: cada mudança incrementa (clientes descartam versões antigas)
  lamport: 0,         // relógio lógico do servidor
  usarLock: true,     // liga/desliga exclusão mútua para demonstrar a condição de corrida
  reservas: [],       // {id, nome, paginas, L, expira}
  fila: [],           // pedidos confirmados aguardando a copiadora {id, nome, paginas, L}
  impressos: [],      // já impressos
  log: [],
};
let seq = 0;
const dispositivos = new Map(); // deviceId -> nome (identidade fixa do celular)

function identificar(deviceId, nome) {
  deviceId = String(deviceId || '').slice(0, 40);
  if (!deviceId) return null;
  if (!dispositivos.has(deviceId) && nome) dispositivos.set(deviceId, String(nome).trim().slice(0, 20));
  return dispositivos.get(deviceId) || null;
}

// ---------------- Mutex (exclusão mútua) ----------------
class Mutex {
  constructor() { this.cauda = Promise.resolve(); }
  lock() {
    let liberar;
    const proximo = new Promise(r => (liberar = r));
    const anterior = this.cauda;
    this.cauda = anterior.then(() => proximo);
    return anterior.then(() => liberar);
  }
}
const mutex = new Mutex();
const dormir = ms => new Promise(r => setTimeout(r, ms));
const latencia = () => dormir(LATENCIA_BANCO[0] + Math.random() * (LATENCIA_BANCO[1] - LATENCIA_BANCO[0]));

async function secaoCritica(fn) {
  if (!estado.usarLock) return fn();       // SEM lock: todos entram juntos
  const liberar = await mutex.lock();      // COM lock: um por vez
  try { return await fn(); } finally { liberar(); }
}

// ---------------- Relógio de Lamport ----------------
function tick(Lrecebido = 0) {
  estado.lamport = Math.max(estado.lamport, Number(Lrecebido) || 0) + 1;
  return estado.lamport;
}

// ---------------- Consistência: verificação de integridade ----------------
function distribuido() {
  const soma = arr => arr.reduce((s, x) => s + x.paginas, 0);
  return soma(estado.reservas) + soma(estado.fila) + soma(estado.impressos);
}

function registrar(texto, tipo = 'info') {
  estado.log.unshift({ texto, tipo, L: estado.lamport, hora: Date.now() });
  estado.log = estado.log.slice(0, 40);
}

function publicar() {
  estado.versao++;
  const dist = distribuido();
  io.emit('estado', {
    versao: estado.versao,
    lamport: estado.lamport,
    cota: COTA,
    saldo: estado.saldo,
    distribuido: dist,
    integridade: dist + estado.saldo === COTA && estado.saldo >= 0,
    excesso: Math.max(0, dist - COTA),
    usarLock: estado.usarLock,
    reservas: estado.reservas,
    fila: [...estado.fila].sort((a, b) => a.L - b.L || a.nome.localeCompare(b.nome)),
    impressos: estado.impressos.slice(-10).reverse(),
    log: estado.log,
    online: io.engine.clientsCount,
    reservaMs: RESERVA_MS,
  });
}

// ---------------- Operações ----------------
async function reservar({ deviceId, nome, paginas, L }) {
  paginas = Math.floor(Number(paginas));
  nome = identificar(deviceId, nome);
  if (!nome) return { ok: false, msg: 'Dispositivo não identificado. Recarregue a página.' };
  if (!(paginas >= 1 && paginas <= 50)) return { ok: false, msg: 'Peça entre 1 e 50 páginas.' };
  const Lpedido = tick(L);
  return secaoCritica(async () => {
    if (estado.reservas.some(r => r.dono === deviceId)) return { ok: false, msg: 'Você já tem uma reserva aberta. Confirme ou cancele antes.' };
    const lido = estado.saldo;            // 1. LÊ o saldo
    await latencia();                     // 2. "banco" demora...
    if (lido < paginas) {
      registrar(`${nome} pediu ${paginas} pág — RECUSADO (saldo ${lido})`, 'erro');
      publicar();
      return { ok: false, msg: `Saldo insuficiente (restam ${lido}).`, L: tick() };
    }
    estado.saldo = lido - paginas;        // 3. ESCREVE o novo saldo
    const r = { id: ++seq, dono: deviceId, nome, paginas, L: Lpedido, expira: Date.now() + RESERVA_MS };
    estado.reservas.push(r);
    registrar(`${nome} reservou ${paginas} pág`, 'ok');
    publicar();
    return { ok: true, reserva: r, L: tick() };
  });
}

async function confirmar({ id, deviceId, L }) {
  tick(L);
  return secaoCritica(async () => {
    const i = estado.reservas.findIndex(r => r.id === id && r.dono === deviceId);
    if (i < 0) return { ok: false, msg: 'Reserva expirou ou não existe.' };
    const [r] = estado.reservas.splice(i, 1);
    estado.fila.push({ id: r.id, dono: r.dono, nome: r.nome, paginas: r.paginas, L: r.L });
    registrar(`${r.nome} confirmou ${r.paginas} pág → fila da copiadora`, 'ok');
    publicar();
    return { ok: true };
  });
}

async function devolver(id, motivo, deviceId = null) {
  return secaoCritica(async () => {
    const i = estado.reservas.findIndex(r => r.id === id && (!deviceId || r.dono === deviceId));
    if (i < 0) return { ok: false };
    const [r] = estado.reservas.splice(i, 1);
    estado.saldo += r.paginas;
    tick();
    registrar(`${r.nome}: reserva de ${r.paginas} pág ${motivo} — páginas voltaram`, 'aviso');
    publicar();
    return { ok: true };
  });
}

// Reservas que expiram (lock com timeout)
setInterval(() => {
  const agora = Date.now();
  estado.reservas.filter(r => r.expira <= agora).forEach(r => devolver(r.id, 'EXPIROU'));
}, 1000);

// ---------------- Comunicação em tempo real (WebSocket) ----------------
io.on('connection', socket => {
  publicar(); // novo dispositivo recebe o estado completo (e todos veem o nº online)
  socket.on('reservar', async (dados, cb) => cb?.(await reservar(dados)));
  socket.on('confirmar', async (dados, cb) => cb?.(await confirmar(dados)));
  socket.on('cancelar', async ({ id, deviceId }, cb) => cb?.(await devolver(id, 'CANCELADA', deviceId)));
  // celular se apresenta com seu ID fixo e recebe o nome já registrado (se houver)
  socket.on('identificar', ({ deviceId, nome }, cb) => cb?.({ nome: identificar(deviceId, nome) }));

  // --- comandos do Tio (tela da copiadora) ---
  socket.on('tio:imprimir', () => {
    const fila = [...estado.fila].sort((a, b) => a.L - b.L || a.nome.localeCompare(b.nome));
    const prox = fila[0];
    if (!prox) return;
    estado.fila = estado.fila.filter(f => f.id !== prox.id);
    estado.impressos.push(prox);
    tick();
    registrar(`🖨️ Tio imprimiu ${prox.paginas} pág de ${prox.nome}`, 'ok');
    publicar();
  });
  socket.on('tio:lock', valor => {
    estado.usarLock = !!valor;
    tick();
    registrar(`Exclusão mútua ${estado.usarLock ? 'LIGADA 🔒' : 'DESLIGADA 🔓'}`, 'aviso');
    publicar();
  });
  socket.on('tio:reset', () => {
    Object.assign(estado, { saldo: COTA, reservas: [], fila: [], impressos: [], log: [] });
    [...dispositivos.keys()].filter(k => k.startsWith('robo-')).forEach(k => dispositivos.delete(k));
    tick();
    registrar('Cota zerada: 200 páginas disponíveis', 'aviso');
    publicar();
  });
  socket.on('tio:estresse', () => {
    // 15 "alunos robôs" pedem 20 páginas no MESMO instante
    for (let i = 1; i <= 15; i++) reservar({ deviceId: `robo-${i}`, nome: `Robô ${i}`, paginas: 20, L: 0 });
  });
  socket.on('disconnect', () => setTimeout(publicar, 100));
});

server.listen(PORT, () => {
  console.log(`Tio da Xerox rodando em http://localhost:${PORT}  (tela do tio: /tio)`);
});
