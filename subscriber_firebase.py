"""
Puente MQTT → Firestore
=======================
Se suscribe a los topics del simulador y persiste todo en Firestore para que
el dashboard lo lea en tiempo real.

  ✓ Escritura asíncrona en batches (el hilo MQTT nunca se bloquea con Firestore)
  ✓ Reintentos con backoff si Firestore falla
  ✓ Latencia por etapa: cola del dispositivo, red MQTT y escritura en Firestore
  ✓ Watchdog: marca dispositivos OFFLINE si dejan de reportar y registra eventos
  ✓ Detección de mensajes perdidos / duplicados por número de secuencia
  ✓ Heartbeat propio con RTT al broker (sistema/puente)

Instalación:
    pip install paho-mqtt python-dotenv firebase-admin

Uso:
    python subscriber_firebase.py      (requiere firebase-key.json)

Colecciones que escribe:
    telemetria            lecturas + recibido_ts, servidor_ts, latencia_ms
    alertas               alertas publicadas por el simulador
    eventos               DISPOSITIVO_OFFLINE / DISPOSITIVO_ONLINE / SIMULADOR_*
    dispositivos/{id}     registro por dispositivo (último dato, estado)
    sistema/puente        heartbeat del puente, RTT al broker, contadores
    sistema/simulador     último status y métricas del simulador
"""

import json
import logging
import os
import queue
import random
import threading
import time
from collections import deque
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:  # python-dotenv es opcional para el puente
    pass

VERSION = "2.1"

log = logging.getLogger("puente")


# ══════════════════════════════════════════════════════════════
# CONFIGURACIÓN — mismo .env que el simulador
# ══════════════════════════════════════════════════════════════
class Config:
    def __init__(self):
        self.host = os.getenv("MQTT_HOST", "aec31a90bbda48f3b180689b08b9e33b.s1.eu.hivemq.cloud")
        self.port = int(os.getenv("MQTT_PORT", "8883"))
        self.user = os.getenv("MQTT_USER", "pablo")
        self.password = os.getenv("MQTT_PASSWORD", "Test1234")
        self.tls = os.getenv("MQTT_TLS", "True").lower() == "true"
        self.ca_cert = os.getenv("MQTT_CA_CERT", "")

        self.firebase_key = os.getenv("FIREBASE_KEY", "firebase-key.json")

        self.offline_factor = float(os.getenv("OFFLINE_FACTOR", "3"))
        self.offline_min_s = float(os.getenv("OFFLINE_MIN_S", "15"))
        self.intervalo_default_s = float(os.getenv("INTERVALO_SEGUNDOS", "5"))
        self.registro_throttle_s = float(os.getenv("REGISTRO_THROTTLE_S", "30"))
        self.heartbeat_s = float(os.getenv("HEARTBEAT_PUENTE_S", "15"))
        self.watchdog_s = 2.0

        self.cola_max = int(os.getenv("COLA_ESCRITURA_MAX", "5000"))
        self.batch_max = 100  # Firestore permite hasta 500 operaciones por batch

        self.log_level = os.getenv("LOG_LEVEL", "INFO")


# ══════════════════════════════════════════════════════════════
# HELPERS DE TIEMPO
# ══════════════════════════════════════════════════════════════
def parse_iso_ms(valor):
    """ISO-8601 → epoch en milisegundos (None si no se puede parsear).
    Sin zona horaria se asume UTC, que es lo que publica el simulador."""
    if not isinstance(valor, str) or not valor:
        return None
    try:
        dt = datetime.fromisoformat(valor.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp() * 1000.0


def ms_to_iso(ms):
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat()


def calcular_latencias(payload, recibido_ms):
    """Latencias (ms) que se pueden medir al llegar el mensaje al puente.

    cola          timestamp → enviado_ts   tiempo que la medición esperó en el
                                           dispositivo (alto si pasó por la cola offline)
    red           enviado_ts → recibido    dispositivo → broker → puente
    hasta_puente  timestamp → recibido     total desde que se midió

    La etapa puente → Firestore → dashboard la completa el dashboard usando
    servidor_ts y la hora de llegada al navegador.
    """
    generado = parse_iso_ms(payload.get("timestamp"))
    enviado = parse_iso_ms(payload.get("enviado_ts"))
    lat = {}
    if generado is not None and enviado is not None:
        lat["cola"] = round(enviado - generado, 1)
    if enviado is not None:
        lat["red"] = round(recibido_ms - enviado, 1)
    if generado is not None:
        lat["hasta_puente"] = round(recibido_ms - generado, 1)
    return lat


def crear_cliente_mqtt(client_id):
    """Compatible con paho-mqtt 1.x y 2.x (callbacks con la firma de 1.x)."""
    if hasattr(mqtt, "CallbackAPIVersion"):
        return mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id=client_id, protocol=mqtt.MQTTv311)
    return mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)


def promedio(valores):
    valores = list(valores)
    return round(sum(valores) / len(valores), 1) if valores else None


# ══════════════════════════════════════════════════════════════
# MONITOR DE DISPOSITIVOS — estado online/offline y secuencia
# ══════════════════════════════════════════════════════════════
class MonitorDispositivos:
    """Lleva, por dispositivo, cuándo se lo vio por última vez (reloj del
    puente, así no dependemos del reloj del dispositivo) y el último número
    de secuencia. No toca Firestore: devuelve qué pasó y el puente decide qué
    escribir. El reloj es inyectable para poder testearlo."""

    SALTO_REINICIO = 50  # un seq que retrocede más que esto = el dispositivo se reinició

    def __init__(self, offline_factor=3.0, offline_min_s=15.0, intervalo_default_s=5.0, reloj=time.time):
        self.offline_factor = offline_factor
        self.offline_min_s = offline_min_s
        self.intervalo_default_s = intervalo_default_s
        self._reloj = reloj
        self._dev = {}
        self._lock = threading.Lock()

    def umbral_s(self, intervalo_s=None):
        intervalo = intervalo_s or self.intervalo_default_s
        return max(self.offline_factor * intervalo, self.offline_min_s)

    def _nuevo(self, casa_id):
        return {
            "casa_id": casa_id,
            "nombre": None,
            "ultimo_visto": None,
            "intervalo_s": None,
            "estado": "desconocido",
            "offline_desde": None,
            "ultimo_seq": None,
            "perdidos": 0,
            "lecturas": 0,
        }

    def cargar_registro(self, casa_id, doc):
        """Siembra el estado con lo guardado en dispositivos/{id} al arrancar,
        para que el watchdog detecte también los que nunca vuelven a reportar."""
        with self._lock:
            d = self._dev.setdefault(casa_id, self._nuevo(casa_id))
            d["nombre"] = doc.get("nombre")
            d["intervalo_s"] = doc.get("intervalo_s")
            visto = parse_iso_ms(doc.get("ultimo_recibido_ts"))
            d["ultimo_visto"] = visto / 1000.0 if visto is not None else None
            d["estado"] = doc.get("estado") or "desconocido"
            d["perdidos"] = int(doc.get("mensajes_perdidos") or 0)

    def registrar_lectura(self, casa_id, nombre=None, seq=None, intervalo_s=None):
        ahora = self._reloj()
        info = {
            "nuevo": False,
            "volvio_online": False,
            "duracion_offline_s": None,
            "perdidos": 0,
            "duplicado": False,
            "reinicio": False,
            "fuera_de_orden": False,
        }
        with self._lock:
            d = self._dev.get(casa_id)
            if d is None:
                d = self._dev[casa_id] = self._nuevo(casa_id)
                info["nuevo"] = True

            # ── Secuencia ─────────────────────────────────────
            if isinstance(seq, int):
                ultimo = d["ultimo_seq"]
                if ultimo is None:
                    d["ultimo_seq"] = seq
                elif seq == ultimo:
                    info["duplicado"] = True  # QoS 1 puede entregar duplicados
                    return info
                elif seq > ultimo:
                    if seq > ultimo + 1:
                        info["perdidos"] = seq - ultimo - 1
                        d["perdidos"] += info["perdidos"]
                    d["ultimo_seq"] = seq
                elif seq <= 1 or ultimo - seq > self.SALTO_REINICIO:
                    info["reinicio"] = True
                    d["ultimo_seq"] = seq
                else:
                    # Llegó tarde uno que ya habíamos contado como perdido
                    info["fuera_de_orden"] = True
                    d["perdidos"] = max(0, d["perdidos"] - 1)

            # ── Estado ────────────────────────────────────────
            if d["estado"] == "offline":
                info["volvio_online"] = True
                if d["offline_desde"] is not None:
                    info["duracion_offline_s"] = round(ahora - d["offline_desde"], 1)
                elif d["ultimo_visto"] is not None:
                    info["duracion_offline_s"] = round(ahora - d["ultimo_visto"], 1)

            d["estado"] = "online"
            d["offline_desde"] = None
            d["ultimo_visto"] = ahora
            d["lecturas"] += 1
            if nombre:
                d["nombre"] = nombre
            if intervalo_s:
                d["intervalo_s"] = intervalo_s
        return info

    def revisar(self):
        """Devuelve los dispositivos que acaban de pasar a OFFLINE."""
        ahora = self._reloj()
        caidos = []
        with self._lock:
            for d in self._dev.values():
                if d["estado"] == "offline" or d["ultimo_visto"] is None:
                    continue
                silencio = ahora - d["ultimo_visto"]
                umbral = self.umbral_s(d["intervalo_s"])
                if silencio > umbral:
                    d["estado"] = "offline"
                    d["offline_desde"] = d["ultimo_visto"]
                    caidos.append({
                        "casa_id": d["casa_id"],
                        "nombre": d["nombre"],
                        "ultimo_visto": d["ultimo_visto"],
                        "silencio_s": round(silencio, 1),
                        "umbral_s": umbral,
                    })
        return caidos

    def estado(self, casa_id):
        with self._lock:
            d = self._dev.get(casa_id)
            return dict(d) if d else None

    def resumen(self):
        with self._lock:
            online = sum(1 for d in self._dev.values() if d["estado"] == "online")
            offline = sum(1 for d in self._dev.values() if d["estado"] == "offline")
            perdidos = sum(d["perdidos"] for d in self._dev.values())
            return {"total": len(self._dev), "online": online, "offline": offline, "perdidos_seq": perdidos}


# ══════════════════════════════════════════════════════════════
# ESCRITOR FIRESTORE — cola + batches + reintentos
# ══════════════════════════════════════════════════════════════
class EscritorFirestore(threading.Thread):
    MAX_INTENTOS = 5
    MAX_BACKOFF_S = 30

    def __init__(self, db, cola_max=5000, batch_max=100):
        super().__init__(name="Firestore", daemon=True)
        self.db = db
        self.batch_max = batch_max
        self._cola = queue.Queue(maxsize=cola_max)
        self._parar = threading.Event()
        self._lock = threading.Lock()
        self.escritos = 0
        self.descartados = 0
        self.errores = 0
        self._tiempos_ms = deque(maxlen=50)
        self._ultimo_aviso_llena = 0.0

    # ── API ────────────────────────────────────────────────
    def agregar(self, coleccion, data):
        self._encolar(("add", coleccion, data, False))

    def guardar(self, ruta, data, merge=True):
        self._encolar(("set", ruta, data, merge))

    def pendientes(self):
        return self._cola.qsize()

    def commit_prom_ms(self):
        with self._lock:
            return promedio(self._tiempos_ms)

    def detener(self, timeout=10):
        self._parar.set()
        self.join(timeout)

    # ── Interno ────────────────────────────────────────────
    def _encolar(self, op):
        try:
            self._cola.put_nowait(op)
        except queue.Full:
            with self._lock:
                self.descartados += 1
                ahora = time.time()
                if ahora - self._ultimo_aviso_llena > 10:
                    self._ultimo_aviso_llena = ahora
                    log.error(f"Cola de escritura llena ({self._cola.maxsize}); descartando datos")

    def _tomar_lote(self):
        try:
            lote = [self._cola.get(timeout=1)]
        except queue.Empty:
            return []
        while len(lote) < self.batch_max:
            try:
                lote.append(self._cola.get_nowait())
            except queue.Empty:
                break
        return lote

    def _commit(self, lote):
        batch = self.db.batch()
        for tipo, destino, data, merge in lote:
            if tipo == "add":
                batch.set(self.db.collection(destino).document(), data)
            else:
                batch.set(self.db.document(destino), data, merge=merge)
        batch.commit()

    def escribir_lote(self, lote):
        """Escribe un lote con reintentos. Devuelve True si se guardó."""
        espera = 1
        for intento in range(1, self.MAX_INTENTOS + 1):
            t0 = time.perf_counter()
            try:
                self._commit(lote)
                with self._lock:
                    self._tiempos_ms.append((time.perf_counter() - t0) * 1000)
                    self.escritos += len(lote)
                return True
            except Exception as e:
                with self._lock:
                    self.errores += 1
                if intento == self.MAX_INTENTOS or self._parar.is_set():
                    log.error(f"Firestore: lote de {len(lote)} descartado tras {intento} intentos: {e}")
                    with self._lock:
                        self.descartados += len(lote)
                    return False
                log.warning(f"Firestore falló (intento {intento}): {e}. Reintento en {espera}s")
                self._parar.wait(espera)
                espera = min(espera * 2, self.MAX_BACKOFF_S)
        return False

    def run(self):
        while not (self._parar.is_set() and self._cola.empty()):
            lote = self._tomar_lote()
            if lote:
                self.escribir_lote(lote)


# ══════════════════════════════════════════════════════════════
# CONTADORES DEL PUENTE
# ══════════════════════════════════════════════════════════════
class Contadores:
    CAMPOS = ("recibidos", "telemetria", "alertas", "status", "invalidos", "duplicados", "errores")

    def __init__(self):
        self._lock = threading.Lock()
        self._v = {c: 0 for c in self.CAMPOS}

    def incr(self, campo, n=1):
        with self._lock:
            self._v[campo] += n

    def snapshot(self):
        with self._lock:
            return dict(self._v)


# ══════════════════════════════════════════════════════════════
# PUENTE
# ══════════════════════════════════════════════════════════════
class Puente:
    TOPIC_TELEMETRIA = "iot/casas/+/telemetria"
    TOPIC_ALERTAS = "iot/casas/+/alertas"
    TOPIC_STATUS = "iot/casas/status"
    TOPIC_METRICAS = "iot/casas/metricas"

    EVENTOS_SIMULADOR = ("SIMULADOR_INICIADO", "SIMULADOR_DETENIDO", "SIMULADOR_DESCONECTADO")

    def __init__(self, cfg, db, fs, reloj=time.time):
        """db: cliente Firestore. fs: módulo firestore (SERVER_TIMESTAMP, Increment)."""
        self.cfg = cfg
        self.db = db
        self.fs = fs
        self._reloj = reloj
        self.client_id = f"puente_fs_{random.randint(1000, 9999)}"
        self.topic_ping = f"iot/sistema/ping/{self.client_id}"

        self.monitor = MonitorDispositivos(cfg.offline_factor, cfg.offline_min_s, cfg.intervalo_default_s, reloj)
        self.escritor = EscritorFirestore(db, cfg.cola_max, cfg.batch_max)
        self.contadores = Contadores()

        self.conectado = threading.Event()
        self._parar = threading.Event()
        self._inicio_iso = ms_to_iso(reloj() * 1000)

        self._lock = threading.Lock()
        self._lat_puente = deque(maxlen=200)   # hasta_puente (ms) de las últimas lecturas
        self._rtt = deque(maxlen=20)           # RTT al broker (ms)
        self._ping_seq = 0
        self._registro_ultimo = {}             # casa_id → última escritura en dispositivos/
        self._registro_pendientes = {}         # casa_id → lecturas sin contabilizar en el registro

        self._client = None

    # ── MQTT ───────────────────────────────────────────────
    def _construir_cliente(self):
        c = crear_cliente_mqtt(self.client_id)
        if self.cfg.user:
            c.username_pw_set(self.cfg.user, self.cfg.password)
        if self.cfg.tls:
            # Sin CA explícita paho usa los certificados del sistema (igual que antes)
            c.tls_set(ca_certs=self.cfg.ca_cert or None)
        c.reconnect_delay_set(min_delay=1, max_delay=60)
        c.on_connect = self._on_connect
        c.on_disconnect = self._on_disconnect
        c.on_message = self._on_message
        return c

    def _on_connect(self, client, userdata, flags, rc):
        if rc != 0:
            log.error(f"Broker rechazó la conexión (rc={rc})")
            return
        log.info(f"✓ MQTT conectado a {self.cfg.host}:{self.cfg.port}")
        client.subscribe([
            (self.TOPIC_TELEMETRIA, 1),
            (self.TOPIC_ALERTAS, 2),
            (self.TOPIC_STATUS, 1),
            (self.TOPIC_METRICAS, 0),
            (self.topic_ping, 1),
        ])
        self.conectado.set()

    def _on_disconnect(self, client, userdata, rc):
        self.conectado.clear()
        if rc != 0 and not self._parar.is_set():
            log.warning(f"MQTT desconectado inesperadamente (rc={rc}); paho reintenta solo")

    def _on_message(self, client, userdata, msg):
        recibido_ms = self._reloj() * 1000
        self.contadores.incr("recibidos")
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            self.contadores.incr("invalidos")
            log.warning(f"Payload inválido en {msg.topic}: {e}")
            return
        try:
            self.procesar(msg.topic, payload, recibido_ms, retained=bool(getattr(msg, "retain", False)))
        except Exception as e:
            self.contadores.incr("errores")
            log.error(f"Error procesando {msg.topic}: {e}", exc_info=True)

    # ── Procesamiento (independiente de MQTT, testeable) ───
    def procesar(self, topic, payload, recibido_ms, retained=False):
        if not isinstance(payload, dict):
            self.contadores.incr("invalidos")
            return
        if topic == self.topic_ping:
            self._procesar_ping(payload)
        elif topic.endswith("/telemetria"):
            self._procesar_telemetria(payload, recibido_ms)
        elif topic.endswith("/alertas"):
            self._procesar_alerta(payload, recibido_ms)
        elif topic == self.TOPIC_STATUS:
            self._procesar_status(payload, recibido_ms, retained)
        elif topic == self.TOPIC_METRICAS:
            self.escritor.guardar("sistema/simulador", {
                "metricas": payload,
                "metricas_recibido_ts": ms_to_iso(recibido_ms),
            })

    def _procesar_telemetria(self, payload, recibido_ms):
        casa_id = payload.get("casa_id")
        if not isinstance(casa_id, str) or not isinstance(payload.get("medicion"), dict):
            self.contadores.incr("invalidos")
            log.warning(f"Telemetría sin casa_id/medicion: {str(payload)[:120]}")
            return

        seq = payload.get("seq")
        info = self.monitor.registrar_lectura(
            casa_id,
            nombre=payload.get("nombre"),
            seq=seq if isinstance(seq, int) else None,
            intervalo_s=payload.get("intervalo_s"),
        )
        if info["duplicado"]:
            self.contadores.incr("duplicados")
            log.debug(f"[{casa_id}] duplicado seq={seq}, ignorado")
            return
        if info["perdidos"]:
            log.warning(f"[{casa_id}] {info['perdidos']} mensaje(s) perdido(s) antes de seq={seq}")
        if info["reinicio"]:
            log.info(f"[{casa_id}] secuencia reiniciada (seq={seq}); el dispositivo se reinició")

        self.contadores.incr("telemetria")
        latencias = calcular_latencias(payload, recibido_ms)
        if "hasta_puente" in latencias and not payload.get("reenviado_offline"):
            with self._lock:
                self._lat_puente.append(latencias["hasta_puente"])

        recibido_iso = ms_to_iso(recibido_ms)
        doc = {
            **payload,
            "recibido_ts": recibido_iso,
            "servidor_ts": self.fs.SERVER_TIMESTAMP,
            "latencia_ms": latencias,
        }
        if info["perdidos"]:
            doc["perdidos_previos"] = info["perdidos"]
        self.escritor.agregar("telemetria", doc)

        if info["volvio_online"]:
            dur = info["duracion_offline_s"]
            log.info(f"[{casa_id}] ✓ volvió a ONLINE" + (f" tras {dur:.0f}s" if dur else ""))
            self.escritor.agregar("eventos", {
                "casa_id": casa_id,
                "nombre": payload.get("nombre"),
                "tipo": "DISPOSITIVO_ONLINE",
                "severidad": "INFO",
                "descripcion": "El dispositivo volvió a reportar"
                               + (f" tras {dur:.0f}s sin datos" if dur else ""),
                "duracion_offline_s": dur,
                "timestamp": recibido_iso,
                "servidor_ts": self.fs.SERVER_TIMESTAMP,
            })

        self._actualizar_registro(casa_id, payload, recibido_ms, latencias, info)

    def _actualizar_registro(self, casa_id, payload, recibido_ms, latencias, info):
        """dispositivos/{casa_id}: se escribe como mucho cada REGISTRO_THROTTLE_S
        (salvo alta nueva o vuelta a online) para no gastar cuota de Firestore."""
        with self._lock:
            self._registro_pendientes[casa_id] = self._registro_pendientes.get(casa_id, 0) + 1
            ultimo = self._registro_ultimo.get(casa_id, 0)
            urgente = info["nuevo"] or info["volvio_online"] or info["perdidos"]
            if not urgente and (recibido_ms / 1000 - ultimo) < self.cfg.registro_throttle_s:
                return
            pendientes = self._registro_pendientes.pop(casa_id, 0)
            self._registro_ultimo[casa_id] = recibido_ms / 1000

        estado = self.monitor.estado(casa_id) or {}
        data = {
            "casa_id": casa_id,
            "nombre": payload.get("nombre"),
            "estado": "online",
            "offline_desde": None,
            "ultimo_ts": payload.get("timestamp"),
            "ultimo_recibido_ts": ms_to_iso(recibido_ms),
            "ultima_medicion": payload.get("medicion"),
            "evento_red": payload.get("evento_red"),
            "intervalo_s": payload.get("intervalo_s"),
            "seq": payload.get("seq"),
            "latencia_ms": latencias,
            "mensajes_perdidos": estado.get("perdidos", 0),
            "lecturas": self.fs.Increment(pendientes),
            "actualizado": self.fs.SERVER_TIMESTAMP,
        }
        self.escritor.guardar(f"dispositivos/{casa_id}", data)

    def _procesar_alerta(self, payload, recibido_ms):
        alerta = payload.get("alerta") or {}
        if not payload.get("casa_id") or not alerta.get("tipo"):
            self.contadores.incr("invalidos")
            return
        self.contadores.incr("alertas")
        self.escritor.agregar("alertas", {
            "casa_id": payload.get("casa_id"),
            "nombre": payload.get("nombre"),
            "timestamp": payload.get("timestamp"),
            "tipo": alerta.get("tipo"),
            "severidad": alerta.get("severidad"),
            "descripcion": alerta.get("descripcion"),
            "valor": alerta.get("valor"),
            "evento_red": alerta.get("evento_red"),
            "recibido_ts": ms_to_iso(recibido_ms),
            "servidor_ts": self.fs.SERVER_TIMESTAMP,
        })

    def _procesar_status(self, payload, recibido_ms, retained):
        self.contadores.incr("status")
        evento = payload.get("evento")
        recibido_iso = ms_to_iso(recibido_ms)
        self.escritor.guardar("sistema/simulador", {
            **payload,
            "estado": "offline" if evento in ("SIMULADOR_DETENIDO", "SIMULADOR_DESCONECTADO") else "online",
            "recibido_ts": recibido_iso,
            "actualizado": self.fs.SERVER_TIMESTAMP,
        })
        # Los mensajes retained se re-entregan en cada (re)conexión: solo
        # registramos como evento los que ocurren en vivo.
        if evento in self.EVENTOS_SIMULADOR and not retained:
            log.info(f"Simulador: {evento}")
            self.escritor.agregar("eventos", {
                "casa_id": None,
                "tipo": evento,
                "severidad": "ALTA" if evento == "SIMULADOR_DESCONECTADO" else "INFO",
                "descripcion": {
                    "SIMULADOR_INICIADO": "El simulador se inició",
                    "SIMULADOR_DETENIDO": "El simulador se detuvo correctamente",
                    "SIMULADOR_DESCONECTADO": "El simulador perdió la conexión (Last Will)",
                }[evento],
                "timestamp": payload.get("timestamp") or recibido_iso,
                "servidor_ts": self.fs.SERVER_TIMESTAMP,
            })

    def _procesar_ping(self, payload):
        t0 = payload.get("t0")
        if isinstance(t0, (int, float)):
            rtt = (time.monotonic() - t0) * 1000
            with self._lock:
                self._rtt.append(rtt)
            log.debug(f"RTT broker: {rtt:.0f} ms")

    # ── Watchdog y heartbeat ───────────────────────────────
    def revisar_dispositivos(self):
        for caido in self.monitor.revisar():
            ultimo_iso = ms_to_iso(caido["ultimo_visto"] * 1000)
            log.warning(
                f"[{caido['casa_id']}] ✗ OFFLINE: {caido['silencio_s']:.0f}s sin datos "
                f"(umbral {caido['umbral_s']:.0f}s)"
            )
            self.escritor.guardar(f"dispositivos/{caido['casa_id']}", {
                "estado": "offline",
                "offline_desde": ultimo_iso,
                "actualizado": self.fs.SERVER_TIMESTAMP,
            })
            self.escritor.agregar("eventos", {
                "casa_id": caido["casa_id"],
                "nombre": caido["nombre"],
                "tipo": "DISPOSITIVO_OFFLINE",
                "severidad": "ALTA",
                "descripcion": f"Sin datos durante más de {caido['umbral_s']:.0f}s",
                "ultimo_dato_ts": ultimo_iso,
                "umbral_s": caido["umbral_s"],
                "timestamp": ms_to_iso(self._reloj() * 1000),
                "servidor_ts": self.fs.SERVER_TIMESTAMP,
            })

    def enviar_ping(self):
        if not self.conectado.is_set() or self._client is None:
            return
        self._ping_seq += 1
        self._client.publish(self.topic_ping, json.dumps({"n": self._ping_seq, "t0": time.monotonic()}), qos=1)

    def estado_puente(self, estado="online"):
        with self._lock:
            rtt_ultimo = round(self._rtt[-1], 1) if self._rtt else None
            rtt_prom = promedio(self._rtt)
            lat_prom = promedio(self._lat_puente)
        return {
            "estado": estado,
            "version": VERSION,
            "client_id": self.client_id,
            "broker": f"{self.cfg.host}:{self.cfg.port}",
            "mqtt_conectado": self.conectado.is_set(),
            "inicio_ts": self._inicio_iso,
            "heartbeat_local_ts": ms_to_iso(self._reloj() * 1000),
            "heartbeat_ts": self.fs.SERVER_TIMESTAMP,
            "heartbeat_intervalo_s": self.cfg.heartbeat_s,
            "offline_factor": self.cfg.offline_factor,
            "offline_min_s": self.cfg.offline_min_s,
            "rtt_broker_ms": rtt_ultimo,
            "rtt_broker_prom_ms": rtt_prom,
            "latencia_hasta_puente_prom_ms": lat_prom,
            "escritura_firestore_prom_ms": self.escritor.commit_prom_ms(),
            "cola_escritura": self.escritor.pendientes(),
            "escritos": self.escritor.escritos,
            "descartados": self.escritor.descartados,
            "errores_firestore": self.escritor.errores,
            "mensajes": self.contadores.snapshot(),
            "dispositivos": self.monitor.resumen(),
        }

    def _bucle_mantenimiento(self):
        proximo_hb = 0.0
        while not self._parar.wait(self.cfg.watchdog_s):
            try:
                self.revisar_dispositivos()
                ahora = time.monotonic()
                if ahora >= proximo_hb:
                    proximo_hb = ahora + self.cfg.heartbeat_s
                    self.enviar_ping()
                    hb = self.estado_puente("online")
                    self.escritor.guardar("sistema/puente", hb, merge=False)
                    r = hb["dispositivos"]
                    log.info(
                        f"[Heartbeat] {r['online']}/{r['total']} online | "
                        f"RTT={hb['rtt_broker_ms']}ms | "
                        f"cola={hb['cola_escritura']} | escritos={hb['escritos']}"
                    )
            except Exception as e:
                log.error(f"Error en mantenimiento: {e}", exc_info=True)

    # ── Ciclo de vida ──────────────────────────────────────
    def cargar_registro(self):
        try:
            n = 0
            for snap in self.db.collection("dispositivos").stream():
                self.monitor.cargar_registro(snap.id, snap.to_dict() or {})
                n += 1
            log.info(f"Registro cargado: {n} dispositivo(s) conocidos")
        except Exception as e:
            log.warning(f"No se pudo leer dispositivos/ ({e}); se arranca sin registro previo")

    def arrancar(self):
        self.cargar_registro()
        self.escritor.start()
        self.escritor.guardar("sistema/puente", self.estado_puente("online"), merge=False)

        self._client = self._construir_cliente()
        log.info(f"Conectando a {self.cfg.host}:{self.cfg.port}…")
        self._client.connect_async(self.cfg.host, self.cfg.port, keepalive=60)
        self._client.loop_start()

        threading.Thread(target=self._bucle_mantenimiento, name="Mantenim.", daemon=True).start()

    def detener(self):
        log.info("Deteniendo puente…")
        self._parar.set()
        if self._client is not None:
            self._client.loop_stop()
            self._client.disconnect()
        self.conectado.clear()
        # El escritor vacía la cola antes de terminar, así que esto llega a Firestore
        self.escritor.guardar("sistema/puente", self.estado_puente("offline"), merge=False)
        self.escritor.detener()
        log.info(f"Puente detenido. Escritos: {self.escritor.escritos}, descartados: {self.escritor.descartados}")


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════
def setup_logging(nivel):
    logging.basicConfig(
        level=getattr(logging, nivel.upper(), logging.INFO),
        format="%(asctime)s [%(threadName)-10s] %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )


def main():
    cfg = Config()
    setup_logging(cfg.log_level)

    import firebase_admin
    from firebase_admin import credentials, firestore

    firebase_admin.initialize_app(credentials.Certificate(cfg.firebase_key))
    db = firestore.client()

    puente = Puente(cfg, db, firestore)
    puente.arrancar()
    log.info("Puente MQTT → Firestore activo. Ctrl+C para detener.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        puente.detener()


if __name__ == "__main__":
    main()
