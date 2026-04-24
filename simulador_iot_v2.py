"""
Simulador IoT v2 — Medidor de consumo eléctrico
================================================
Mejoras sobre v1:
  ✓ Reconexión automática con backoff exponencial
  ✓ Cola offline: guarda mediciones si el broker cae
  ✓ Curva horaria de consumo realista (día/noche/pico)
  ✓ Correlación entre casas (baja tensión afecta a toda la red)
  ✓ Métricas globales y health check por heartbeat
  ✓ Config por .env (sin tocar el código)
  ✓ Autenticación usuario/contraseña y TLS opcional

Instalación:
    pip install paho-mqtt python-dotenv

Uso:
    cp .env.example .env   # configurar broker y umbrales
    python simulador_iot_v2.py

Topics MQTT:
    iot/casas/{id}/telemetria   → medición cada N segundos
    iot/casas/{id}/alertas      → anomalías (QoS 2)
    iot/casas/status            → heartbeat global cada 30s
    iot/casas/metricas          → estadísticas del simulador
"""

import json
import math
import os
import queue
import random
import ssl
import threading
import time
import logging
import logging.handlers
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from dotenv import load_dotenv

load_dotenv()


# ══════════════════════════════════════════════════════════════
# CONFIGURACIÓN — leída del .env con fallbacks razonables
# ══════════════════════════════════════════════════════════════
@dataclass
class Config:
    # Broker
    host = os.getenv("MQTT_HOST", "aec31a90bbda48f3b180689b08b9e33b.s1.eu.hivemq.cloud")
    port = int(os.getenv("MQTT_PORT", "8883"))
    user = os.getenv("MQTT_USER", "pablo")
    password = os.getenv("MQTT_PASSWORD", "Test1234")
    tls = os.getenv("MQTT_TLS", "True").lower() == "true"
    ca_cert:           str   = os.getenv("MQTT_CA_CERT", "")
    client_cert:       str   = os.getenv("MQTT_CLIENT_CERT", "")
    client_key:        str   = os.getenv("MQTT_CLIENT_KEY", "")

    # Simulación
    cantidad_casas:    int   = int(os.getenv("CANTIDAD_CASAS", "10"))
    intervalo_s:       int   = int(os.getenv("INTERVALO_SEGUNDOS", "5"))
    cola_max:          int   = int(os.getenv("COLA_MAX", "500"))

    # Umbrales de anomalías
    tension_min:       float = float(os.getenv("TENSION_MIN", "195"))
    tension_max:       float = float(os.getenv("TENSION_MAX", "245"))
    consumo_max:       float = float(os.getenv("CONSUMO_MAX", "4500"))
    fp_min:            float = float(os.getenv("FACTOR_POTENCIA_MIN", "0.75"))

    # Logs
    log_level:         str   = os.getenv("LOG_LEVEL", "INFO")


CFG = Config()


# ══════════════════════════════════════════════════════════════
# LOGGING — rotación de archivo + consola coloreada
# ══════════════════════════════════════════════════════════════
class ColorFormatter(logging.Formatter):
    _COLORES = {
        logging.DEBUG:   "\033[36m",
        logging.INFO:    "\033[32m",
        logging.WARNING: "\033[33m",
        logging.ERROR:   "\033[31m",
    }
    _RESET = "\033[0m"

    def format(self, record):
        color = self._COLORES.get(record.levelno, "")
        record.levelname = f"{color}{record.levelname:7}{self._RESET}"
        return super().format(record)


def setup_logging():
    fmt = "%(asctime)s [%(threadName)-12s] %(levelname)s %(message)s"
    datefmt = "%H:%M:%S"

    root = logging.getLogger()
    root.setLevel(getattr(logging, CFG.log_level.upper(), logging.INFO))

    # Consola con colores
    ch = logging.StreamHandler()
    ch.setFormatter(ColorFormatter(fmt, datefmt))
    root.addHandler(ch)

    # Archivo con rotación (5 MB × 3 backups)
    fh = logging.handlers.RotatingFileHandler(
        "simulador_iot.log", maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    fh.setFormatter(logging.Formatter(fmt, datefmt))
    root.addHandler(fh)

    return logging.getLogger(__name__)


log = setup_logging()


# ══════════════════════════════════════════════════════════════
# MÉTRICAS GLOBALES — thread-safe con lock
# ══════════════════════════════════════════════════════════════
@dataclass
class Metricas:
    mensajes_enviados:    int = 0
    alertas_enviadas:     int = 0
    mensajes_en_cola:     int = 0
    mensajes_perdidos:    int = 0
    reconexiones:         int = 0
    inicio:               float = field(default_factory=time.time)
    alertas_por_tipo:     dict  = field(default_factory=lambda: defaultdict(int))
    _lock:                object = field(default_factory=threading.Lock, repr=False)

    def incr(self, campo: str, n: int = 1):
        with self._lock:
            setattr(self, campo, getattr(self, campo) + n)

    def incr_alerta(self, tipo: str):
        with self._lock:
            self.alertas_por_tipo[tipo] += 1
            self.alertas_enviadas += 1

    def snapshot(self) -> dict:
        with self._lock:
            uptime = int(time.time() - self.inicio)
            return {
                "mensajes_enviados":  self.mensajes_enviados,
                "alertas_enviadas":   self.alertas_enviadas,
                "mensajes_en_cola":   self.mensajes_en_cola,
                "mensajes_perdidos":  self.mensajes_perdidos,
                "reconexiones":       self.reconexiones,
                "alertas_por_tipo":   dict(self.alertas_por_tipo),
                "uptime_segundos":    uptime,
                "msg_por_minuto":     round(self.mensajes_enviados / max(uptime / 60, 1), 1),
            }


METRICAS = Metricas()


# ══════════════════════════════════════════════════════════════
# ESTADO COMPARTIDO DE RED — correlación entre casas
# ══════════════════════════════════════════════════════════════
@dataclass
class EstadoRed:
    """
    Modela el estado global de la red eléctrica del barrio.
    Cuando hay un evento de red (baja tensión sistémica, corte de
    frecuencia), afecta a TODAS las casas simultáneamente, como
    pasa en la realidad.
    """
    evento_activo:    str   = ""   # "" / "baja_tension" / "sobretension" / "frecuencia"
    delta_tension:    float = 0.0  # Desvío de tensión aplicado globalmente (V)
    delta_frecuencia: float = 0.0  # Desvío de frecuencia global (Hz)
    fin_evento:       float = 0.0  # timestamp de cuándo termina el evento
    _lock:            object = field(default_factory=threading.Lock, repr=False)

    def tick(self):
        """Llamado cada intervalo para decidir si inicia/termina un evento de red."""
        with self._lock:
            ahora = time.time()

            # Terminar evento activo
            if self.evento_activo and ahora > self.fin_evento:
                log.info(f"[Red] Evento de red terminado: {self.evento_activo}")
                self.evento_activo = ""
                self.delta_tension = 0.0
                self.delta_frecuencia = 0.0

            # 1% de chance por ciclo de un nuevo evento de red (si no hay uno activo)
            if not self.evento_activo and random.random() < 0.01:
                tipo = random.choice(["baja_tension", "sobretension", "frecuencia"])
                duracion = random.uniform(10, 45)  # segundos
                self.evento_activo = tipo
                self.fin_evento = ahora + duracion

                if tipo == "baja_tension":
                    self.delta_tension = random.uniform(-35, -20)
                elif tipo == "sobretension":
                    self.delta_tension = random.uniform(20, 40)
                elif tipo == "frecuencia":
                    self.delta_frecuencia = random.choice([
                        random.uniform(-3, -1.6),
                        random.uniform(1.6, 3)
                    ])

                log.warning(
                    f"[Red] ⚡ EVENTO DE RED: {tipo} por {duracion:.0f}s "
                    f"(Δtensión={self.delta_tension:+.1f}V, Δfreq={self.delta_frecuencia:+.2f}Hz)"
                )

    def get_deltas(self) -> tuple[float, float]:
        with self._lock:
            return self.delta_tension, self.delta_frecuencia


RED = EstadoRed()


# ══════════════════════════════════════════════════════════════
# PERFILES DE CASAS
# ══════════════════════════════════════════════════════════════
PERFILES = [
    {"nombre": "Casa familiar grande",     "base_w": 1800, "variacion": 600,  "tension_base": 220, "tipo": "residencial"},
    {"nombre": "Departamento pequeño",     "base_w":  600, "variacion": 200,  "tension_base": 218, "tipo": "residencial"},
    {"nombre": "Local comercial",          "base_w": 3200, "variacion": 900,  "tension_base": 221, "tipo": "comercial"},
    {"nombre": "Casa con paneles solares", "base_w":  900, "variacion": 400,  "tension_base": 222, "tipo": "residencial"},
    {"nombre": "Taller mecánico",          "base_w": 4000, "variacion": 1200, "tension_base": 219, "tipo": "industrial"},
    {"nombre": "Familia numerosa",         "base_w": 2200, "variacion": 700,  "tension_base": 220, "tipo": "residencial"},
    {"nombre": "Casa de fin de semana",    "base_w":  200, "variacion": 100,  "tension_base": 217, "tipo": "residencial"},
    {"nombre": "Edificio de oficinas",     "base_w": 5000, "variacion": 1500, "tension_base": 223, "tipo": "comercial"},
    {"nombre": "Estudio / Home office",    "base_w": 1100, "variacion": 300,  "tension_base": 220, "tipo": "residencial"},
    {"nombre": "Local gastronómico",       "base_w": 3800, "variacion": 1000, "tension_base": 221, "tipo": "comercial"},
]


# ══════════════════════════════════════════════════════════════
# CURVA HORARIA — consumo según hora del día
# ══════════════════════════════════════════════════════════════
def factor_horario(hora: float, tipo_perfil: str) -> float:
    """
    Devuelve un multiplicador de consumo (0.1 – 1.4) según la
    hora UTC actual y el tipo de perfil.

    Residencial: pico mañana (7-9h) y tarde-noche (18-22h).
    Comercial:   pico en horario laboral (9-18h), bajo de noche.
    Industrial:  relativamente plano con algo de pico diurno.
    """
    h = hora % 24

    if tipo_perfil == "residencial":
        # Curva con dos picos: mañana y noche
        manana = 0.7 * math.exp(-0.5 * ((h - 8)  / 1.5) ** 2)
        noche  = 1.0 * math.exp(-0.5 * ((h - 20) / 2.0) ** 2)
        base   = 0.15
        return round(base + manana + noche, 3)

    elif tipo_perfil == "comercial":
        # Campana centrada en mediodía laboral
        if 9 <= h <= 18:
            return round(0.6 + 0.4 * math.sin(math.pi * (h - 9) / 9), 3)
        elif h < 9:
            return 0.15
        else:
            return max(0.1, round(0.6 * math.exp(-0.3 * (h - 18)), 3))

    elif tipo_perfil == "industrial":
        # Relativamente plano en horario laboral, bajo de noche
        if 7 <= h <= 19:
            return round(0.75 + 0.15 * random.random(), 3)
        else:
            return round(0.25 + 0.1 * random.random(), 3)

    return 1.0


# ══════════════════════════════════════════════════════════════
# GENERADOR DE MEDICIONES
# ══════════════════════════════════════════════════════════════
def generar_medicion(
    casa_id: int,
    perfil: dict,
    consumo_anterior: float
) -> tuple[dict, float]:
    """
    Genera una medición eléctrica realista.
    Retorna (medicion_dict, nuevo_consumo_watts).

    Cambios respecto a v1:
    - Aplica curva horaria al consumo base
    - Suaviza cambios de consumo (inercia, no saltos abruptos)
    - Incorpora el estado global de la red (correlación entre casas)
    - Con 4% de probabilidad inyecta una anomalía individual
    """
    ahora = datetime.now(timezone.utc)
    hora_utc = ahora.hour + ahora.minute / 60.0

    # ── 1. Consumo con curva horaria y suavizado ─────────────
    fh = factor_horario(hora_utc, perfil["tipo"])
    consumo_objetivo = perfil["base_w"] * fh + random.uniform(
        -perfil["variacion"] * 0.5, perfil["variacion"] * 0.5
    )
    # Suavizado exponencial: inercia del 70% para evitar saltos
    alpha = 0.30
    consumo = alpha * consumo_objetivo + (1 - alpha) * consumo_anterior
    consumo = max(10.0, consumo)

    # ── 2. Tensión y frecuencia base ─────────────────────────
    tension    = perfil["tension_base"] + random.gauss(0, 1.5)
    frecuencia = 50.0 + random.gauss(0, 0.1)

    # ── 3. Aplicar estado global de la red ───────────────────
    delta_t, delta_f = RED.get_deltas()
    tension    += delta_t
    frecuencia += delta_f

    # ── 4. Factor de potencia ─────────────────────────────────
    factor_potencia = round(random.uniform(0.87, 0.99), 3)

    # ── 5. Anomalía individual (independiente de la red) ──────
    tipo_anomalia = None
    if random.random() < 0.04:
        tipo_anomalia = random.choice([
            "pico_consumo",
            "factor_potencia_bajo",
        ])
        if tipo_anomalia == "pico_consumo":
            consumo = random.uniform(CFG.consumo_max * 1.05, CFG.consumo_max * 1.5)
        elif tipo_anomalia == "factor_potencia_bajo":
            factor_potencia = round(random.uniform(0.45, 0.74), 3)

    # ── 6. Derivados eléctricos ───────────────────────────────
    tension    = round(tension, 1)
    frecuencia = round(frecuencia, 2)
    consumo    = round(consumo, 1)

    potencia_aparente = round(consumo / factor_potencia, 1) if factor_potencia > 0 else 0.0
    potencia_reactiva = 0.0
    if potencia_aparente > consumo:
        potencia_reactiva = round(math.sqrt(potencia_aparente**2 - consumo**2), 1)
    corriente = round(potencia_aparente / tension, 2) if tension > 0 else 0.0

    medicion = {
        "casa_id":   f"CASA_{casa_id:02d}",
        "nombre":    perfil["nombre"],
        "timestamp": ahora.isoformat(),
        "medicion": {
            "tension_v":            tension,
            "consumo_w":            consumo,
            "corriente_a":          corriente,
            "factor_potencia":      factor_potencia,
            "potencia_aparente_va": potencia_aparente,
            "potencia_reactiva_var":potencia_reactiva,
            "frecuencia_hz":        frecuencia,
            "factor_horario":       fh,
            "hora_utc":             round(hora_utc, 2),
        },
        "evento_red":        RED.evento_activo or None,
        "_anomalia_local":   tipo_anomalia,
    }

    return medicion, consumo


# ══════════════════════════════════════════════════════════════
# DETECTOR DE ANOMALÍAS
# ══════════════════════════════════════════════════════════════
def detectar_anomalias(medicion: dict) -> list[dict]:
    alertas = []
    m = medicion["medicion"]

    checks = [
        (m["tension_v"] < CFG.tension_min,
         "BAJA_TENSION",
         "ALTA" if m["tension_v"] < 185 else "MEDIA",
         f"Tensión {m['tension_v']}V bajo mínimo ({CFG.tension_min}V)",
         m["tension_v"]),

        (m["tension_v"] > CFG.tension_max,
         "SOBRETENSION",
         "ALTA" if m["tension_v"] > 255 else "MEDIA",
         f"Tensión {m['tension_v']}V sobre máximo ({CFG.tension_max}V)",
         m["tension_v"]),

        (m["consumo_w"] > CFG.consumo_max,
         "PICO_CONSUMO",
         "ALTA" if m["consumo_w"] > CFG.consumo_max * 1.3 else "MEDIA",
         f"Consumo {m['consumo_w']}W sobre máximo ({CFG.consumo_max}W)",
         m["consumo_w"]),

        (m["factor_potencia"] < CFG.fp_min,
         "FACTOR_POTENCIA_BAJO",
         "BAJA",
         f"FP {m['factor_potencia']} bajo mínimo ({CFG.fp_min})",
         m["factor_potencia"]),

        (not (48.5 <= m["frecuencia_hz"] <= 51.5),
         "FRECUENCIA_ANORMAL",
         "ALTA",
         f"Frecuencia {m['frecuencia_hz']}Hz fuera de rango (48.5-51.5Hz)",
         m["frecuencia_hz"]),
    ]

    for condicion, tipo, severidad, descripcion, valor in checks:
        if condicion:
            alertas.append({
                "tipo":       tipo,
                "severidad":  severidad,
                "descripcion":descripcion,
                "valor":      valor,
                "evento_red": medicion.get("evento_red"),
            })

    return alertas


# ══════════════════════════════════════════════════════════════
# GESTOR DE CONEXIÓN MQTT — reconexión con backoff exponencial
# ══════════════════════════════════════════════════════════════
class GestorMQTT:
    """
    Encapsula la conexión al broker. Si se cae, reintenta con
    backoff exponencial (1s → 2s → 4s → … → máx 60s).
    Mientras no hay conexión, los mensajes van a la cola offline.
    """
    MAX_BACKOFF   = 60
    KEEPALIVE     = 60

    def __init__(self, cfg: Config, cola_offline: "queue.Queue"):
        self.cfg          = cfg
        self.cola_offline = cola_offline
        self.conectado    = threading.Event()
        self._client      = None
        self._lock        = threading.Lock()
        self._backoff     = 1

        self._construir_cliente()

    def _construir_cliente(self):
        client_id = f"sim_iot_{random.randint(1000, 9999)}"
        c = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)

        if self.cfg.user:
            c.username_pw_set(self.cfg.user, self.cfg.password)

        if self.cfg.tls:
            tls_kwargs = {"tls_version": ssl.PROTOCOL_TLS}
            if self.cfg.ca_cert:
                tls_kwargs["ca_certs"] = self.cfg.ca_cert
            if self.cfg.client_cert and self.cfg.client_key:
                tls_kwargs["certfile"] = self.cfg.client_cert
                tls_kwargs["keyfile"]  = self.cfg.client_key
            c.tls_set(**tls_kwargs)

        c.on_connect    = self._on_connect
        c.on_disconnect = self._on_disconnect
        c.on_publish    = self._on_publish

        with self._lock:
            self._client = c

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            log.info(f"✓ Conectado a {self.cfg.host}:{self.cfg.port}")
            self._backoff = 1
            self.conectado.set()
            METRICAS.incr("reconexiones")
            self._vaciar_cola_offline()
        else:
            msgs = {1:"protocolo", 2:"client_id", 3:"broker no disponible", 4:"credenciales", 5:"autorización"}
            log.error(f"✗ Broker rechazó conexión: {msgs.get(rc, rc)}")

    def _on_disconnect(self, client, userdata, rc):
        self.conectado.clear()
        if rc != 0:
            log.warning(f"Desconexión inesperada (rc={rc}). Reconectando…")
            threading.Thread(target=self._reconectar, daemon=True).start()

    def _on_publish(self, client, userdata, mid):
        METRICAS.incr("mensajes_enviados")

    def _reconectar(self):
        while not self.conectado.is_set():
            log.info(f"Reintentando conexión en {self._backoff}s…")
            time.sleep(self._backoff)
            self._backoff = min(self._backoff * 2, self.MAX_BACKOFF)
            try:
                with self._lock:
                    self._client.reconnect()
                break
            except Exception as e:
                log.warning(f"Reconexión fallida: {e}")

    def conectar(self):
        log.info(f"Conectando a {self.cfg.host}:{self.cfg.port}…")
        try:
            with self._lock:
                self._client.connect(self.cfg.host, self.cfg.port, keepalive=self.KEEPALIVE)
                self._client.loop_start()
        except Exception as e:
            log.error(f"Conexión inicial fallida: {e}")
            threading.Thread(target=self._reconectar, daemon=True).start()

    def publicar(self, topic: str, payload: dict, qos: int = 1, retain: bool = False):
        """Publica directo si hay conexión; si no, encola."""
        msg = {"topic": topic, "payload": payload, "qos": qos, "retain": retain}

        if self.conectado.is_set():
            self._publicar_raw(msg)
        else:
            try:
                self.cola_offline.put_nowait(msg)
                METRICAS.mensajes_en_cola = self.cola_offline.qsize()
            except queue.Full:
                METRICAS.incr("mensajes_perdidos")
                log.debug("Cola offline llena, mensaje descartado")

    def _publicar_raw(self, msg: dict):
        payload_str = json.dumps(msg["payload"], ensure_ascii=False)
        with self._lock:
            self._client.publish(
                msg["topic"],
                payload=payload_str,
                qos=msg["qos"],
                retain=msg["retain"]
            )

    def _vaciar_cola_offline(self):
        vaciados = 0
        while not self.cola_offline.empty():
            try:
                msg = self.cola_offline.get_nowait()
                self._publicar_raw(msg)
                vaciados += 1
            except queue.Empty:
                break
        if vaciados:
            log.info(f"Cola offline vaciada: {vaciados} mensajes reenviados")
        METRICAS.mensajes_en_cola = self.cola_offline.qsize()

    def desconectar(self):
        with self._lock:
            self._client.loop_stop()
            self._client.disconnect()


# ══════════════════════════════════════════════════════════════
# HILO SIMULADOR POR CASA
# ══════════════════════════════════════════════════════════════
class SimuladorCasa(threading.Thread):
    def __init__(self, casa_id: int, perfil: dict, gestor: GestorMQTT):
        super().__init__(name=f"Casa-{casa_id:02d}", daemon=True)
        self.casa_id  = casa_id
        self.perfil   = perfil
        self.gestor   = gestor
        self.topic_t  = f"iot/casas/CASA_{casa_id:02d}/telemetria"
        self.topic_a  = f"iot/casas/CASA_{casa_id:02d}/alertas"
        # Estado previo para suavizado de consumo
        self._consumo_prev = float(perfil["base_w"])

    def run(self):
        # Arranque escalonado: evita ráfaga inicial al broker
        time.sleep(self.casa_id * 0.4)
        log.info(f"[{self.name}] Iniciado → {self.perfil['nombre']} ({self.perfil['tipo']})")

        while True:
            try:
                # Tick de estado de red (solo un hilo lo hace, los demás leen)
                if self.casa_id == 1:
                    RED.tick()

                medicion, self._consumo_prev = generar_medicion(
                    self.casa_id, self.perfil, self._consumo_prev
                )

                # Publicar telemetría
                self.gestor.publicar(self.topic_t, medicion, qos=1)

                m = medicion["medicion"]
                log.info(
                    f"[{self.name}] "
                    f"{m['tension_v']:5.1f}V | "
                    f"{m['consumo_w']:6.0f}W | "
                    f"FP={m['factor_potencia']:.2f} | "
                    f"FH={m['factor_horario']:.2f}"
                    + (f" | RED:{medicion['evento_red']}" if medicion["evento_red"] else "")
                )

                # Detectar y publicar alertas
                for alerta in detectar_anomalias(medicion):
                    payload_alerta = {
                        "casa_id":   medicion["casa_id"],
                        "nombre":    medicion["nombre"],
                        "timestamp": medicion["timestamp"],
                        "alerta":    alerta,
                    }
                    self.gestor.publicar(self.topic_a, payload_alerta, qos=2)
                    METRICAS.incr_alerta(alerta["tipo"])
                    log.warning(
                        f"[{self.name}] ⚠  [{alerta['severidad']}] "
                        f"{alerta['tipo']}: {alerta['descripcion']}"
                    )

            except Exception as e:
                log.error(f"[{self.name}] Error inesperado: {e}", exc_info=True)

            time.sleep(CFG.intervalo_s)


# ══════════════════════════════════════════════════════════════
# HILO DE HEARTBEAT Y MÉTRICAS
# ══════════════════════════════════════════════════════════════
class HeartbeatThread(threading.Thread):
    """
    Cada 30 segundos publica:
    - iot/casas/status    → estado del simulador (retained)
    - iot/casas/metricas  → snapshot de métricas
    """
    INTERVALO = 30

    def __init__(self, gestor: GestorMQTT):
        super().__init__(name="Heartbeat", daemon=True)
        self.gestor = gestor

    def run(self):
        while True:
            time.sleep(self.INTERVALO)
            try:
                snap = METRICAS.snapshot()
                status = {
                    "evento": "HEARTBEAT",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "casas_activas": CFG.cantidad_casas,
                    "intervalo_s": CFG.intervalo_s,
                    "evento_red": RED.evento_activo or None,
                    "cola_offline": snap["mensajes_en_cola"],
                    "uptime_s": snap["uptime_segundos"],
                }
                self.gestor.publicar("iot/casas/status",   status,  qos=1, retain=True)
                self.gestor.publicar("iot/casas/metricas", snap,    qos=0)
                log.info(
                    f"[Heartbeat] ↑{snap['mensajes_enviados']} msg "
                    f"| ⚠{snap['alertas_enviadas']} alertas "
                    f"| cola={snap['mensajes_en_cola']} "
                    f"| {snap['msg_por_minuto']} msg/min"
                )
            except Exception as e:
                log.error(f"[Heartbeat] Error: {e}")


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════
def main():
    log.info("═" * 62)
    log.info("  SIMULADOR IoT v2 — Sistema de Monitoreo Eléctrico")
    log.info(f"  {CFG.cantidad_casas} casas | Intervalo: {CFG.intervalo_s}s")
    log.info(f"  Broker: {CFG.host}:{CFG.port} | TLS: {CFG.tls}")
    log.info("═" * 62)

    # Cola offline compartida entre todos los hilos
    cola_offline = queue.Queue(maxsize=CFG.cola_max)

    # Gestor de conexión MQTT
    gestor = GestorMQTT(CFG, cola_offline)
    gestor.conectar()

    # Esperar conexión inicial (máx 15s antes de arrancar igual)
    gestor.conectado.wait(timeout=15)
    if not gestor.conectado.is_set():
        log.warning("Sin conexión inicial — el simulador arranca en modo offline")

    # Publicar mensaje de inicio
    gestor.publicar("iot/casas/status", {
        "evento":          "SIMULADOR_INICIADO",
        "timestamp":       datetime.now(timezone.utc).isoformat(),
        "casas_activas":   CFG.cantidad_casas,
        "intervalo_s":     CFG.intervalo_s,
        "version":         "2.0",
    }, qos=1, retain=True)

    # Lanzar hilos de casas
    hilos = []
    for i, perfil in enumerate(PERFILES[:CFG.cantidad_casas], start=1):
        h = SimuladorCasa(casa_id=i, perfil=perfil, gestor=gestor)
        h.start()
        hilos.append(h)

    # Lanzar heartbeat
    HeartbeatThread(gestor).start()

    log.info(f"\n{CFG.cantidad_casas} hilos activos. Topics:")
    log.info(f"  iot/casas/CASA_01/telemetria … CASA_{CFG.cantidad_casas:02d}/telemetria")
    log.info(f"  iot/casas/CASA_01/alertas    … CASA_{CFG.cantidad_casas:02d}/alertas")
    log.info(f"  iot/casas/status  (heartbeat cada {HeartbeatThread.INTERVALO}s, retained)")
    log.info(f"  iot/casas/metricas")
    log.info("\nCtrl+C para detener.\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("\nDeteniendo simulador…")
        gestor.publicar("iot/casas/status", {
            "evento":    "SIMULADOR_DETENIDO",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "metricas":  METRICAS.snapshot(),
        }, qos=1, retain=True)
        time.sleep(1)  # dar tiempo al último publish
        gestor.desconectar()
        log.info("Simulador detenido correctamente.")


if __name__ == "__main__":
    main()
