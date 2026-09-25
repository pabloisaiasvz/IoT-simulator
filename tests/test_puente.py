"""Tests del puente MQTT → Firestore (sin red ni Firestore reales).

Ejecutar desde la carpeta IoT-simulator:
    python -m unittest discover -s tests -v
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import subscriber_firebase as sf  # noqa: E402


# ── Fakes ──────────────────────────────────────────────────────
class FakeFirestoreModule:
    SERVER_TIMESTAMP = "<SERVER_TS>"

    @staticmethod
    def Increment(n):
        return ("increment", n)


class FakeRef:
    def __init__(self, path):
        self.path = path


class FakeCollection:
    def __init__(self, db, name):
        self.db, self.name = db, name

    def document(self, doc_id=None):
        self.db.auto_id += 1
        return FakeRef(f"{self.name}/{doc_id or f'auto{self.db.auto_id}'}")

    def stream(self):
        return self.db.stream_docs.get(self.name, [])


class FakeBatch:
    def __init__(self, db):
        self.db, self.ops = db, []

    def set(self, ref, data, merge=False):
        self.ops.append((ref.path, data, merge))

    def commit(self):
        if self.db.fallos_pendientes > 0:
            self.db.fallos_pendientes -= 1
            raise RuntimeError("Firestore no disponible")
        self.db.commits.append(self.ops)


class FakeDB:
    def __init__(self):
        self.auto_id = 0
        self.commits = []
        self.fallos_pendientes = 0
        self.stream_docs = {}

    def batch(self):
        return FakeBatch(self)

    def collection(self, name):
        return FakeCollection(self, name)

    def document(self, path):
        return FakeRef(path)


class FakeSnap:
    def __init__(self, doc_id, data):
        self.id, self._data = doc_id, data

    def to_dict(self):
        return self._data


class Reloj:
    def __init__(self, t=1_800_000_000.0):
        self.t = t

    def __call__(self):
        return self.t


def operaciones(puente):
    """Saca de la cola del escritor las operaciones pendientes."""
    ops = []
    while True:
        lote = puente.escritor._tomar_lote() if not puente.escritor._cola.empty() else []
        if not lote:
            return ops
        ops.extend(lote)


def lectura(casa="CASA_01", seq=1, ts="2027-01-15T08:00:00+00:00", enviado=None, **extra):
    p = {
        "casa_id": casa,
        "nombre": "Casa test",
        "timestamp": ts,
        "medicion": {"tension_v": 220.0, "consumo_w": 1000.0},
        "seq": seq,
        "intervalo_s": 5,
    }
    if enviado:
        p["enviado_ts"] = enviado
    p.update(extra)
    return p


# ── Helpers de tiempo ──────────────────────────────────────────
class TestTiempo(unittest.TestCase):
    def test_parse_iso_con_offset_z_y_sin_zona(self):
        a = sf.parse_iso_ms("2027-01-15T08:00:00+00:00")
        self.assertEqual(a, sf.parse_iso_ms("2027-01-15T08:00:00Z"))
        self.assertEqual(a, sf.parse_iso_ms("2027-01-15T08:00:00"))
        self.assertEqual(sf.parse_iso_ms("2027-01-15T05:00:00-03:00"), a)

    def test_parse_iso_invalido(self):
        self.assertIsNone(sf.parse_iso_ms(None))
        self.assertIsNone(sf.parse_iso_ms(""))
        self.assertIsNone(sf.parse_iso_ms("ayer"))
        self.assertIsNone(sf.parse_iso_ms(12345))

    def test_ms_to_iso_ida_y_vuelta(self):
        ms = sf.parse_iso_ms("2027-01-15T08:00:00.250000+00:00")
        self.assertEqual(sf.parse_iso_ms(sf.ms_to_iso(ms)), ms)

    def test_latencias_completas(self):
        gen = sf.parse_iso_ms("2027-01-15T08:00:00+00:00")
        lat = sf.calcular_latencias(
            {"timestamp": "2027-01-15T08:00:00+00:00", "enviado_ts": "2027-01-15T08:00:00.100+00:00"},
            gen + 350,
        )
        self.assertEqual(lat, {"cola": 100.0, "red": 250.0, "hasta_puente": 350.0})

    def test_latencias_payload_viejo_sin_enviado_ts(self):
        gen = sf.parse_iso_ms("2027-01-15T08:00:00+00:00")
        lat = sf.calcular_latencias({"timestamp": "2027-01-15T08:00:00+00:00"}, gen + 80)
        self.assertEqual(lat, {"hasta_puente": 80.0})
        self.assertEqual(sf.calcular_latencias({}, gen), {})


# ── Monitor de dispositivos ────────────────────────────────────
class TestMonitor(unittest.TestCase):
    def setUp(self):
        self.reloj = Reloj()
        self.m = sf.MonitorDispositivos(offline_factor=3, offline_min_s=15, intervalo_default_s=5, reloj=self.reloj)

    def test_umbral(self):
        self.assertEqual(self.m.umbral_s(5), 15)
        self.assertEqual(self.m.umbral_s(10), 30)
        self.assertEqual(self.m.umbral_s(1), 15)   # nunca menos que el mínimo
        self.assertEqual(self.m.umbral_s(None), 15)

    def test_primera_lectura_es_nueva(self):
        info = self.m.registrar_lectura("A", seq=1, intervalo_s=5)
        self.assertTrue(info["nuevo"])
        self.assertFalse(info["volvio_online"])
        self.assertEqual(self.m.estado("A")["estado"], "online")

    def test_hueco_en_secuencia_cuenta_perdidos(self):
        self.m.registrar_lectura("A", seq=1)
        info = self.m.registrar_lectura("A", seq=4)
        self.assertEqual(info["perdidos"], 2)
        self.assertEqual(self.m.estado("A")["perdidos"], 2)
        self.assertEqual(self.m.resumen()["perdidos_seq"], 2)

    def test_duplicado(self):
        self.m.registrar_lectura("A", seq=7)
        info = self.m.registrar_lectura("A", seq=7)
        self.assertTrue(info["duplicado"])
        self.assertEqual(self.m.estado("A")["lecturas"], 1)

    def test_reinicio_del_dispositivo(self):
        self.m.registrar_lectura("A", seq=500)
        info = self.m.registrar_lectura("A", seq=1)
        self.assertTrue(info["reinicio"])
        self.assertEqual(info["perdidos"], 0)
        self.assertEqual(self.m.registrar_lectura("A", seq=2)["perdidos"], 0)

    def test_fuera_de_orden_descuenta_perdido(self):
        self.m.registrar_lectura("A", seq=1)
        self.m.registrar_lectura("A", seq=3)       # se cuenta 1 perdido (seq 2)
        info = self.m.registrar_lectura("A", seq=2)  # llega tarde
        self.assertTrue(info["fuera_de_orden"])
        self.assertEqual(self.m.estado("A")["perdidos"], 0)

    def test_sin_seq_no_rompe(self):
        info = self.m.registrar_lectura("A", seq=None)
        self.assertEqual(info["perdidos"], 0)
        self.assertFalse(info["duplicado"])

    def test_offline_y_vuelta_a_online(self):
        self.m.registrar_lectura("A", nombre="Casa A", seq=1, intervalo_s=5)
        self.reloj.t += 14
        self.assertEqual(self.m.revisar(), [])       # aún dentro del umbral
        self.reloj.t += 2
        caidos = self.m.revisar()
        self.assertEqual(len(caidos), 1)
        self.assertEqual(caidos[0]["casa_id"], "A")
        self.assertEqual(caidos[0]["umbral_s"], 15)
        self.assertEqual(self.m.revisar(), [])       # no se reporta dos veces
        self.assertEqual(self.m.resumen(), {"total": 1, "online": 0, "offline": 1, "perdidos_seq": 0})

        self.reloj.t += 30
        info = self.m.registrar_lectura("A", seq=2)
        self.assertTrue(info["volvio_online"])
        self.assertEqual(info["duracion_offline_s"], 46.0)
        self.assertEqual(self.m.estado("A")["estado"], "online")

    def test_registro_previo_detecta_dispositivo_que_no_vuelve(self):
        hace_un_rato = sf.ms_to_iso((self.reloj.t - 120) * 1000)
        self.m.cargar_registro("B", {"nombre": "Casa B", "estado": "online",
                                     "ultimo_recibido_ts": hace_un_rato, "intervalo_s": 5})
        caidos = self.m.revisar()
        self.assertEqual([c["casa_id"] for c in caidos], ["B"])


# ── Escritor Firestore ─────────────────────────────────────────
class TestEscritor(unittest.TestCase):
    def setUp(self):
        self.db = FakeDB()
        self.e = sf.EscritorFirestore(self.db, cola_max=3, batch_max=10)
        self.e._parar.wait = lambda t: False  # sin esperas reales en los reintentos

    def test_lote_mezcla_add_y_set(self):
        self.e.agregar("telemetria", {"a": 1})
        self.e.guardar("sistema/puente", {"b": 2}, merge=False)
        self.assertTrue(self.e.escribir_lote(self.e._tomar_lote()))
        ops = self.db.commits[0]
        self.assertTrue(ops[0][0].startswith("telemetria/auto"))
        self.assertEqual(ops[1], ("sistema/puente", {"b": 2}, False))
        self.assertEqual(self.e.escritos, 2)

    def test_reintenta_hasta_que_funciona(self):
        self.db.fallos_pendientes = 2
        self.e.agregar("telemetria", {"a": 1})
        self.assertTrue(self.e.escribir_lote(self.e._tomar_lote()))
        self.assertEqual(self.e.errores, 2)
        self.assertEqual(len(self.db.commits), 1)

    def test_descarta_tras_max_intentos(self):
        self.db.fallos_pendientes = 99
        self.e.agregar("telemetria", {"a": 1})
        self.assertFalse(self.e.escribir_lote(self.e._tomar_lote()))
        self.assertEqual(self.e.descartados, 1)

    def test_cola_llena_descarta_sin_bloquear(self):
        for i in range(5):
            self.e.agregar("telemetria", {"i": i})
        self.assertEqual(self.e.pendientes(), 3)
        self.assertEqual(self.e.descartados, 2)


# ── Puente ─────────────────────────────────────────────────────
class TestPuente(unittest.TestCase):
    def setUp(self):
        self.reloj = Reloj(sf.parse_iso_ms("2027-01-15T08:00:00+00:00") / 1000)
        cfg = sf.Config()
        cfg.offline_factor, cfg.offline_min_s, cfg.registro_throttle_s = 3, 15, 30
        self.p = sf.Puente(cfg, FakeDB(), FakeFirestoreModule, reloj=self.reloj)

    def recibir(self, payload, dt=0.0, topic=None, retained=False):
        self.reloj.t += dt
        topic = topic or f"iot/casas/{payload.get('casa_id', 'X')}/telemetria"
        self.p.procesar(topic, payload, self.reloj.t * 1000, retained=retained)
        return operaciones(self.p)

    def test_telemetria_enriquecida_con_latencias(self):
        ops = self.recibir(lectura(enviado="2027-01-15T08:00:00.040+00:00"), dt=0.3)
        tel = [o for o in ops if o[1] == "telemetria"]
        self.assertEqual(len(tel), 1)
        doc = tel[0][2]
        self.assertEqual(doc["latencia_ms"], {"cola": 40.0, "red": 260.0, "hasta_puente": 300.0})
        self.assertEqual(doc["servidor_ts"], "<SERVER_TS>")
        self.assertIn("recibido_ts", doc)
        self.assertEqual(doc["medicion"]["consumo_w"], 1000.0)

    def test_registro_con_throttle(self):
        ops1 = self.recibir(lectura(seq=1))
        ops2 = self.recibir(lectura(seq=2), dt=5)
        ops3 = self.recibir(lectura(seq=3), dt=30)
        reg = lambda ops: [o for o in ops if o[1] == "dispositivos/CASA_01"]  # noqa: E731
        self.assertEqual(len(reg(ops1)), 1)   # alta nueva → inmediato
        self.assertEqual(len(reg(ops2)), 0)   # dentro del throttle
        self.assertEqual(len(reg(ops3)), 1)
        self.assertEqual(reg(ops3)[0][2]["lecturas"], ("increment", 2))
        self.assertEqual(reg(ops3)[0][2]["estado"], "online")

    def test_duplicado_no_se_guarda(self):
        self.recibir(lectura(seq=1))
        ops = self.recibir(lectura(seq=1), dt=1)
        self.assertEqual(ops, [])
        self.assertEqual(self.p.contadores.snapshot()["duplicados"], 1)

    def test_invalidos(self):
        self.assertEqual(self.recibir({"casa_id": "CASA_01"}), [])
        self.p.procesar("iot/casas/CASA_01/telemetria", ["no", "dict"], 0)
        self.assertEqual(self.p.contadores.snapshot()["invalidos"], 2)

    def test_watchdog_offline_y_online(self):
        self.recibir(lectura(seq=1))
        self.reloj.t += 20
        self.p.revisar_dispositivos()
        ops = operaciones(self.p)
        evento = [o[2] for o in ops if o[1] == "eventos"]
        self.assertEqual(evento[0]["tipo"], "DISPOSITIVO_OFFLINE")
        estado = [o[2] for o in ops if o[1] == "dispositivos/CASA_01"]
        self.assertEqual(estado[0]["estado"], "offline")

        ops = self.recibir(lectura(seq=2), dt=10)
        evento = [o[2] for o in ops if o[1] == "eventos"]
        self.assertEqual(evento[0]["tipo"], "DISPOSITIVO_ONLINE")
        self.assertEqual(evento[0]["duracion_offline_s"], 30.0)
        self.assertEqual(self.p.estado_puente()["dispositivos"]["online"], 1)

    def test_alerta(self):
        ops = self.recibir({
            "casa_id": "CASA_02", "nombre": "N", "timestamp": "2027-01-15T08:00:00+00:00",
            "alerta": {"tipo": "PICO_CONSUMO", "severidad": "ALTA", "descripcion": "x", "valor": 6000},
        }, topic="iot/casas/CASA_02/alertas")
        self.assertEqual(ops[0][1], "alertas")
        self.assertEqual(ops[0][2]["tipo"], "PICO_CONSUMO")

    def test_status_retained_no_genera_evento(self):
        st = {"evento": "SIMULADOR_INICIADO", "timestamp": "2027-01-15T08:00:00+00:00"}
        ops = self.recibir(st, topic="iot/casas/status", retained=True)
        self.assertEqual([o[1] for o in ops], ["sistema/simulador"])
        ops = self.recibir(st, topic="iot/casas/status", retained=False)
        self.assertEqual(sorted(o[1] for o in ops), ["eventos", "sistema/simulador"])

    def test_last_will_marca_simulador_offline(self):
        ops = self.recibir({"evento": "SIMULADOR_DESCONECTADO"}, topic="iot/casas/status")
        sim = [o[2] for o in ops if o[1] == "sistema/simulador"][0]
        self.assertEqual(sim["estado"], "offline")

    def test_ping_calcula_rtt(self):
        t0 = sf.time.monotonic() - 0.05
        self.p.procesar(self.p.topic_ping, {"n": 1, "t0": t0}, 0)
        rtt = self.p.estado_puente()["rtt_broker_ms"]
        self.assertGreaterEqual(rtt, 50)
        self.assertLess(rtt, 5000)

    def test_carga_registro_desde_firestore(self):
        self.p.db.stream_docs["dispositivos"] = [FakeSnap("CASA_09", {
            "nombre": "Vieja", "estado": "online",
            "ultimo_recibido_ts": sf.ms_to_iso((self.reloj.t - 300) * 1000),
        })]
        self.p.cargar_registro()
        self.p.revisar_dispositivos()
        tipos = [o[2]["tipo"] for o in operaciones(self.p) if o[1] == "eventos"]
        self.assertEqual(tipos, ["DISPOSITIVO_OFFLINE"])


if __name__ == "__main__":
    unittest.main()
