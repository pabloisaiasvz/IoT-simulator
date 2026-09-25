"""Tests del simulador (metadatos de latencia y fallas simuladas)."""

import json
import os
import sys
import tempfile
import unittest

RAIZ = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, RAIZ)

# El simulador abre simulador_iot.log en el directorio actual al importarse:
# lo importamos desde un directorio temporal para no ensuciar el log real.
_cwd = os.getcwd()
os.chdir(tempfile.mkdtemp(prefix="sim_test_"))
try:
    import simulador_iot_v2 as sim  # noqa: E402
finally:
    os.chdir(_cwd)

import logging  # noqa: E402
logging.getLogger().setLevel(logging.CRITICAL)


class FakeClient:
    def __init__(self):
        self.publicados = []

    def publish(self, topic, payload=None, qos=0, retain=False):
        self.publicados.append((topic, json.loads(payload), qos, retain))


def gestor_con_fake():
    g = sim.GestorMQTT(sim.CFG, sim.queue.Queue(maxsize=10))
    g._client = FakeClient()
    return g


class TestPublicacion(unittest.TestCase):
    def test_envio_directo_estampa_enviado_ts(self):
        g = gestor_con_fake()
        g.conectado.set()
        g.publicar("t", {"timestamp": "x", "v": 1})
        _, payload, _, _ = g._client.publicados[0]
        self.assertIn("enviado_ts", payload)
        self.assertNotIn("reenviado_offline", payload)
        self.assertEqual(payload["v"], 1)

    def test_mensaje_encolado_se_marca_al_reenviar(self):
        g = gestor_con_fake()
        original = {"timestamp": "x"}
        g.publicar("t", original)                  # sin conexión → cola
        self.assertEqual(g._client.publicados, [])
        g._vaciar_cola_offline()
        _, payload, _, _ = g._client.publicados[0]
        self.assertTrue(payload["reenviado_offline"])
        self.assertNotIn("enviado_ts", original)    # no muta el dict original


class TestFallas(unittest.TestCase):
    def setUp(self):
        self._prob = sim.CFG.prob_falla
        self.casa = sim.SimuladorCasa(3, sim.PERFILES[2], gestor=None)

    def tearDown(self):
        sim.CFG.prob_falla = self._prob
        sim.FALLAS.marcar("CASA_03", False)

    def test_sin_probabilidad_nunca_falla(self):
        sim.CFG.prob_falla = 0
        self.assertFalse(any(self.casa._en_falla() for _ in range(200)))

    def test_entra_en_falla_y_se_recupera(self):
        sim.CFG.prob_falla = 1.0
        self.assertTrue(self.casa._en_falla())
        self.assertIn("CASA_03", sim.FALLAS.lista())
        self.assertTrue(self.casa._en_falla())      # sigue caída mientras dure
        sim.CFG.prob_falla = 0
        self.casa._falla_hasta = sim.time.time() - 1
        self.assertFalse(self.casa._en_falla())
        self.assertNotIn("CASA_03", sim.FALLAS.lista())


class TestMedicion(unittest.TestCase):
    def test_estructura(self):
        m, consumo = sim.generar_medicion(1, sim.PERFILES[0], 1800.0)
        self.assertEqual(m["casa_id"], "CASA_01")
        for campo in ("tension_v", "consumo_w", "corriente_a", "factor_potencia", "frecuencia_hz"):
            self.assertIn(campo, m["medicion"])
        self.assertEqual(consumo, m["medicion"]["consumo_w"])


if __name__ == "__main__":
    unittest.main()
