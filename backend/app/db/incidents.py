"""Abertura de incidente: uma transação, três escritas.

Metade desse estado é pior que nenhum — um provedor marcado como degradado sem
incidente por trás é achado de auditoria na operação do trilho. O evento que acorda
o change stream é a própria marcação, não uma escrita sintética numa coleção
paralela.

O backend nunca escreve em `payment_events`. Evento de pagamento é fato; incidente é
uma opinião sobre uma janela de eventos, e mora em outro lugar.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from pymongo.errors import DuplicateKeyError

from ..config import CURRENCY
from .client import client, db, with_retry


def abrir(provedor_id: str, canal: str, z_recusa: float, z_p99: float, janelas: int,
          taxa_recusa: float, p99_ms: float, eventos: int, aberto_por: str,
          nota: str | None = None) -> dict:
    d = db()
    prov = d.provedores.find_one({"provedor_id": provedor_id})
    if not prov:
        raise ValueError(f"provedor {provedor_id} não existe")
    if prov.get("em_incidente"):
        raise DuplicateKeyError(f"provedor {provedor_id} já está em incidente")

    incidente_id = f"INC-{uuid.uuid4().hex[:10].upper()}"
    agora = datetime.now(timezone.utc)

    doc = {
        "incident_id": incidente_id,
        "provedor_id": provedor_id,
        "canal": canal,
        "status": "aberto",
        "opened_at": agora,
        "opened_by": aberto_por,
        "note": nota,
        "evidencia": {
            "z_recusa": round(z_recusa, 2),
            "z_p99": round(z_p99, 2),
            "janelas_seguidas": janelas,
            "taxa_recusa_pct": round(taxa_recusa, 3),
            "p99_ms": round(p99_ms, 1),
            "eventos_na_janela": eventos,
            "recusa_base_cadastro_pct": round(prov.get("recusa_base", 0) * 100, 3),
            "sla_p99_ms": prov.get("sla_p99_ms"),
        },
        "moeda": CURRENCY,
    }

    def executar():
        with client().start_session() as sessao:
            with sessao.start_transaction():
                resultado = d.provedores.update_one(
                    {"provedor_id": provedor_id, "em_incidente": {"$ne": True}},
                    {"$set": {"em_incidente": True, "incident_id": incidente_id,
                              "flagged_at": agora}},
                    session=sessao)
                # A checagem em memória no início da função é só fast-path: entre ela e
                # aqui, outra chamada concorrente pode ter vencido a corrida e já
                # marcado `em_incidente`. O filtro `$ne: True` não casa nesse caso — sem
                # olhar `modified_count`, o código seguia e inseria um segundo
                # documento de incidente "aberto" para o mesmo provedor.
                if resultado.modified_count == 0:
                    raise DuplicateKeyError(
                        f"provedor {provedor_id} já está em incidente")
                # insert_many/insert_one injeta _id no dicionário original; devolver
                # isso rende ObjectId não serializável e um 500 depois do commit.
                d.incidents.insert_one(dict(doc), session=sessao)
                # A marcação é o evento. O listener observa `incidents`, não a coleção
                # time series: lá o change stream dispara por evento de pagamento.
                d.provedores.update_one(
                    {"provedor_id": provedor_id},
                    {"$set": {"last_event": {"kind": "incidente_aberto",
                                             "incident_id": incidente_id, "at": agora}}},
                    session=sessao)
        return doc

    return with_retry(executar)


def encerrar(incidente_id: str, desfecho: str, por: str) -> dict | None:
    d = db()
    agora = datetime.now(timezone.utc)

    def executar():
        with client().start_session() as sessao:
            with sessao.start_transaction():
                inc = d.incidents.find_one_and_update(
                    {"incident_id": incidente_id, "status": "aberto"},
                    {"$set": {"status": "encerrado", "outcome": desfecho,
                              "closed_at": agora, "closed_by": por}},
                    return_document=True, session=sessao)
                if not inc:
                    return None
                d.provedores.update_one({"provedor_id": inc["provedor_id"]},
                                        {"$set": {"em_incidente": False},
                                         "$unset": {"incident_id": ""}},
                                        session=sessao)
                inc.pop("_id", None)
                return inc

    return with_retry(executar)


def recentes(limite: int = 50) -> list[dict]:
    return with_retry(lambda: list(db().incidents.find({}, {"_id": 0})
                                   .sort("opened_at", -1).limit(limite)))


def reset_demo() -> dict:
    """Volta o cenário ao estado inicial. O roteiro roda mais de uma vez por dia."""
    d = db()
    inc = d.incidents.delete_many({})
    prov = d.provedores.update_many({"em_incidente": True},
                                    {"$set": {"em_incidente": False},
                                     "$unset": {"incident_id": "", "last_event": "",
                                                "flagged_at": ""}})
    alertas = d.incident_alerts.delete_many({})
    return {"incidentes_removidos": inc.deleted_count,
            "provedores_liberados": prov.modified_count,
            "alertas_removidos": alertas.deleted_count}
