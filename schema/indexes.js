// Índices do PoV. Idempotente: createIndex é no-op se o índice já existe.
// Rodar com: mongosh "$MONGODB_URI" schema/indexes.js
const dbName = process.env.MONGODB_DB || "trilho_pagamentos";
const d = db.getSiblingDB(dbName);

// --- telemetria ---------------------------------------------------------------
// Índice em coleção time series indexa buckets, não eventos. As duas primeiras
// formas cobrem a rota; a terceira é a que responde a objeção de cardinalidade.
d.payment_events.createIndex({ "meta.provedor": 1, ts: 1 });
d.payment_events.createIndex({ "meta.canal": 1, ts: 1 });

// conta_id é CAMPO DE MEDIÇÃO, não metaField. São milhões de contas: no metaField
// cada uma vira uma série própria. Com índice secundário sobre o campo, o velocity
// da conta é uma consulta pontual. Ver docs/adr/0002-cardinalidade.md.
d.payment_events.createIndex({ conta_id: 1, ts: 1 });

// --- cadastro -----------------------------------------------------------------
d.provedores.createIndex({ provedor_id: 1 }, { unique: true });
d.provedores.createIndex({ canal: 1 });
d.provedores.createIndex({ em_incidente: 1 }, { sparse: true });
d.degradation_scenarios.createIndex({ provedor_id: 1, kind: 1 }, { unique: true });
d.demo_accounts.createIndex({ conta_id: 1 }, { unique: true });

// --- incidentes ---------------------------------------------------------------
d.incidents.createIndex({ provedor_id: 1, status: 1 });
d.incidents.createIndex({ opened_at: -1 });
d.incident_alerts.createIndex({ at: -1 });

// Deliberadamente ausentes:
//  - índice em `valor` ou `latencia_ms`: "todo evento acima de X" não é pergunta
//    deste workload e o índice teria o tamanho do dado que indexa.
//  - {meta.uf, ts}: o corte por UF sempre acompanha canal ou provedor, que já
//    prefixam um índice existente.

print("índices em " + dbName + ":");
["payment_events", "provedores", "incidents"].forEach(function (c) {
  print("  " + c + ": " + d[c].getIndexes().map(function (i) { return i.name; }).join(", "));
});
