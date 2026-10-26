-- A tabela que sustenta a idempotência do consumidor.
-- A chave primaria e o id do envelope CloudEvents (ce_id).
CREATE TABLE IF NOT EXISTS evento_processado (
  evento_id     VARCHAR(64) PRIMARY KEY,
  processado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Em producao, defina a retencao da chave. Sete dias costumam cobrir
-- retry e replay operacional:
--   DELETE FROM evento_processado WHERE processado_em < now() - INTERVAL '7 days';

CREATE TABLE IF NOT EXISTS estoque (
  sku        VARCHAR(32) PRIMARY KEY,
  quantidade INT NOT NULL
);

INSERT INTO estoque (sku, quantidade) VALUES ('TEC-002', 100) ON CONFLICT DO NOTHING;
