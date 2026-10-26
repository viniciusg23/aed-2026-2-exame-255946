package br.pucminas.aed.estoque.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Toda a persistencia deste servico, num lugar so.
 *
 * Sao duas tabelas com papeis bem diferentes, e vale separa-las na cabeca do
 * aluno:
 *
 *   evento_processado  memoria do que ja foi visto — sustenta a IDEMPOTENCIA
 *   estoque            o efeito de negocio — e o que nao pode acontecer 2 vezes
 *
 * Elas moram na mesma classe porque precisam morar na mesma transacao. E
 * justamente isso que fecha a janela em que o processo morreria entre registrar
 * a chave e aplicar o efeito.
 *
 * Repare que os metodos recebem sku e quantidade, e nao o evento inteiro. Um
 * repositorio nao deveria saber percorrer a lista de itens de um evento: isso e
 * orquestracao, e fica no EstoqueService.
 */
@Repository
public class EstoqueRepository {

    private final JdbcTemplate jdbc;

    public EstoqueRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    // ------------------------------------------------------------------
    // deduplicacao
    // ------------------------------------------------------------------

    /**
     * INSERT ... ON CONFLICT DO NOTHING devolve 1 quando a linha e nova e 0
     * quando ja existia. E isso, e so isso, que distingue "primeira vez" de
     * "reentrega".
     *
     * @return true se o evento e novo; false se ja foi processado antes.
     */
    public boolean registrarEventoSeNovo(String eventoId) {
        int linhas = jdbc.update(
                "INSERT INTO evento_processado (evento_id) VALUES (?) ON CONFLICT DO NOTHING",
                eventoId);
        return linhas == 1;
    }

    public long contarEventosProcessados() {
        Long total = jdbc.queryForObject("SELECT COUNT(*) FROM evento_processado", Long.class);
        if (total == null) {
            return 0L;
        }
        return total.longValue();
    }

    public void limparEventosProcessados() {
        jdbc.update("DELETE FROM evento_processado");
    }

    // ------------------------------------------------------------------
    // estoque
    // ------------------------------------------------------------------

    /** O efeito de negocio: baixar a quantidade reservada. */
    public void reservar(String sku, int quantidade) {
        jdbc.update("UPDATE estoque SET quantidade = quantidade - ? WHERE sku = ?",
                Integer.valueOf(quantidade), sku);
    }

    public int quantidadeEmEstoque(String sku) {
        Integer quantidade = jdbc.queryForObject(
                "SELECT quantidade FROM estoque WHERE sku = ?", Integer.class, sku);
        if (quantidade == null) {
            return 0;
        }
        return quantidade.intValue();
    }

    /** Usado para reiniciar a demonstracao e nos testes. */
    public void redefinirEstoque(String sku, int quantidade) {
        jdbc.update("UPDATE estoque SET quantidade = ? WHERE sku = ?",
                Integer.valueOf(quantidade), sku);
    }
}
