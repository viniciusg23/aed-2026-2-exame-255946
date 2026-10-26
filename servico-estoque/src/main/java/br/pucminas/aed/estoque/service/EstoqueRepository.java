package br.pucminas.aed.estoque.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import br.pucminas.aed.estoque.domain.ItemDoPedidoVO;

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

    /**
     * O efeito de negocio: baixar a quantidade reservada.
     *
     * Devolve o numero de linhas afetadas. Zero quer dizer que o SKU nao existe
     * — o UPDATE nao reclama disso, entao quem decide o que fazer e o service.
     *
     * @return 1 se o SKU existe e foi debitado; 0 se o SKU nao existe.
     */
    public int reservar(String sku, int quantidade) {
        return jdbc.update("UPDATE estoque SET quantidade = quantidade - ? WHERE sku = ?",
                Integer.valueOf(quantidade), sku);
    }

    /**
     * Registra o que foi debitado para o pedido. E o que permite, depois,
     * devolver exatamente isso. So INSERT: a reserva e historico.
     */
    public void registrarReserva(String pedidoId, String sku, int quantidade, String eventoId) {
        jdbc.update("INSERT INTO reserva (pedido_id, sku, quantidade, evento_id) VALUES (?, ?, ?, ?)",
                pedidoId, sku, Integer.valueOf(quantidade), eventoId);
    }

    /**
     * O que foi reservado para o pedido, somado por SKU; lista vazia se nada
     * foi reservado.
     */
    public List<ItemDoPedidoVO> reservasDoPedido(String pedidoId) {
        return jdbc.query("SELECT sku, SUM(quantidade) AS quantidade FROM reserva"
                        + " WHERE pedido_id = ? GROUP BY sku ORDER BY sku",
                new ItemReservadoMapper(), pedidoId);
    }

    /** A compensacao: a quantidade reservada volta ao estoque. */
    public void devolver(String sku, int quantidade) {
        jdbc.update("UPDATE estoque SET quantidade = quantidade + ? WHERE sku = ?",
                Integer.valueOf(quantidade), sku);
    }

    // ------------------------------------------------------------------
    // pagamento recusado
    // ------------------------------------------------------------------

    /**
     * Mesmo mecanismo do registrarEventoSeNovo, com a chave no pedido.
     *
     * @return true se e a primeira recusa deste pedido; false se o pedido ja
     *         teve a recusa tratada antes.
     */
    public boolean registrarRecusaSeNova(String pedidoId, String eventoId, String motivo,
                                         boolean reservaDevolvida) {
        int linhas = jdbc.update(
                "INSERT INTO pagamento_recusado (pedido_id, evento_id, motivo, reserva_devolvida)"
                        + " VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
                pedidoId, eventoId, motivo, Boolean.valueOf(reservaDevolvida));
        return linhas == 1;
    }

    public boolean pagamentoFoiRecusado(String pedidoId) {
        Long total = jdbc.queryForObject(
                "SELECT COUNT(*) FROM pagamento_recusado WHERE pedido_id = ?", Long.class, pedidoId);
        return total != null && total.longValue() > 0L;
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

    /** Converte uma linha da tabela reserva no item que foi reservado. */
    private static class ItemReservadoMapper implements RowMapper<ItemDoPedidoVO> {
        @Override
        public ItemDoPedidoVO mapRow(ResultSet linha, int numero) throws SQLException {
            return new ItemDoPedidoVO(linha.getString("sku"), linha.getInt("quantidade"));
        }
    }
}
