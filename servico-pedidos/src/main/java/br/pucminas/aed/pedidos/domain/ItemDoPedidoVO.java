package br.pucminas.aed.pedidos.domain;

import java.math.BigDecimal;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Um item dentro do evento PedidoConfirmadoEvent.
 *
 * Escrito como classe imutavel classica, e nao como record, por decisao
 * didatica: aqui todo mecanismo fica visivel. O record esconde o construtor,
 * os acessores, o equals e o hashCode — comodo para quem ja sabe, opaco para
 * quem esta aprendendo o que torna um evento imutavel.
 *
 * Imutabilidade, neste caso, e o conjunto de:
 *   - campos private final;
 *   - atribuicao unica, no construtor;
 *   - nenhum metodo que altere estado (nao existe setter);
 *   - a classe e final, para ninguem herdar e introduzir estado mutavel.
 */
public final class ItemDoPedidoVO {

    private final String sku;
    private final int quantidade;
    private final BigDecimal preco;

    @JsonCreator
    public ItemDoPedidoVO(@JsonProperty("sku") String sku,
                        @JsonProperty("quantidade") int quantidade,
                        @JsonProperty("preco") BigDecimal preco) {
        this.sku = Objects.requireNonNull(sku, "sku e obrigatorio");
        this.quantidade = quantidade;
        this.preco = Objects.requireNonNull(preco, "preco e obrigatorio");
    }

    public String getSku() {
        return sku;
    }

    public int getQuantidade() {
        return quantidade;
    }

    public BigDecimal getPreco() {
        return preco;
    }

    @Override
    public boolean equals(Object outro) {
        if (this == outro) {
            return true;
        }
        if (!(outro instanceof ItemDoPedidoVO)) {
            return false;
        }
        ItemDoPedidoVO aquele = (ItemDoPedidoVO) outro;
        return this.quantidade == aquele.quantidade
                && this.sku.equals(aquele.sku)
                && this.preco.compareTo(aquele.preco) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sku, quantidade, preco);
    }

    @Override
    public String toString() {
        return "ItemDoPedidoVO{sku=" + sku + ", quantidade=" + quantidade + ", preco=" + preco + "}";
    }
}
