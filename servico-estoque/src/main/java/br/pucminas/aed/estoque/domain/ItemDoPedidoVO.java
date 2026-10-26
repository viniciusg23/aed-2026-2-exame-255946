package br.pucminas.aed.estoque.domain;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Um item, do ponto de vista de quem reserva estoque.
 *
 * POR QUE O SUFIXO VO (Value Object)
 * Este item nao tem identidade: dois itens com o mesmo sku e a mesma quantidade
 * sao intercambiaveis, e nao ha por que saber "qual e qual". Ele e definido
 * pelos seus valores, e imutavel e nunca e persistido.
 *
 * O contraste esta na classe ao lado: PedidoConfirmadoEvent TEM identidade — o
 * eventoId, que e justamente a chave da deduplicacao. Esse par serve de regra
 * de bolso: se dois exemplares com os mesmos valores sao intercambiaveis, e
 * Value Object; se voce precisa saber qual e qual, e Entity.
 *
 * O QUE FALTA AQUI, DE PROPOSITO
 * O item publicado tem sku, quantidade e preco. Aqui o preco nao existe:
 * reservar estoque nao depende de dinheiro. Se um dia o preco mudar de formato
 * — virar objeto com moeda, por exemplo — este servico nao percebe e nao
 * quebra, porque nunca dependeu dele.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class ItemDoPedidoVO {

    private final String sku;
    private final int quantidade;

    @JsonCreator
    public ItemDoPedidoVO(@JsonProperty("sku") String sku,
                          @JsonProperty("quantidade") int quantidade) {
        this.sku = Objects.requireNonNull(sku, "sku e obrigatorio");
        this.quantidade = quantidade;
    }

    public String getSku() {
        return sku;
    }

    public int getQuantidade() {
        return quantidade;
    }

    /**
     * Igualdade POR VALOR, e nao por referencia — e o que caracteriza um Value
     * Object. Sem isto, dois itens identicos seriam considerados diferentes.
     */
    @Override
    public boolean equals(Object outro) {
        if (this == outro) {
            return true;
        }
        if (!(outro instanceof ItemDoPedidoVO)) {
            return false;
        }
        ItemDoPedidoVO aquele = (ItemDoPedidoVO) outro;
        return this.quantidade == aquele.quantidade && this.sku.equals(aquele.sku);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sku, Integer.valueOf(quantidade));
    }

    @Override
    public String toString() {
        return "ItemDoPedidoVO{sku=" + sku + ", quantidade=" + quantidade + "}";
    }
}
