package br.pucminas.aed.pedidos.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * O EVENTO: registro imutavel de um fato do dominio que ja aconteceu.
 *
 * Repare no que NAO esta aqui: nenhuma anotacao de JPA nem de Kafka. As unicas
 * anotacoes sao de serializacao, e existem para tornar EXPLICITO o mapeamento
 * entre o JSON que trafega e os campos da classe. O contrato do evento e de
 * dominio, nao de infraestrutura: trocar Kafka por Pulsar nao mexe neste
 * arquivo.
 *
 * O QUE GARANTE A IMUTABILIDADE
 *   - todos os campos private final, atribuidos so no construtor;
 *   - a lista de itens e copiada na entrada e devolvida como somente-leitura,
 *     senao quem passou a lista original poderia altera-la depois;
 *   - nao existe setter, e a classe e final.
 *
 * Um evento mutavel nao e um evento: e um registro que alguem pode reescrever
 * depois do fato ter acontecido.
 */
public final class PedidoConfirmadoEvent {

    private final String eventoId;
    private final Instant ocorridoEm;
    private final String pedidoId;
    private final String clienteId;
    private final BigDecimal valorTotal;
    private final String moeda;
    private final List<ItemDoPedidoVO> itens;

    @JsonCreator
    public PedidoConfirmadoEvent(@JsonProperty("eventoId") String eventoId,
                            @JsonProperty("ocorridoEm") Instant ocorridoEm,
                            @JsonProperty("pedidoId") String pedidoId,
                            @JsonProperty("clienteId") String clienteId,
                            @JsonProperty("valorTotal") BigDecimal valorTotal,
                            @JsonProperty("moeda") String moeda,
                            @JsonProperty("itens") List<ItemDoPedidoVO> itens) {

        this.eventoId = Objects.requireNonNull(eventoId, "eventoId e obrigatorio");
        this.ocorridoEm = Objects.requireNonNull(ocorridoEm, "ocorridoEm e obrigatorio");
        this.pedidoId = Objects.requireNonNull(pedidoId, "pedidoId e obrigatorio");
        this.clienteId = clienteId;
        this.valorTotal = valorTotal;
        this.moeda = moeda;

        // copia defensiva na ENTRADA: a lista recebida pode mudar depois
        List<ItemDoPedidoVO> copia = new ArrayList<ItemDoPedidoVO>();
        if (itens != null) {
            copia.addAll(itens);
        }
        this.itens = Collections.unmodifiableList(copia);
    }

    /** id do envelope; e a chave que torna o consumidor idempotente. */
    public String getEventoId() {
        return eventoId;
    }

    /** quando o FATO aconteceu no dominio, e nao quando chegou ao broker. */
    public Instant getOcorridoEm() {
        return ocorridoEm;
    }

    /** identidade do agregado; vira a chave de particao no topico. */
    public String getPedidoId() {
        return pedidoId;
    }

    public String getClienteId() {
        return clienteId;
    }

    public BigDecimal getValorTotal() {
        return valorTotal;
    }

    public String getMoeda() {
        return moeda;
    }

    /** lista somente-leitura: tentar alterar lanca UnsupportedOperationException. */
    public List<ItemDoPedidoVO> getItens() {
        return itens;
    }

    @Override
    public boolean equals(Object outro) {
        if (this == outro) {
            return true;
        }
        if (!(outro instanceof PedidoConfirmadoEvent)) {
            return false;
        }
        PedidoConfirmadoEvent aquele = (PedidoConfirmadoEvent) outro;
        return this.eventoId.equals(aquele.eventoId);   // a identidade do evento e o seu id
    }

    @Override
    public int hashCode() {
        return eventoId.hashCode();
    }

    @Override
    public String toString() {
        return "PedidoConfirmadoEvent{eventoId=" + eventoId
                + ", pedidoId=" + pedidoId
                + ", ocorridoEm=" + ocorridoEm
                + ", itens=" + itens.size() + "}";
    }
}
