package br.pucminas.aed.estoque.domain;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A reserva de estoque do pedido foi cancelada: a quantidade voltou ao
 * estoque.
 *
 * E a COMPENSACAO, e ela e um fato novo. A reserva aconteceu e continua
 * registrada; a devolucao tambem aconteceu, e fica registrada ao lado. Nada e
 * apagado — nem a linha da reserva, nem o evento do topico.
 *
 * O nome segue a regra de sempre: participio, linguagem do dominio, fato
 * consumado. Nao "DesfazerReserva" (comando) nem "ReservaDeletada"
 * (vocabulario de banco de dados).
 *
 * pagamentoRecusadoId e a CAUSA: o ce_id do PagamentoRecusado que provocou a
 * compensacao. Quem le o topico consegue ligar um fato ao outro.
 */
public final class ReservaDeEstoqueCanceladaEvent {

    private final String eventoId;
    private final Instant ocorridoEm;
    private final String pedidoId;
    private final String pagamentoRecusadoId;
    private final List<ItemDoPedidoVO> itens;

    public ReservaDeEstoqueCanceladaEvent(String eventoId,
                                          Instant ocorridoEm,
                                          String pedidoId,
                                          String pagamentoRecusadoId,
                                          List<ItemDoPedidoVO> itens) {
        this.eventoId = Objects.requireNonNull(eventoId, "eventoId e obrigatorio");
        this.ocorridoEm = Objects.requireNonNull(ocorridoEm, "ocorridoEm e obrigatorio");
        this.pedidoId = Objects.requireNonNull(pedidoId, "pedidoId e obrigatorio");
        this.pagamentoRecusadoId = pagamentoRecusadoId;
        this.itens = Collections.unmodifiableList(new ArrayList<ItemDoPedidoVO>(itens));
    }

    public String getEventoId() {
        return eventoId;
    }

    public Instant getOcorridoEm() {
        return ocorridoEm;
    }

    public String getPedidoId() {
        return pedidoId;
    }

    public String getPagamentoRecusadoId() {
        return pagamentoRecusadoId;
    }

    public List<ItemDoPedidoVO> getItens() {
        return itens;
    }

    @Override
    public String toString() {
        return "ReservaDeEstoqueCanceladaEvent{eventoId=" + eventoId
                + ", pedidoId=" + pedidoId
                + ", itens=" + itens.size() + "}";
    }
}
