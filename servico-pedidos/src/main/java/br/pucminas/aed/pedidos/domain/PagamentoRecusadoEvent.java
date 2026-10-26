package br.pucminas.aed.pedidos.domain;

import java.time.Instant;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * O pagamento do pedido foi recusado.
 *
 * Fato consumado, no participio, na linguagem do dominio. Nao e um pedido para
 * ninguem "desfazer" nada: quem reservou estoque decide, sozinho, o que fazer
 * diante deste fato.
 *
 * Nesta atividade nao ha servico de pagamento de verdade; este evento e
 * publicado pelo endpoint POST /pagamentos/recusados, que simula a recusa.
 */
public final class PagamentoRecusadoEvent {

    private final String eventoId;
    private final Instant ocorridoEm;
    private final String pedidoId;
    private final String motivo;

    @JsonCreator
    public PagamentoRecusadoEvent(@JsonProperty("eventoId") String eventoId,
                                  @JsonProperty("ocorridoEm") Instant ocorridoEm,
                                  @JsonProperty("pedidoId") String pedidoId,
                                  @JsonProperty("motivo") String motivo) {
        this.eventoId = Objects.requireNonNull(eventoId, "eventoId e obrigatorio");
        this.ocorridoEm = Objects.requireNonNull(ocorridoEm, "ocorridoEm e obrigatorio");
        this.pedidoId = Objects.requireNonNull(pedidoId, "pedidoId e obrigatorio");
        this.motivo = motivo;
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

    public String getMotivo() {
        return motivo;
    }

    @Override
    public String toString() {
        return "PagamentoRecusadoEvent{eventoId=" + eventoId
                + ", pedidoId=" + pedidoId
                + ", motivo=" + motivo + "}";
    }
}
