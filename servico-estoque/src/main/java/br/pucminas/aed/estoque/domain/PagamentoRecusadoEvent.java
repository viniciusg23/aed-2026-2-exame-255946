package br.pucminas.aed.estoque.domain;

import java.time.Instant;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A visao QUE ESTE SERVICO TEM do evento PagamentoRecusado.
 *
 * Mesma regra do PedidoConfirmadoEvent: classe propria, sem JAR compartilhado
 * com quem publica, e consumidor tolerante — campos desconhecidos sao
 * ignorados.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
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
        this.ocorridoEm = ocorridoEm;
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
        return "PagamentoRecusadoEvent{eventoId=" + eventoId + ", pedidoId=" + pedidoId + "}";
    }
}
