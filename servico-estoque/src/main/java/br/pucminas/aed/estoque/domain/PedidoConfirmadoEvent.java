package br.pucminas.aed.estoque.domain;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A visao QUE ESTE SERVICO TEM do evento PedidoConfirmado.
 *
 * ISTO NAO E DUPLICACAO POR DESCUIDO. E o ponto da aula.
 *
 * O servico-pedidos tem a classe dele; este tem a sua. Nao ha JAR
 * compartilhado entre os dois — se houvesse, publicar uma versao nova da classe
 * obrigaria a recompilar e reimplantar TODOS os consumidores, o que recria
 * exatamente o acoplamento de compilacao que os eventos deveriam eliminar.
 * O contrato entre os dois servicos e o JSON que trafega no topico, e so.
 *
 * CONSUMIDOR TOLERANTE: repare no que falta aqui. O evento publicado tem
 * clienteId, valorTotal e moeda; esta classe nao os declara, porque reservar
 * estoque nao depende deles. Campos desconhecidos sao ignorados
 * (@JsonIgnoreProperties). E isso que permite ao produtor acrescentar campos
 * sem quebrar ninguem — a compatibilidade para frente do slide de evolucao de
 * contrato.
 *
 * Declare apenas o que voce usa. Cada campo declarado vira uma dependencia sua
 * sobre o formato alheio.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class PedidoConfirmadoEvent {

    private final String eventoId;
    private final Instant ocorridoEm;
    private final String pedidoId;
    private final List<ItemDoPedidoVO> itens;

    @JsonCreator
    public PedidoConfirmadoEvent(@JsonProperty("eventoId") String eventoId,
                            @JsonProperty("ocorridoEm") Instant ocorridoEm,
                            @JsonProperty("pedidoId") String pedidoId,
                            @JsonProperty("itens") List<ItemDoPedidoVO> itens) {

        this.eventoId = Objects.requireNonNull(eventoId, "eventoId e obrigatorio");
        this.ocorridoEm = ocorridoEm;
        this.pedidoId = Objects.requireNonNull(pedidoId, "pedidoId e obrigatorio");

        List<ItemDoPedidoVO> copia = new ArrayList<ItemDoPedidoVO>();
        if (itens != null) {
            copia.addAll(itens);
        }
        this.itens = Collections.unmodifiableList(copia);
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

    public List<ItemDoPedidoVO> getItens() {
        return itens;
    }

    @Override
    public String toString() {
        return "PedidoConfirmadoEvent{eventoId=" + eventoId
                + ", pedidoId=" + pedidoId
                + ", itens=" + itens.size() + "}";
    }
}
