package br.pucminas.aed.pedidos.controller;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import br.pucminas.aed.pedidos.domain.PedidoConfirmadoEvent;
import br.pucminas.aed.pedidos.service.PedidoService;

/**
 * A API de pedidos: recebe o pedido e afirma o fato. Mais nada.
 *
 * Repare no que este controlador NAO faz. Ele nao reserva estoque, nao autoriza
 * pagamento, nao emite nota e nao espera ninguem. Publica o evento e devolve.
 * E o slide do case motivador em codigo: a API deixou de ORQUESTRAR e passou a
 * AFIRMAR um fato.
 *
 * Por isso a resposta e 202 Accepted, e nao 200 OK. Sao coisas diferentes:
 *   200 OK       "terminei, esta feito"
 *   202 Accepted "recebi e aceitei; vai acontecer, mas ainda nao aconteceu"
 *
 * O 202 e honesto com o cliente sobre a consistencia eventual. Prometer 200
 * aqui seria mentir: no instante da resposta, o estoque ainda nao foi reservado
 * — e o servico que reserva pode nem estar rodando.
 */
@RestController
@RequestMapping("/pedidos")
public class PedidoController {

    private final PedidoService pedidoService;

    public PedidoController(PedidoService pedidoService) {
        this.pedidoService = pedidoService;
    }

    /**
     * O evento chega pronto no corpo da requisicao e o Spring o desserializa
     * direto em PedidoConfirmadoEvent, pelo construtor anotado com @JsonCreator.
     * Faltando campo obrigatorio, a resposta e 400: o contrato do evento e
     * validado na entrada.
     *
     * Reentrega, na demonstracao, e repetir este mesmo comando — o eventoId
     * esta no arquivo JSON, entao e literalmente o mesmo evento.
     */
    @PostMapping("/confirmados")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void confirmarPedido(@RequestBody PedidoConfirmadoEvent evento) {
        pedidoService.confirmar(evento);
    }
}
