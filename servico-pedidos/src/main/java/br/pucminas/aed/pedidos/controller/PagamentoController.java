package br.pucminas.aed.pedidos.controller;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import br.pucminas.aed.pedidos.domain.PagamentoRecusadoEvent;
import br.pucminas.aed.pedidos.service.PagamentoService;

/**
 * Simulacao do pagamento: afirma a recusa. Existe porque esta atividade nao
 * tem servico de pagamento de verdade.
 *
 * 202 Accepted, como no /pedidos/confirmados: no instante da resposta o
 * estoque ainda nao devolveu nada.
 */
@RestController
@RequestMapping("/pagamentos")
public class PagamentoController {

    private final PagamentoService pagamentoService;

    public PagamentoController(PagamentoService pagamentoService) {
        this.pagamentoService = pagamentoService;
    }

    @PostMapping("/recusados")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void recusarPagamento(@RequestBody PagamentoRecusadoEvent evento) {
        pagamentoService.recusar(evento);
    }
}
