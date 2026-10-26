package br.pucminas.aed.estoque.service;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import br.pucminas.aed.estoque.domain.ItemDoPedidoVO;
import br.pucminas.aed.estoque.domain.PagamentoRecusadoEvent;
import br.pucminas.aed.estoque.domain.ReservaDeEstoqueCanceladaEvent;

/**
 * A compensacao: o pagamento foi recusado, e a reserva de estoque do pedido e
 * devolvida.
 *
 * COREOGRAFIA, NAO ORQUESTRACAO. Ninguem manda o estoque desfazer nada; ele
 * reage ao fato PagamentoRecusado e afirma um fato novo,
 * ReservaDeEstoqueCancelada. Nao ha chamada HTTP entre os servicos.
 *
 * TUDO NUMA TRANSACAO SO, NESTA ORDEM:
 *
 *   1. deduplica pelo ce_id da recusa (reentrega do mesmo evento);
 *   2. registra a recusa com chave no PEDIDO (outra recusa do mesmo pedido);
 *   3. devolve a quantidade de cada item reservado;
 *   4. publica ReservaDeEstoqueCancelada e ESPERA a confirmacao do broker;
 *   5. commit — e so depois o listener confirma o offset.
 *
 * Se o processo cair entre 3 e 4, ou durante 4, a transacao nao foi
 * confirmada: o banco desfaz a devolucao e o Kafka entrega a recusa de novo.
 * O caso que sobra e o inverso — publicado, e o commit falha. Nesse caso a
 * reentrega publica de novo, com o MESMO ce_id (ver idDaCompensacao), e quem
 * consome deduplica.
 */
@Service
public class CompensacaoService {

    private static final Logger log = LoggerFactory.getLogger(CompensacaoService.class);

    private final EstoqueRepository repositorio;
    private final ReservaCanceladaPublicador publicador;

    public CompensacaoService(EstoqueRepository repositorio, ReservaCanceladaPublicador publicador) {
        this.repositorio = repositorio;
        this.publicador = publicador;
    }

    /**
     * @return true se a reserva foi devolvida e o evento publicado; false se
     *         era duplicata ou se nao havia reserva a devolver.
     */
    @Transactional
    public boolean compensar(String eventoId, PagamentoRecusadoEvent recusa) {
        String pedidoId = recusa.getPedidoId();

        if (!repositorio.registrarEventoSeNovo(eventoId)) {
            log.info("recusa {} JA PROCESSADA, descartando em silencio", eventoId);
            return false;
        }

        List<ItemDoPedidoVO> reservado = repositorio.reservasDoPedido(pedidoId);
        boolean haReserva = !reservado.isEmpty();

        if (!repositorio.registrarRecusaSeNova(pedidoId, eventoId, recusa.getMotivo(), haReserva)) {
            log.info("pedido {} ja teve a recusa tratada; recusa {} nao devolve de novo", pedidoId, eventoId);
            return false;
        }

        if (!haReserva) {
            // Recusa sem reserva: chegou antes do PedidoConfirmado, ou o pedido
            // nunca reservou (foi para a DLQ, por exemplo). Nao ha o que
            // devolver e nao ha reserva cancelada para afirmar. A recusa fica
            // registrada; se o PedidoConfirmado chegar depois, o EstoqueService
            // nao reserva.
            log.warn("recusa {} do pedido {} sem reserva: registrada, nada a devolver", eventoId, pedidoId);
            return false;
        }

        for (ItemDoPedidoVO item : reservado) {
            repositorio.devolver(item.getSku(), item.getQuantidade());
        }

        ReservaDeEstoqueCanceladaEvent cancelada = new ReservaDeEstoqueCanceladaEvent(
                idDaCompensacao(pedidoId), Instant.now(), pedidoId, eventoId, reservado);
        publicador.publicar(cancelada);

        log.info("reserva devolvida  pedido={}  recusa={}  itens={}", pedidoId, eventoId, reservado);
        return true;
    }

    /**
     * O ce_id do ReservaDeEstoqueCancelada e DERIVADO do pedido, e nao
     * sorteado: um pedido tem no maximo uma reserva cancelada. Se a mesma
     * compensacao for publicada duas vezes (publicou, e o commit falhou), as
     * duas copias tem o mesmo ce_id, e o consumidor descarta a segunda.
     */
    static String idDaCompensacao(String pedidoId) {
        String nome = "loja.estoque.reservacancelada.v1/" + pedidoId;
        return UUID.nameUUIDFromBytes(nome.getBytes(UTF_8)).toString();
    }
}
