package br.pucminas.aed.estoque;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Servico de Estoque: consome PedidoConfirmado e reserva o estoque.
 *
 * Nao expoe HTTP. Sobe, assina o topico e fica esperando. Pode ser derrubado e
 * subido a vontade — os eventos publicados enquanto ele esteve fora continuam
 * no log do broker, e ele os processa ao voltar, a partir do offset do grupo.
 *
 * E esse o experimento mais convincente da aula 01: derrube este servico,
 * publique varios pedidos, suba de novo e veja o estoque cair de uma vez.
 */
@SpringBootApplication
public class EstoqueApplication {

    public static void main(String[] args) {
        SpringApplication.run(EstoqueApplication.class, args);
    }
}
