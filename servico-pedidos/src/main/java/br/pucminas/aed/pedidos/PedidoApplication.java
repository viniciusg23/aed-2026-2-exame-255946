package br.pucminas.aed.pedidos;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Servico de Pedidos: recebe o pedido, publica o fato, acabou.
 *
 * Nao conhece o servico-estoque. Nao sabe se ele existe, quantos sao, ou se
 * estao de pe. Publica no topico e devolve 202.
 */
@SpringBootApplication
public class PedidoApplication {

    public static void main(String[] args) {
        SpringApplication.run(PedidoApplication.class, args);
    }
}
