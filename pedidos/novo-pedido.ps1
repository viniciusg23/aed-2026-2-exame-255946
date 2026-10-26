<#
.SYNOPSIS
Gera um arquivo de pedido para a demonstracao da aula 01.

.DESCRIPTION
Isto NAO e uma fabrica dentro da aplicacao: e so um gerador de arquivo JSON,
para a hora em que voce precisa de varios pedidos DIFERENTES (o passo 5 do
roteiro, o do desacoplamento temporal). O evento continua chegando ao servico
por HTTP, no corpo da requisicao, e sendo desserializado direto na classe de
dominio.

O que faz um pedido ser "outro" e o eventoId, nao o pedidoId: a deduplicacao
olha o eventoId. Por isso ele e sorteado a cada chamada.

.EXAMPLE
.\pedidos\novo-pedido.ps1 -PedidoId 4713
Gera pedidos/pedido-4713.json com um eventoId novo.

.EXAMPLE
.\pedidos\novo-pedido.ps1 -PedidoId 4714 -Quantidade 3 -Publicar
Gera e ja publica em localhost:8080.

.EXAMPLE
.\pedidos\novo-pedido.ps1 -PedidoId 4715 -EventoId 11111111-1111-1111-1111-111111111111
Repete um eventoId de proposito, para demonstrar o descarte.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string] $PedidoId,

    [string] $Sku = "TEC-002",
    [int]    $Quantidade = 1,
    [double] $Preco = 289.90,
    [string] $ClienteId = "c-9931",

    # sorteado por padrao; informe um valor para repetir um evento de proposito
    [string] $EventoId = [guid]::NewGuid().ToString(),

    [string] $Url = "localhost:8080/pedidos/confirmados",
    [switch] $Publicar
)

$ErrorActionPreference = "Stop"

$pasta = $PSScriptRoot
$arquivo = Join-Path $pasta "pedido-$PedidoId.json"

# ISO-8601 com fuso, nunca epoch: o contrato no fio precisa ser neutro de
# linguagem. Ver a secao "O contrato do evento no fio" no CLAUDE.md.
$ocorridoEm = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
$total = [math]::Round($Preco * $Quantidade, 2)

# Formatacao com InvariantCulture: nesta maquina o separador decimal e a
# virgula, e "289,90" nao e JSON valido.
$inv = [System.Globalization.CultureInfo]::InvariantCulture
$precoJson = $Preco.ToString("0.00", $inv)
$totalJson = $total.ToString("0.00", $inv)

$json = @"
{
  "eventoId":   "$EventoId",
  "ocorridoEm": "$ocorridoEm",
  "pedidoId":   "$PedidoId",
  "clienteId":  "$ClienteId",
  "valorTotal": $totalJson,
  "moeda":      "BRL",
  "itens": [
    { "sku": "$Sku", "quantidade": $Quantidade, "preco": $precoJson }
  ]
}
"@

Set-Content -Path $arquivo -Value $json -Encoding utf8
Write-Host "gerado: $arquivo" -ForegroundColor Green
Write-Host $json

if ($Publicar) {
    $relativo = "pedidos/pedido-$PedidoId.json"
    Push-Location (Split-Path $pasta -Parent)
    try {
        # curl.exe com o .exe: no PowerShell, "curl" e apelido de
        # Invoke-WebRequest e nao entende -X, -d nem @arquivo.
        curl.exe -s -w "`nHTTP %{http_code}`n" -X POST $Url `
                 -H "Content-Type: application/json" -d "@$relativo"
    }
    finally {
        Pop-Location
    }
}
