# Sistema de troca de mensagens instantâneas

Projeto da disciplina CC7261 — Sistemas Distribuídos (FEI).

Implementação de um sistema inspirado em BBS/IRC, em que bots fazem login,
criam canais públicos e publicam mensagens neles, usando ZeroMQ para a troca
de mensagens e containers para a execução.

## Partes entregues

- **Parte 1** — login dos bots, criação e listagem de canais, com
  persistência em disco nos servidores.
- **Parte 2** — publicação em canais via Publisher-Subscriber, inscrição dos
  bots em tópicos e persistência das publicações.
- **Parte 3** — relógio lógico de Lamport em todas as mensagens e serviço de
  referência para rank, lista de servidores, heartbeat e sincronização do
  relógio físico.

## Arquitetura

Três caminhos independentes de comunicação:

```
Request-Reply (pedidos dos bots aos servidores)
bot (REQ) → broker:5555 (ROUTER) → (DEALER) broker:5556 → servidor (REP)

Publisher-Subscriber (publicações dos servidores aos bots)
servidor (PUB) → proxy:5557 (XSUB) → (XPUB) proxy:5558 → bot (SUB)

Serviço de referência (apenas servidores, sem passar pelo broker)
servidor (REQ) → referencia:5559 (REP)
```

O broker e o proxy são repassadores puros: nenhum dos dois desempacota o
conteúdo das mensagens. Por isso funcionam igual com bots e servidores
escritos em qualquer linguagem.

O socket DEALER do broker faz o balanceamento de carga entre os servidores em
round-robin, sem nenhuma lógica de distribuição escrita por nós. No proxy, o
`zmq.proxy()` repassa as publicações e também propaga as inscrições no sentido
inverso, do XPUB para o XSUB — sem isso os sockets PUB descartariam as
mensagens por não haver inscritos conhecidos.

Containers em execução: 1 broker, 1 proxy, 1 serviço de referência,
4 servidores (2 por integrante) e 4 bots (2 por integrante).

## Escolhas feitas

### Linguagens

JavaScript (Node.js) e Python. Cada integrante implementou cliente e
servidor completos na sua linguagem, e as quatro combinações se comunicam
entre si — o bot em Node conversa com o servidor em Python e vice-versa,
tanto no Request-Reply quanto no Pub/Sub.

O broker, o proxy e o serviço de referência foram escritos em Python por
serem componentes únicos e independentes das implementações de cliente e
servidor.

### Serialização: MessagePack

O enunciado exige serialização binária e proíbe JSON, XML ou texto simples
nas mensagens trafegadas.

Escolhemos MessagePack por não exigir schema compilado: as bibliotecas
`msgpack` (Python) e `@msgpack/msgpack` (Node) codificam dicionários
diretamente, o que mantém as duas implementações independentes sem passo de
geração de código no build.

O preço dessa escolha é que o contrato não é garantido pelo compilador,
como seria com Protobuf. Para compensar, o formato do envelope está
documentado abaixo e implementado em um módulo dedicado (`mensagem.js` e
`mensagem.py`) em cada linguagem.

### Formato das mensagens

Mesmo envelope para pedidos, respostas e publicações:

| campo | tipo | descrição |
|---|---|---|
| `tipo` | string | ação, tipo da resposta ou `publicacao` |
| `origem` | string | quem enviou |
| `timestamp` | float | epoch em segundos, UTC |
| `relogio` | int | relógio lógico de Lamport de quem envia |
| `status` | string | `ok` ou `erro`, apenas nas respostas |
| `dados` | map | conteúdo específico do tipo |

O campo `relogio` foi previsto desde a Parte 1, zerado, justamente para que a
adição do relógio lógico na Parte 3 não exigisse mudança no formato.

O timestamp é sempre em **segundos**. Em Node, `Date.now()` devolve
milissegundos, então o valor é dividido por 1000 antes do envio.

Operações implementadas:

| pedido | `dados` | resposta | `dados` |
|---|---|---|---|
| `login` | `{usuario}` | `login_resp` | `{mensagem}` |
| `criar_canal` | `{canal}` | `criar_canal_resp` | `{mensagem}` |
| `listar_canais` | `{}` | `listar_canais_resp` | `{canais: [...]}` |
| `publicar` | `{canal, mensagem}` | `publicar_resp` | `{mensagem}` |

### O tópico como frame separado

O filtro do socket SUB no ZeroMQ casa com o **prefixo dos bytes** da
mensagem. Como o conteúdo vai serializado em MessagePack, o nome do canal
não ficaria no início da mensagem e a filtragem por tópico não funcionaria.

Por isso as publicações são enviadas em duas partes:

1. frame 1 — nome do canal, em texto
2. frame 2 — envelope completo, em MessagePack

O SUB filtra pelo primeiro frame e desserializa o segundo. O payload continua
binário, como o enunciado exige; o frame do tópico é metadado de roteamento,
da mesma natureza do envelope de endereçamento que o próprio ZeroMQ já usa
nos sockets ROUTER.

### Relógio lógico

O relógio de Lamport é um contador por processo, implementado dentro do
próprio módulo `mensagem` em cada linguagem. As duas regras do enunciado
ficam embutidas nas funções que todo processo já usa:

- `criar()` incrementa o contador antes de montar o envelope;
- `desempacotar()` sincroniza o contador local com o máximo entre ele e o
  valor recebido.

Como consequência, bots, servidores e serviço de referência obedecem ao
relógio lógico sem nenhuma linha extra no código de cada um — basta usarem o
módulo. O log mostra os dois valores lado a lado: `relogio=` é o que veio na
mensagem e `local=` é o contador do processo depois da sincronização.

### Serviço de referência

Processo separado que conversa apenas com os servidores, nunca com os bots.
Diferente dos servidores, ele faz `bind` direto na porta 5559 e não passa
pelo broker, porque não há balanceamento a fazer: é um serviço único.

Operações:

| pedido | `dados` | resposta | `dados` |
|---|---|---|---|
| `rank` | `{servidor}` | `rank_resp` | `{rank}` |
| `list` | `{}` | `list_resp` | `{servidores: [{servidor, rank}]}` |
| `heartbeat` | `{servidor}` | `heartbeat_resp` | `{mensagem, hora}` |

Cada servidor pede o seu rank ao iniciar. Nomes repetidos recebem sempre o
mesmo rank, conforme o enunciado. A cada 10 mensagens de clientes recebidas,
o servidor envia um heartbeat e aproveita a resposta para obter a hora
correta.

Duas decisões de implementação:

**Remoção por tempo.** O enunciado pede que o servidor saia da lista se não
enviar heartbeat, mas não define o critério. Adotamos um limite de 30
segundos desde o último sinal, verificado a cada pedido recebido. Como o
socket REP é síncrono, essa verificação no momento do pedido dispensa uma
thread de vigilância separada.

**O ajuste do relógio físico é guardado, não aplicado.** Um container não
pode alterar o relógio do sistema do host, então cada servidor armazena a
diferença entre a hora recebida e a sua própria, e uma função
`hora_corrigida()` devolve o valor ajustado. É o procedimento usual em
sincronização de relógios e será reaproveitado na Parte 4, com o algoritmo
de Berkeley.

### Tratamento de erros

Respostas de erro usam `status: "erro"` e trazem a descrição em
`dados.mensagem`. Os casos tratados:

- nome de usuário vazio
- nome de canal inválido (regra: `a-z`, `0-9`, `-`, `_`, de 3 a 20 caracteres)
- canal já existente
- mensagem de publicação vazia
- tipo de mensagem desconhecido

Em caso de erro no login, o bot repete a tentativa, conforme o enunciado.

Na publicação o servidor valida apenas o **formato** do nome do canal, não a
sua existência. Isso é deliberado: como cada servidor conhece somente os
canais que ele próprio criou e o broker distribui os pedidos em round-robin,
exigir que o canal estivesse na lista local rejeitaria a maior parte das
publicações válidas. A restrição deixa de ser necessária na Parte 5, quando
todos os servidores passarem a ter todos os dados.

### Comportamento dos bots

Conforme padronizado no enunciado, cada bot:

1. faz login e repete a tentativa em caso de erro;
2. cria um canal novo se existirem menos de 5 canais;
3. inscreve-se em mais um canal, escolhido aleatoriamente, enquanto estiver
   inscrito em menos de 3;
4. em laço infinito, escolhe um canal da lista e envia 10 mensagens
   aleatórias com 1 segundo de intervalo.

O socket SUB conecta ao proxy e começa a escutar **antes** do login. Como um
socket PUB descarta mensagens quando ainda não há inscritos conhecidos
(o problema do *slow joiner*), quanto antes a inscrição chegar ao proxy,
menos publicações iniciais se perdem.

Cada publicação recebida é exibida com o canal, o autor, a mensagem e os
**dois** timestamps: o do envio pelo servidor e o do recebimento pelo bot.

### Persistência: JSON Lines

Cada servidor grava em `/dados/<nome_do_servidor>.jsonl`, uma linha por
evento, com o timestamp. Na inicialização o servidor relê o arquivo e
reconstrói a lista de canais, de modo que os dados sobrevivem a reinícios.

Eventos gravados: `login`, `canal_criado` e `publicacao` (com canal,
mensagem e autor).

A proibição de JSON no enunciado vale para as mensagens trafegadas pela
rede, que usam MessagePack. O formato de armazenamento em disco é livre, e
JSON Lines foi escolhido por não exigir dependência externa em nenhuma das
duas linguagens e por permitir leitura incremental, sem carregar o arquivo
inteiro na memória.

Conforme o enunciado, cada servidor mantém o seu próprio arquivo, que não é
compartilhado com os demais.

## Limitação conhecida

Como cada servidor mantém os seus próprios dados e o broker distribui as
requisições em round-robin, o estado fica repartido entre os quatro
servidores:

- um canal criado em um servidor não aparece quando a listagem é atendida
  por outro, e o mesmo nome pode ser criado em servidores diferentes;
- o histórico de publicações fica dividido, e nenhum servidor possui o
  registro completo das mensagens trocadas.

Esse é exatamente o problema que a Parte 5 (consistência e replicação)
resolve, e por isso foi mantido nestas entregas. Vale notar que a limitação
afeta apenas o estado armazenado: a entrega das publicações em si não sofre
com ela, porque o Pub/Sub passa pelo proxy e alcança todos os bots inscritos
no canal, independentemente de qual servidor publicou.

## Execução

```bash
docker compose up --build
```

Sobem 11 containers. Tanto os bots quanto os servidores imprimem na tela
cada mensagem enviada e recebida, com o conteúdo completo do envelope.

## Estrutura

```
.
├── docker-compose.yaml
├── dados/                      # um .jsonl por servidor
├── node/
│   ├── Dockerfile
│   ├── package.json
│   ├── mensagem.js             # envelope, MessagePack e relógio lógico
│   ├── armazenamento.js        # persistência em JSONL
│   ├── servidor.js
│   └── bot.js
└── python/
    ├── Dockerfile
    ├── requirements.txt
    ├── mensagem.py
    ├── armazenamento.py
    ├── servidor.py
    ├── bot.py
    ├── broker.py               # ROUTER/DEALER do Request-Reply
    ├── proxy.py                # XSUB/XPUB do Pub/Sub
    └── referencia.py           # rank, lista, heartbeat e hora
```

## Integrantes

- João Pedro Martins — Node.js
- Wellington de Menezes Paim — Python