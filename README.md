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
- **Parte 4** — eleição de coordenador entre os servidores e sincronização
  dos relógios pelo algoritmo de Berkeley.
- **Parte 5** — replicação dos dados: todos os servidores passam a ter todos
  os logins, canais e publicações.

## Arquitetura

Quatro caminhos independentes de comunicação:

```
Request-Reply (pedidos dos bots aos servidores)
bot (REQ) → broker:5555 (ROUTER) → (DEALER) broker:5556 → servidor (REP)

Publisher-Subscriber (publicações e replicação)
servidor (PUB) → proxy:5557 (XSUB) → (XPUB) proxy:5558 → bot/servidor (SUB)

Serviço de referência (apenas servidores, sem passar pelo broker)
servidor (REQ) → referencia:5559 (REP)

Entre servidores (eleição e relógio)
servidor (REQ) → outro servidor:5560 (REP)
```

O broker e o proxy são repassadores puros: nenhum dos dois desempacota o
conteúdo das mensagens. Por isso funcionam igual com bots e servidores
escritos em qualquer linguagem.

O socket DEALER do broker faz o balanceamento de carga entre os servidores em
round-robin, sem nenhuma lógica de distribuição escrita por nós. No proxy, o
`zmq.proxy()` repassa as publicações e também propaga as inscrições no sentido
inverso, do XPUB para o XSUB — sem isso os sockets PUB descartariam as
mensagens por não haver inscritos conhecidos.

A partir da Parte 4, cada servidor também abre um socket REP próprio na porta
5560 para atender os outros servidores. Esse caminho não passa pelo broker de
propósito: o broker distribui em round-robin e entregaria a mensagem a
qualquer servidor, enquanto a eleição e a sincronização de relógio precisam
falar com um servidor específico. Os servidores se encontram pelo nome do
container, que vem da lista fornecida pelo serviço de referência.

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

Operações entre bot e servidor:

| pedido | `dados` | resposta | `dados` |
|---|---|---|---|
| `login` | `{usuario}` | `login_resp` | `{mensagem}` |
| `criar_canal` | `{canal}` | `criar_canal_resp` | `{mensagem}` |
| `listar_canais` | `{}` | `listar_canais_resp` | `{canais: [...]}` |
| `publicar` | `{canal, mensagem}` | `publicar_resp` | `{mensagem}` |

Operações entre servidores (porta 5560):

| pedido | `dados` | resposta | `dados` |
|---|---|---|---|
| `eleicao` | `{}` | `eleicao_resp` | `{mensagem}` |
| `hora` | `{}` | `hora_resp` | `{hora}` |
| `hora_local` | `{}` | `hora_local_resp` | `{hora}` |

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
| `heartbeat` | `{servidor}` | `heartbeat_resp` | `{mensagem}` |

Cada servidor pede o seu rank ao iniciar. Nomes repetidos recebem sempre o
mesmo rank, conforme o enunciado. A cada 10 mensagens de clientes recebidas,
o servidor envia um heartbeat para se manter na lista.

Na Parte 3 a resposta do heartbeat também trazia a hora correta. A partir da
Parte 4 isso foi removido, conforme o enunciado: a hora passa a vir do
servidor coordenador.

**Remoção por tempo.** O enunciado pede que o servidor saia da lista se não
enviar heartbeat, mas não define o critério. Adotamos um limite de 30
segundos desde o último sinal, verificado a cada pedido recebido. Como o
socket REP é síncrono, essa verificação no momento do pedido dispensa uma
thread de vigilância separada.

### Eleição do coordenador

Usamos o algoritmo do valentão (*bully*): vence quem tem o maior rank.

Quando um servidor inicia uma eleição, ele pede a lista ao serviço de
referência e envia `eleicao` a todos os servidores de rank superior. Se
algum responder, ele cede e aguarda o anúncio. Se nenhum responder, declara-se
coordenador e publica o próprio nome no tópico `servers`, por onde todos os
servidores ficam sabendo quem é o coordenador atual.

A eleição é disparada em três situações: na inicialização, quando o
coordenador não responde dentro do tempo limite de 2 segundos, e quando um
servidor recebe um pedido `eleicao` de outro.

**O socket REQ é descartado a cada pergunta entre servidores.** No ZeroMQ, um
socket REQ entra em estado inválido quando um `recv` expira, pois ele espera
a resposta antes de aceitar um novo envio. Criar um socket novo por pergunta
é mais simples do que tentar recuperá-lo.

### Sincronização dos relógios: Berkeley

A cada 15 mensagens de clientes, cada servidor sincroniza o relógio:

- se for o coordenador, executa o algoritmo de Berkeley;
- caso contrário, pede a hora ao coordenador com o pedido `hora`.

No Berkeley, o coordenador consulta todos os servidores conhecidos com
`hora_local`, calcula a média incluindo o próprio relógio já ajustado e
adota essa média. O diagrama do enunciado mostra apenas "pede a hora e
recebe a hora", mas o texto exige Berkeley, então a média é de fato
calculada — e por isso existe o terceiro tipo de mensagem, `hora_local`,
que qualquer servidor responde.

**O ajuste é guardado, não aplicado.** Um container não pode alterar o
relógio do sistema do host, então cada servidor armazena a diferença entre a
hora de referência e a sua própria, e uma função `hora_corrigida()` devolve
o valor ajustado.

**O pedido `hora` não dispara rede.** Esta foi uma correção necessária. Na
primeira versão, o coordenador respondia `hora` executando o Berkeley na
hora, o que o levava a perguntar `hora_local` ao mesmo servidor que estava
bloqueado esperando a resposta — um impasse que derrubava a sincronização de
todos. Hoje o coordenador responde imediatamente com o valor que já tem, e o
Berkeley roda no ciclo próprio dele. O resultado prático é que os quatro
relógios convergem com diferença na casa dos microssegundos entre si.

## Replicação (Parte 5)

### Método escolhido: replicação ativa via Pub/Sub

Dentre os métodos vistos em aula, escolhemos a **replicação ativa**: toda
atualização é entregue a todas as réplicas, que a aplicam localmente.

Como o enunciado deixa a implementação livre, usamos a infraestrutura que já
existia: quando um servidor grava um evento, ele também o publica no tópico
`servers` do proxy Pub/Sub. Todos os servidores estão inscritos nesse tópico
e aplicam o evento no próprio estado e no próprio arquivo em disco.

### Como isso resolve o problema do projeto

O broker distribui as requisições em round-robin, então cada servidor
recebia apenas uma fração das mensagens. Isso gerava dois defeitos: a
listagem de canais variava conforme o servidor que atendesse o pedido, e
nenhum servidor tinha o histórico completo — se um caísse, essa parte do
histórico se perdia.

Com a replicação ativa, cada evento alcança as quatro réplicas. A listagem
passa a ser consistente independentemente de quem atende, e qualquer servidor
isolado contém o histórico inteiro.

### Adaptações necessárias

**Deduplicação por identificador.** Cada evento recebe um id único no formato
`<nome do servidor>-<contador>`. Antes de aplicar um evento replicado, o
servidor verifica se aquele id já foi visto. Sem isso o servidor que originou
o evento o gravaria duas vezes, já que também está inscrito no tópico. Na
inicialização, os ids são reconstruídos a partir do próprio arquivo.

**Gravação local direta.** O servidor que origina o evento grava e publica no
mesmo passo, em vez de esperar o evento voltar pelo proxy. É mais simples e
evita que uma falha de entrega deixe o autor sem o próprio dado.

**Timestamp do relógio sincronizado.** O registro replicado usa
`hora_corrigida()`, não a hora local bruta. Assim o histórico distribuído
fica com horas coerentes entre servidores, aproveitando o Berkeley da
Parte 4.

**Serialização dos envios no socket PUB (Node).** Correção necessária
durante o desenvolvimento. No servidor em Node, três laços assíncronos
concorrentes compartilham o mesmo socket PUB, e o ZeroMQ lança
`Socket is busy writing` quando dois envios se sobrepõem. As publicações
passaram a ser encadeadas numa fila de promessas, de modo que só há um envio
por vez. O servidor em Python não precisou disso porque o seu laço principal
é um poller sequencial.

### Consistência eventual

A replicação é assíncrona: o servidor responde ao bot assim que grava
localmente, sem esperar confirmação das réplicas. Existe, portanto, uma
janela de alguns milissegundos em que um evento está em um servidor e ainda
não nos outros. É consistência eventual, não forte — suficiente para o que o
enunciado pede, e a escolha mantém a latência de resposta aos bots
inalterada.

Em um teste de 90 segundos, os quatro servidores registraram 400 eventos
distintos: três com os 400 e um com 399, faltando exatamente o último evento
publicado antes do encerramento dos containers. É o comportamento esperado
da replicação assíncrona interrompida no meio, não perda de dados.

## Tratamento de erros

Respostas de erro usam `status: "erro"` e trazem a descrição em
`dados.mensagem`. Os casos tratados:

- nome de usuário vazio
- nome de canal inválido (regra: `a-z`, `0-9`, `-`, `_`, de 3 a 20 caracteres)
- canal já existente
- mensagem de publicação vazia
- tipo de mensagem desconhecido

Em caso de erro no login, o bot repete a tentativa, conforme o enunciado.

Na publicação o servidor valida apenas o **formato** do nome do canal, não a
sua existência. Essa decisão foi tomada nas Partes 2 a 4, quando cada servidor
conhecia somente os canais que ele próprio havia criado e exigir a existência
rejeitaria a maior parte das publicações válidas. Com a replicação da Parte 5
a validação passou a ser possível, mas foi mantida como está para não alterar
o comportamento dos bots.

## Comportamento dos bots

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

## Persistência: JSON Lines

Cada servidor grava em `/dados/<nome_do_servidor>.jsonl`, uma linha por
evento, com o id e o timestamp. Na inicialização o servidor relê o arquivo e
reconstrói a lista de canais e os ids já aplicados, de modo que os dados
sobrevivem a reinícios.

Eventos gravados: `login`, `canal_criado` e `publicacao` (com canal,
mensagem e autor).

A proibição de JSON no enunciado vale para as mensagens trafegadas pela
rede, que usam MessagePack. O formato de armazenamento em disco é livre, e
JSON Lines foi escolhido por não exigir dependência externa em nenhuma das
duas linguagens e por permitir leitura incremental, sem carregar o arquivo
inteiro na memória.

Cada servidor mantém o seu próprio arquivo, conforme o enunciado. A partir da
Parte 5 os quatro arquivos passam a conter os mesmos eventos, por replicação,
mas continuam sendo arquivos independentes — nenhum é compartilhado.

## Execução

```bash
docker compose up --build
```

Sobem 11 containers. Tanto os bots quanto os servidores imprimem na tela
cada mensagem enviada e recebida, com o conteúdo completo do envelope.

Para verificar a replicação, basta comparar os arquivos depois de alguns
segundos de execução:

```bash
wc -l dados/*.jsonl
```

## Estrutura

```
.
├── docker-compose.yaml
├── dados/
├── node/
│   ├── Dockerfile
│   ├── package.json
│   ├── mensagem.js
│   ├── armazenamento.js
│   ├── servidor.js
│   └── bot.js
└── python/
    ├── Dockerfile
    ├── requirements.txt
    ├── mensagem.py
    ├── armazenamento.py
    ├── servidor.py
    ├── bot.py
    ├── broker.py
    ├── proxy.py
    └── referencia.py
```

## Integrantes

- João Pedro Martins — Node.js
- Wellington de Menezes Paim — Python