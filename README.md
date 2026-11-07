# sistemas-distribuidos
O broker é um repassador puro: move os frames entre os dois sockets sem
desempacotar o conteúdo. Por isso funciona igual com bots e servidores
escritos em qualquer linguagem.

O socket DEALER faz o balanceamento de carga entre os servidores em
round-robin, sem nenhuma lógica de distribuição escrita por nós.

Containers em execução: 1 broker, 4 servidores (2 por integrante) e 4 bots
(2 por integrante).

## Escolhas feitas

### Linguagens

JavaScript (Node.js) e Python. Cada integrante implementou cliente e
servidor completos na sua linguagem, e as quatro combinações se comunicam
entre si — o bot em Node conversa com o servidor em Python e vice-versa.

O broker foi escrito em Python por ser um componente único e independente
das implementações de cliente e servidor.

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

Mesmo envelope para pedidos e respostas:

| campo | tipo | descrição |
|---|---|---|
| `tipo` | string | ação ou tipo da resposta |
| `origem` | string | quem enviou |
| `timestamp` | float | epoch em segundos, UTC |
| `relogio` | int | relógio lógico (Parte 3; zero nesta entrega) |
| `status` | string | `ok` ou `erro`, apenas nas respostas |
| `dados` | map | conteúdo específico do tipo |

O campo `relogio` já existe nesta parte, zerado, para que a adição do
relógio lógico na Parte 3 não exija mudança no formato.

O timestamp é sempre em **segundos**. Em Node, `Date.now()` devolve
milissegundos, então o valor é dividido por 1000 antes do envio.

Operações implementadas:

| pedido | `dados` | resposta | `dados` |
|---|---|---|---|
| `login` | `{usuario}` | `login_resp` | `{mensagem}` |
| `criar_canal` | `{canal}` | `criar_canal_resp` | `{mensagem}` |
| `listar_canais` | `{}` | `listar_canais_resp` | `{canais: [...]}` |

### Tratamento de erros

Respostas de erro usam `status: "erro"` e trazem a descrição em
`dados.mensagem`. Os casos tratados:

- nome de usuário vazio
- nome de canal inválido (regra: `a-z`, `0-9`, `-`, `_`, de 3 a 20 caracteres)
- canal já existente
- tipo de mensagem desconhecido

Em caso de erro no login, o bot repete a tentativa, conforme o enunciado.

### Persistência: JSON Lines

Cada servidor grava em `/dados/<nome_do_servidor>.jsonl`, uma linha por
evento, com o timestamp. Na inicialização o servidor relê o arquivo e
reconstrói a lista de canais, de modo que os dados sobrevivem a reinícios.

Eventos gravados nesta parte: `login` e `canal_criado`.

A proibição de JSON no enunciado vale para as mensagens trafegadas pela
rede, que usam MessagePack. O formato de armazenamento em disco é livre, e
JSON Lines foi escolhido por não exigir dependência externa em nenhuma das
duas linguagens e por permitir leitura incremental, sem carregar o arquivo
inteiro na memória.

Conforme o enunciado, cada servidor mantém o seu próprio arquivo, que não é
compartilhado com os demais.

## Limitação conhecida

Como cada servidor mantém a sua própria lista de canais e o broker
distribui as requisições em round-robin, um canal criado em um servidor não
aparece quando a listagem é atendida por outro. O mesmo nome também pode
ser criado em servidores diferentes.

Esse é exatamente o problema que a Parte 5 (consistência e replicação)
resolve, e por isso foi mantido nesta entrega.

## Execução

```bash
docker compose up --build
```

Sobem 9 containers. Tanto os bots quanto os servidores imprimem na tela
cada mensagem enviada e recebida, com o conteúdo completo do envelope.

## Estrutura

.
├── docker-compose.yaml
├── dados/ # um .jsonl por servidor
├── node/
│ ├── Dockerfile
│ ├── package.json
│ ├── mensagem.js # envelope e MessagePack
│ ├── armazenamento.js # persistência em JSONL
│ ├── servidor.js
│ └── bot.js
└── python/
├── Dockerfile
├── requirements.txt
├── mensagem.py
├── armazenamento.py
├── servidor.py
├── bot.py
└── broker.py


## Integrantes

- João Pedro Martins — Node.js
- Wellington de Menezes Paim — Python