import zmq

context = zmq.Context()

# Lado dos clientes (bots)
clientes = context.socket(zmq.ROUTER)
clientes.bind("tcp://*:5555")

# Lado dos servidores
servidores = context.socket(zmq.DEALER)
servidores.bind("tcp://*:5556")

print("Broker: clientes em 5555, servidores em 5556", flush=True)

poller = zmq.Poller()
poller.register(clientes, zmq.POLLIN)
poller.register(servidores, zmq.POLLIN)

while True:
    eventos = dict(poller.poll())

    if clientes in eventos:
        quadro = clientes.recv(copy=False)
        mais = clientes.get(zmq.RCVMORE)
        servidores.send(quadro, zmq.SNDMORE if mais else 0)

    if servidores in eventos:
        quadro = servidores.recv(copy=False)
        mais = servidores.get(zmq.RCVMORE)
        clientes.send(quadro, zmq.SNDMORE if mais else 0)