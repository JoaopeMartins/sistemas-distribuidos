import zmq

context = zmq.Context()

# Lado dos publishers (servidores)
entrada = context.socket(zmq.XSUB)
entrada.bind("tcp://*:5557")

# Lado dos subscribers (bots)
saida = context.socket(zmq.XPUB)
saida.bind("tcp://*:5558")

print("Proxy: publishers em 5557, subscribers em 5558", flush=True)

zmq.proxy(saida, entrada)