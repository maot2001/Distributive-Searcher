def clockToList(clock: str) -> list[int]:
    return [int(num) for num in clock.strip('[]').split(',') if num.strip()]

class VectorClock:
    def __init__(self, num_nodes, pid):
        self.num_nodes = num_nodes
        self.clock = [0] * num_nodes
        self.pid = pid  
        
    def increment(self):
        """Incrementa el contador del nodo actual."""
        # print(f'clocks: {len(self.clock)} my pid: {self.pid}')
        self.clock[self.pid] += 1

    def update(self, received_clock):
        """Actualiza el reloj vectorial con otro reloj vectorial recibido."""
        try:
            received_clock = clockToList(received_clock)
            if isinstance(received_clock, list) and len(received_clock) == self.num_nodes:
                #print(f"mi reloj {self.clock}")
                #print(f"received_clock {received_clock}")
                #print(self.num_nodes)
                for i in range(self.num_nodes):
                    #print(f"node num: {i}")
                    self.clock[i] = max(self.clock[i], received_clock[i])
                # Incrementa después de recibir el reloj vectorial
                self.increment()
        except:
            pass

    def send_event(self, toString=True):
        """Incrementando el reloj local. Envío de una copia del reloj vectorial de este nodo"""
        self.increment()
        if toString:
            return str(self.clock[:])
        else:
            return self.clock[:]

    def receive_event(self, received_clock):
        """Simula la recepción de un evento actualizando el reloj."""
        self.update(received_clock)

    def __str__(self):
        return str(self.clock)