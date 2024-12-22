from mrjob.job import MRJob

class IntensidadPuntos(MRJob):

    def mapper(self, key, line):
        (punto,mes,dia,anyo,hora,intensidad) = line.split(',')
        if punto !='Punto':
            yield int(punto), int(intensidad)

    def reducer(self, punto, intensidad):
        total = 0
        numElements = 0
        for x in intensidad:
            total += x
            numElements += 1
            
        yield punto, total / float(numElements)

if __name__ == '__main__':
    IntensidadPuntos.run()