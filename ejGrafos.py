from pyspark import SparkContext
import sys

def mapper(line):
    edge = line.split(',')
    n1 = edge[0]
    n2 = edge[1]
    return [(n1,n2), (n2,n1)]

def mapper2(elem):
    if elem[0] > elem[1]:
        return (elem[1],elem[0])
    else:
        return elem
    
primera_componente = [] 
segunda_comp = []      
def parejas(elem):
    lista = []
    for i in range(len(elem[0])):
        if elem[0][i] in primera_componente:
            pos = primera_componente.index(elem[0][i])
            lista.append((elem[0][i], elem[1], ('pending', segunda_comp[pos])))
        else:
            lista.append((elem[0][i], elem[1], 'exists'))
            primera_componente.append(elem[0][i])
            segunda_comp.append(elem[1])
    return lista

def triciclos(lista):
    result = []
    for i in range(len(lista)):
        if lista[i][2][0] == 'pending':
            if (lista[i][1], lista[i][2][1], 'exists') in lista:
                result.append((lista[i][0],lista[i][1],lista[i][2][1]))
    return result

SAMPLE = 15
sc = SparkContext()

rdd = sc.textFile(sys.argv[1])
print('textFile', rdd.take(SAMPLE))

rdd = rdd.flatMap(mapper)
print('flatMap', rdd.take(SAMPLE))

rdd = rdd.filter(lambda x: x[0]!=x[1])
print('filter', rdd.take(SAMPLE))

rdd = rdd.map(mapper2)
print('menores_primero',rdd.take(SAMPLE))

rdd = rdd.distinct()
print('distinct', rdd.take(SAMPLE))

rdd = rdd.groupByKey()
print('groupByKey', rdd.take(SAMPLE))


rdd = rdd.map(lambda x: (tuple(sorted(x[1])), x[0]))
print('map', rdd.take(SAMPLE))

rdd = rdd.flatMap(parejas)
print('flatmap', rdd.take(SAMPLE))

print('Result:')
lista = triciclos(rdd.collect())
print(lista)
