**1 - Qual o objetivo do comando cache em Spark?**

**R:** O objetivo do comando cache em Spark é receber operações de resultados intermediarios lazy, armazená-los para que possam ser reutilizados, otimizado assim o desempenho de resultados, uma vez que sem este comando em ações repetidas teria que ser executado diversas vezes sobre um mesmo conjunto de dados mesmo que resultados intermediários sejam iguais.

**2 - O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?**

**R:** O mesmo código implementado em Spark é Sim normalmente mais rápido que a implementação equivalente em MapReduce.
Para cada Job no MapReduce precisa ser iniciada uma instância da JVM, já no Spark, está instância fica sempre em execução em cada um dos nós, com isso o processo em Spark se torna normalmente mais rápido.
Outro fator que torna mais rápido o uso de Spark é quando mais de um Job precisa ser executado, o uso de memória é menor pois o Job não precisa ser escrito em disco e depois lido novamente quando passado ao próximo Job como é no caso do MapReduce, o Spark permite que entre as operações o resultado intermediário seja passado diretamente através do cache dos dados em memória.

**3 - Qual é a função do SparkContext?**

**R:** A função do SparkContext é receber as configurações e as solicitações para tomadas de decisão sobre: criação de RDDs, colocar Jobs em execução, criar acumuladores, criar variáveis de broadcast.
Pode ser acessado como uma variável em um programa para utilizar os seus recursos.

**4 - Explique com suas palavras o que é Resilient Distributed Datasets (RDD).**

**R:** O RDD (Resilient Distributed Datasets) é como uma tabela em um banco de dados onde podem ser armazenados distintos tipos de dados. 
É o principal conceito do Spark (em adstração de dados), que armazena estes dados em diversas partições (distributed) para otimização dos processos, consegue recompor dados (resilient) uma vez que traballha com vários nós dentro do cluster.
São imutáveis (podendo sofrer transformações que resultam em novos RDDs), são objetos apenas de leitura.
Um mesmo RDD pode ser executado ao mesmo tempo por diversas partições.

**5 - GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?**

**R:** Com o GrupyByKey se transfere todo o DataSet pela rede, de uma única vez, já com o ReduceByKey trabalha com somas parciais através de cada chave em cada partição com isto há a redução de grande tráfego em rede, além de uma alocação de memoria muito menor, logo após executa a soma das parciais nos executores finais.
Por esta razão conclui-se que a performace e desempenho de ReduceByKey em grandes DataSets é muito mais considerável, gerando um impácto mais positivo durante e ao final de sua execução.

**Explique o que o código Scala abaixo faz**


```
1. val textFile = sc.textFile ( "hdfs://..." )
2. val counts = textFile.flatMap ( line => line.split ( " " ))
3.           .map( word => ( word , 1 ))
4.           .reduceByKey ( _ + _ )
5. counts.saveAsTextFile ( "hdfs://..." )
```

**Linha 1:** Uma RDD é criada, onde recebe o caminho do arquivo que será usado para manipulação posteriormente.
**Linha 2:** Outra RDD é criada, onde recebe a primeira aplicando uma quebra de strings atravez do comando split que está sendo feito pelos espaços em branco, pegando as sequencias de cada linha e criando assim uma coleção de palavras.
**Linha 3:** É feito o mapeamento, onde cada palavra é aplicado o conceito de chave-valor, onde a chave é a palavra e o valor definido 1.
**Linha 4:** Os valores definidos como 1 são agrupados de acordo com a chave e aplicados o valor 1 para cada vez que a palavra aparecer na busca.
**Linha 5:** Os valores são contados e o RDD salvo em um arquivo de texto no caminho informado.




**Teste Prático:**

**1 - Número de hosts únicos:**

**R:** 137979

**2 - O total de erros 404:**

**R:** 20901

**3 - Os 5 URLs que mais causaram erro 404:** 

**R:**

| Url                                      | Qtd_erro_404 |
| --- | --- |
| GET /pub/winvn/readme.txt HTTP/1.0 | 2004 |
| GET /pub/winvn/release.txt HTTP/1.0 | 1732 |
| GET /shuttle/missions/STS-69/mission-STS-69.html HTTP/1.0 | 682 |
| GET /shuttle/missions/sts-68/ksc-upclose.gif HTTP/1.0 | 426 |
| GET /history/apollo/a-001/a-001-patch-small.gif HTTP/1.0 | 384 |

**4 - Quantidade de erros 404 por dia:**

**R:**

| DIA/MES/ANO | QTD_ERROS_404 |
| --- | --- |
| 01/Jul/1995 | 316 |
| 02/Jul/1995 | 291 |
| 03/Jul/1995 | 474 |
| 04/Jul/1995 | 359 |
| 05/Jul/1995 | 497 |
| 06/Jul/1995 | 640 |
| 07/Jul/1995 | 570 |
| 08/Jul/1995 | 302 |
| 09/Jul/1995 | 348 |
| 10/Jul/1995 | 398 |
| 11/Jul/1995 | 471 |
| 12/Jul/1995 | 471 |
| 13/Jul/1995 | 532 |
| 14/Jul/1995 | 413 |
| 15/Jul/1995 | 254 |
| 16/Jul/1995 | 257 |
| 17/Jul/1995 | 406 |
| 18/Jul/1995 | 465 |
| 19/Jul/1995 | 639 |
| 20/Jul/1995 | 428 |
| 21/Jul/1995 | 334 |
| 22/Jul/1995 | 192 |
| 23/Jul/1995 | 233 |
| 24/Jul/1995 | 328 |
| 25/Jul/1995 | 461 |
| 26/Jul/1995 | 336 |
| 27/Jul/1995 | 336 |
| 28/Jul/1995 | 94 |
| 01/Aug/1995 | 243 |
| 03/Aug/1995 | 304 |
| 04/Aug/1995 | 346 |
| 05/Aug/1995 | 236 |
| 06/Aug/1995 | 373 |
| 07/Aug/1995 | 537 |
| 08/Aug/1995 | 391 |
| 09/Aug/1995 | 279 |
| 10/Aug/1995 | 315 |
| 11/Aug/1995 | 263 |
| 12/Aug/1995 | 196 |
| 13/Aug/1995 | 216 |
| 14/Aug/1995 | 287 |
| 15/Aug/1995 | 327 |
| 16/Aug/1995 | 259 |
| 17/Aug/1995 | 271 |
| 18/Aug/1995 | 256 |
| 19/Aug/1995 | 209 |
| 20/Aug/1995 | 312 |
| 21/Aug/1995 | 305 |
| 22/Aug/1995 | 288 |
| 23/Aug/1995 | 345 |
| 24/Aug/1995 | 420 |
| 25/Aug/1995 | 415 |
| 26/Aug/1995 | 366 |
| 27/Aug/1995 | 370 |
| 28/Aug/1995 | 410 |
| 29/Aug/1995 | 420 |
| 30/Aug/1995 | 571 |
| 31/Aug/1995 | 526 |

**5 - O total de bytes retornados:**

**R:** 65.524.314.915 bytes
