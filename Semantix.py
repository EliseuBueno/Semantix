### Como o projeto foi criado no ambiente virtual do Databricks, toda a parte de configuração de sessão e imports das bibliotecas já vem pronta nativamente ###
###Imports das bibliotecas do pyspark###
#from pyspark import SparkConf, SparkContext
#from pyspark.sql import SparkSession, Row
#from pyspark.sql.functions import udf, dense_rank, desc
#from pyspark.sql.types import StringType, FloatType
#from pyspark.sql.window import Window

###Configuração de sessão###
#conf = (SparkConf()
#         .setMaster("local")
#         .setAppName("Semantix")
#         .set("spark.executor.memory", "8g"))
#sc = SparkContext(conf = conf)
#spark = SparkSession(sc)


#Classe que lerá os arquivos e retornará apenas um Rdd unificando todos os logs
class Reader:
  
  '''Método que recebe uma lista com o nome dos arquivos e os unifica em um RDD:
      Parâmetro: paths = Deve ser uma lista contendo o path absoluto dos arquivos
      Retorno: Rdd unificado'''
  def getRdd(self, paths):
    i = 0
    for path in paths:
      if i == 0:
        self.rdd_full = sc.textFile(path) 
      else:
        self.rdd_full = self.rdd_full.union(sc.textFile(path)) 
      i = i + 1
    return self.rdd_full 

#Classe que realiza as análises dos dados
class Analyzer:
  
  '''Método que recebe um rdd e delimitador e retorna a quantidade de itens distintos
    Parâmetros: rdd = Deve ser do tipo Rdd e conter os dados a serem analisados
                delimiter: Deve ser do tipo String e conter o critério para split dos dados
    Retorno: A quantidade de hosts distintos do tipo Integer'''
  def getDistinctHosts(self, rdd, delimiter):
    self.rdd_hosts = rdd.map(lambda row: row.split(delimiter)[0])
    qtdHosts = self.rdd_hosts.distinct().count()
    return qtdHosts
  
  '''Método que recebe um rdd e delimitador
    Parâmetros: rdd = Deve ser do tipo Rdd e conter os dados a serem analisados
                delimiter: Deve ser do tipo String e conter o critério para split dos dados
    Retorno: Retorna os erros filtrados de acordo com os valores passados ao delimitador'''
  def getRddErro(self, rdd, delimiter):
    self.rdd_erro = rdd.filter(lambda row: delimiter in row)
    return self.rdd_erro
  
  '''Método que recebe um rddErro e retorna a quantidade de erros que contém na rddErro
    Parâmetros: rddErro: Deve ser do tipo String e conter os erros a serem contados
    Retorno: A quantidade de erros filtrados de acordo com os valores passados ao delimitador do tipo Integer'''
  def getQtdErro(self, rddErro):
    qtdErro = rddErro.count()
    return qtdErro
  
  '''Método que recebe um rddErro e delimitador e retorna as 5 urls que mais contém os erros que estão na rddErro
    Parâmetros: rddErro: Deve ser do tipo String e conter os erros a serem tratados
                delimiter: Deve ser do tipo String e conter o critério para split dos dados
    Retorno: Retorna as 5 urls que mais contém os erros que estão na rddErro'''
  def getURLErro(self, rddErro, delimiter):
    self.rdd_url = rddErro.map(lambda row: row.split(delimiter)[1]).map(lambda x: (x, 1)).reduceByKey(lambda v1,v2: v1+v2)
    urlsTopErros = self.rdd_url.sortBy(lambda x: x[1], ascending = False).take(5)
    return urlsTopErros
  
  '''Método que recebe um rddErro e delimitador e retorna a quantidade de erros que estão na rddErro filtrados e sequenciados por quantidade/dia
    Parâmetros: rddErro: Deve ser do tipo String e conter os erros a serem tratados
                delimiter: Deve ser do tipo String e conter o critério para split dos dados
    Retorno: Retorna a quantidade de erros que estão na rddErro filtrados e sequenciados por quantidade/dia'''
  def getErrosDate(self, rddErro, delimiter):
    self.rdd_data = rddErro.map(lambda row: row.split(delimiter)[1][0:11]).map(lambda x: (x, 1)).reduceByKey(lambda v1,v2: v1+v2)
    from dateutil.parser import parse
    erroDate = self.rdd_data.sortBy(lambda x: parse(x[0])).take(self.rdd_data.count())
    return erroDate
  
  '''Método que recebe um rdd e retorna a quantidade total de bytes
    Parâmetros: rdd: Deve ser do tipo Rdd e conter os dados a serem analisados
    Retorno: retorna a quantidade total de bytes'''
  def getBytes(self, rdd):
    self.bytes = rdd.map(lambda x: x.split()[-1]).filter(lambda x: "-" not in x).filter(lambda x: "alyssa" not in x).map(lambda x: int(x)).sum()
    return self.bytes

#Recebem e iniciam as classes Analyzer e Reader
analyzer = Analyzer()
reader = Reader()

#Armazenam os dados e retornam a união de todos os valores dentro do rdd - Tabela access_log_Jul95 em união com a tabela acess_log_Aug95 contendo o caminho de onde se encontram os arquivos.
root = '/FileStore/tables/'
paths = [root + 'access_log_Jul95',
		 root + 'access_log_Aug95']
rdd = reader.getRdd(paths)

#Passa os valores ao delimiter, recebe e imprime os valores passados pelo retorno do método getDistincHosts - Quantidade de Erros Distintos
delimiter = ' - - '
qtdDistinctsHosts = analyzer.getDistinctHosts(rdd, delimiter)
print(qtdDistinctsHosts)

#Passa os valores ao delimiter, recebe os valores passados pelo retorno do método getRddErro - Erros 404
delimiter = ' 404 '
rddErro = analyzer.getRddErro(rdd, delimiter)

#Recebe e imprime os valores passados pelo retorno do método getQtdErro - Quantidade de erros 404
qtdErro = analyzer.getQtdErro(rddErro)
print(qtdErro)

#Passa os valores ao delimiter, recebe e imprime os valores passados pelo retorno do método getURLErro colocando cada valor em uma linha - 5 URLs que mais aparecem o erro 404 - URL e Quantidade
delimiter = '\"'
urlsTop5Erros = analyzer.getURLErro(rddErro, delimiter)
for item in urlsTop5Erros:
    print (item)

#Recebe e imprime os valores passados pelo retorno do método getErroDate colocando cada valor em uma linha -Data e quantidade
qtdErroDate = analyzer.getErrosDate(rddErro, delimiter)
for item in qtdErroDate:
    print (item)

#Recebe e imprime os valores passados pelo retorno do método getBytes - Quantidade total de Bytes
qtdBytes = analyzer.getBytes(rdd)
print(qtdBytes)
