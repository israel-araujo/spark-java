 Listagem 3. Número de linhas de ônibus no arquivo.
 
 package com.devmedia.transformation;
   
  import org.apache.spark.SparkConf;
  import org.apache.spark.api.java.JavaRDD;
  import org.apache.spark.api.java.JavaSparkContext;
   
  public class Exemplo2 {
   
        public static void main(String[] args) {
              
  // configuração do Spark
              SparkConf conf = new SparkConf().setMaster("local").setAppName("BusProcessor");
              JavaSparkContext ctx = new JavaSparkContext(conf);
              
  // carrega os dados dos ônibus de sp
              JavaRDD<String> linhas = ctx.textFile("c:/dev/teste7.log");
              long numeroLinhas = linhas.count();
              
              // escreve o número de ônibus que existem no arquivo
              System.out.println(numeroLinhas);
              
              ctx.close();
              
        }
   
  }