package com.devmedia.map.reduce;
   
  import java.util.List;
   
  import org.apache.spark.SparkConf;
  import org.apache.spark.api.java.JavaPairRDD;
  import org.apache.spark.api.java.JavaRDD;
  import org.apache.spark.api.java.JavaSparkContext;
   
  import scala.Tuple2;
   
  public class Exemplo1 {
   
        public static void main(String[] args) {
   
  // configuração do Spark
              SparkConf conf = new SparkConf().setMaster("local").setAppName("BusProcessor");
              JavaSparkContext ctx = new JavaSparkContext(conf);
   
  // carrega os dados dos ônibus de sp
              JavaRDD<String> onibus = ctx.textFile("c:/dev/teste7.log");
   
  // faz o map com as linhas de ônibus
              JavaPairRDD<String, Integer> agrupaOnibus = onibus
                          .mapToPair(s -> new Tuple2<String, Integer>(s.split(" ")[2], 1));
              JavaPairRDD<String, Integer> numeroOnibus = agrupaOnibus.reduceByKey((x, y) -> x + y);
              List<Tuple2<String, Integer>> lista = numeroOnibus.collect();
   
  // mostra as linhas e o número de ônibus da linha
              for (Tuple2<String, Integer> onibusNumero : lista) {
                    System.out.println("Linha: " + onibusNumero._1());
                    System.out.println("Número de ônibus: " + onibusNumero._2());
              }
   
        }
   
  }  