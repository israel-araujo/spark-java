  package com.devmedia.transformation;
   
  import java.util.List;
   
  import org.apache.spark.SparkConf;
  import org.apache.spark.api.java.JavaRDD;
  import org.apache.spark.api.java.JavaSparkContext;
   
  public class Exemplo4 {
   
        public static void main(String[] args) {
              
  // configuração do Spark
              SparkConf conf = new SparkConf().setMaster("local").setAppName("BusProcessor");
              JavaSparkContext ctx = new JavaSparkContext(conf);
   
  // carrega os dados dos ônibus de sp de sábado e domingo
              JavaRDD<String> linhasSabado = ctx.textFile("c:/dev/teste7.log");
              JavaRDD<String> linhasDomingo = ctx.textFile("c:/dev/teste8.log");
   
  // filtra os ônibus pelo nome da linha
              JavaRDD<String> linhasFiltradasSabado = linhasSabado.filter(s -> s.contains("JD.BONFIGLIOLI"));
              JavaRDD<String> linhasFiltradasDomingo = linhasDomingo.filter(s -> s.contains("JD.BONFIGLIOLI"));
              
  // une os dados de sábado e domingo
              JavaRDD<String> linhasUniao = linhasFiltradasSabado.union(linhasFiltradasDomingo);
              
  // mostra os ônibus resultantes da união
              List<String> resultados = linhasUniao.collect();
              for (String linha : resultados) {
                    System.out.println(linha);
              }
              
              ctx.close();
              
        }
   
  }  