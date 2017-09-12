 kage com.devmedia.load.save;
   
  import org.apache.spark.SparkConf;
  import org.apache.spark.api.java.JavaRDD;
  import org.apache.spark.api.java.JavaSparkContext;
   
  public class Exemplo1 {
   
        public static void main(String[] args) {
              
  // configuração do Spark
              SparkConf conf = new SparkConf().setMaster("local").setAppName("BusProcessor");
              JavaSparkContext ctx = new JavaSparkContext(conf);
   
  // carrega os dados dos ônibus de sp
              JavaRDD<String> linhasSabado = ctx.textFile("c:/dev/teste7.log");
              JavaRDD<String> linhasDomingo = ctx.textFile("c:/dev/teste8.log");
   
  // filtra os ônibus pelo nome da linha
              JavaRDD<String> linhasFiltradasSabado = linhasSabado.filter(s -> s.contains("JD.BONFIGLIOLI"));
              JavaRDD<String> linhasFiltradasDomingo = linhasDomingo.filter(s -> s.contains("JD.BONFIGLIOLI"));
              
  // une os dados de sábado e domingo
              JavaRDD<String> linhasUniao = linhasFiltradasSabado.union(linhasFiltradasDomingo);
              
  // salva os dados da união em um arquivo
              linhasUniao.saveAsTextFile("c:/dev/onibus-uniao.txt");
              
              ctx.close();
              
        }
   
  }  