package cntic.plugin;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cntic.process.ContainerData;

public class SparkMesosContainer extends ProcessPlugin {

  private static final Pattern CONTAINER_ID_FORMAT = Pattern
      .compile("^sh.-c.+org.apache.spark.executor.MesosExecutorBackend");

  public SparkMesosContainer(float rate) {
    super(rate);
  }

  @Override
  public ContainerData apply(String cmdLine, int pid) {
    Matcher m = CONTAINER_ID_FORMAT.matcher(cmdLine);
    if (m.find()) {
      String clsId = "MesosSpark_"
          + pid + "_" + System.currentTimeMillis();
      System.out.println("Found pattern! clsid: "
          + clsId);
      return new ContainerData(clsId, pid, rate);
    }
    return null;
  }

  public static void main(String[] args) {
    String cmdline =
        "sh -c/opt/spark/spark-1.3.1-bin-hadoop2.6/bin/spark-class org.apache.spark.executor.MesosExecutorBackend";

    Matcher m = CONTAINER_ID_FORMAT.matcher(cmdline);

    if (m.find()) {
      System.out.println("Found pattern!");
    }
  }
}
