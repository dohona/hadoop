package cntic.plugin;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cntic.process.ContainerData;

public class MesosLTContainer extends ProcessPlugin {

  private static final Pattern CONTAINER_ID_FORMAT =
      Pattern
          .compile("mesos-container.+/runs/([a-z\\d]+-[a-z\\d]+-[a-z\\d]+-[a-z\\d]+-[a-z\\d]+)");

  public MesosLTContainer(float rate) {
    super(rate);
  }

  @Override
  public ContainerData apply(String cmdLine, int pid) {
    Matcher m = CONTAINER_ID_FORMAT.matcher(cmdLine);
    if (m.find()) {
      String clsId = m.group(1);
      System.out.println("Found pattern! clsid: "
          + clsId);
      return new ContainerData(clsId, pid, rate);
    }
    return null;
  }

  public static void main(String[] args) {
    String cmdline =
        "/home/apuser2/mesos-0.21.0/build/src/.libs/lt-mesos-containerizer launch --command={\"environment\":{\"variables\":[{\"name\":\"SPARK_EXECUTOR_OPTS\",\"value\":\"\"},{\"name\":\"SPARK_USER\",\"value\":\"apuser2\"},{\"name\":\"SPARK_EXECUTOR_MEMORY\",\"value\":\"2048m\"}]},\"shell\":true,\"uris\":[{\"extract\":true,\"value\":\"hdfs:\\/\\/10.10.0.190:9000\\/spark-1.3.1-bin-hadoop2.6.tgz\"}],\"value\":\"cd spark-1*;  .\\/bin\\/spark-class org.apache.spark.executor.MesosExecutorBackend\"} --commands={\"commands\":[]} --directory=/home/apuser2/mesos-0.21.0/build/work/slaves/20150602-143120-3187673610-5050-32529-S0/frameworks/20150602-143120-3187673610-5050-32529-0003/executors/20150602-143120-3187673610-5050-32529-S0/runs/36c1b78a-6002-401d-a19f-afbe378daf2b --pipe_read=9 --pipe_write=10 --user=apuser2";
    ;

    Matcher m = CONTAINER_ID_FORMAT.matcher(cmdline);

    if (m.find()) {
      System.out.println("Found pattern: "
          + m.group(1));
    }
  }
}
