package cntic.plugin;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cntic.process.ContainerData;

public class HdfsReader extends ProcessPlugin {

  private static final Pattern CONTAINER_ID_FORMAT = Pattern
      .compile("(org\\.apache\\.hadoop\\.fs\\.HdfsReader)");

  public HdfsReader(float rate) {
    super(rate);
  }

  @Override
  public ContainerData apply(String cmdLine, int pid) {
    Matcher m = CONTAINER_ID_FORMAT.matcher(cmdLine);
    if (m.find()) {
      String clsId =
          String.format("HdfsReader_%d_%d", pid, System.currentTimeMillis());
      System.out.println("Found pattern! clsid: "
          + clsId);
      return new ContainerData(clsId, pid, rate);
    }
    return null;
  }

  public static void main(String[] args) {
    String cmdline =
        "/usr/lib/jvm/java-7-oracle/bin/java-Dproc_dfs-Djava.net.preferIPv4Stack=true-Djava.net.preferIPv4Stack=true-Dyarn.log.dir=/opt/yarn/logs-Dyarn.log.file=hadoop.log-Dyarn.home.dir=/opt/yarn/hadoop-Dyarn.root.logger=INFO,console-Djava.library.path=/opt/yarn/hadoop/lib/native-Dhadoop.log.dir=/opt/yarn/logs-Dhadoop.log.file=hadoop.log-Dhadoop.home.dir=/opt/yarn/hadoop-Dhadoop.id.str=hduser-Dhadoop.root.logger=INFO,console-Dhadoop.policy.file=hadoop-policy.xml-Dhadoop.security.logger=INFO,NullAppenderorg.apache.hadoop.fs.FsShell-copyToLocal/data/spark/testfile-10G-bs2G";
    Matcher m = CONTAINER_ID_FORMAT.matcher(cmdline);

    if (m.find()) {
      System.out.println("Found pattern!");
    }
  }
}
