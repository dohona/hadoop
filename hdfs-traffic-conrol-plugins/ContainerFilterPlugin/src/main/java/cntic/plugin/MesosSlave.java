package cntic.plugin;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cntic.process.ContainerData;

public class MesosSlave extends ProcessPlugin {

  private static final Pattern CONTAINER_ID_FORMAT = Pattern
      .compile("lt-mesos-slave");

  public MesosSlave(float rate) {
    super(rate);
  }

  @Override
  public ContainerData apply(String cmdLine, int pid) {
    Matcher m = CONTAINER_ID_FORMAT.matcher(cmdLine);
    if (m.find()) {
      String clsId = "MesosSlave_"
          + pid + "_" + System.currentTimeMillis();
      System.out.println("Found pattern! clsid: "
          + clsId);
      return new ContainerData(clsId, pid, 0);
    }
    return null;
  }
}
