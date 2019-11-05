package cntic.plugin;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cntic.process.ContainerData;

public class YarnContainer extends ProcessPlugin {

  static final Pattern CONTAINER_ID_FORMAT = Pattern
      .compile("container-executor.+(container_\\d+_\\d+_\\d+_\\d+)");

  public YarnContainer(float rate) {
    super(rate);
  }

  @Override
  public ContainerData apply(String cmdLine, int pid) {
    Matcher m = CONTAINER_ID_FORMAT.matcher(cmdLine);
    if (m.find()) {
      String clsId = m.group(1);
      System.out.println("Found pattern! clsid: "
          + clsId);
      return new ContainerData(clsId, pid, 0);
    }
    return null;
  }

}
