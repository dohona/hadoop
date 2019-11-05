package cntic.plugin;

import cntic.process.ContainerData;

public abstract class ProcessPlugin {
  float rate;

  public abstract ContainerData apply(String cmdLine, int pid);

  public ProcessPlugin(float rate) {
    this.rate = rate;
  }
}
