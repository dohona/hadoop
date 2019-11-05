package cntic.process;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cntic.plugin.FlumeAgent;
import cntic.plugin.HdfsFsShel;
import cntic.plugin.HdfsReader;
import cntic.plugin.HdfsWriter;
import cntic.plugin.MesosLTContainer;
import cntic.plugin.MesosSlave;
import cntic.plugin.ProcessPlugin;
import cntic.plugin.SparkMesosContainer;
import cntic.plugin.YarnContainer;

public class BuildProcessTree extends Thread {

  static final Log LOG = LogFactory.getLog(BuildProcessTree.class);

  public static final Pattern validPid = Pattern.compile("^(\\d+)$");

  enum cType {
    NATIVE_SHELL, MESOS, SPARK, YARN
  }

  public static final String PROCSROOT = "/proc";

  private Map<String, ContainerData> containerMap =
      new HashMap<String, ContainerData>();

  // 9607 (java) S 31586 9607 ...
  static final Pattern PROCFS_STAT_FILE_FORMAT = Pattern
      .compile("^([0-9-]+)\\s\\(([^\\s]+)\\)\\s[^\\s]\\s([0-9-]+)\\s.+");

  static final Pattern CONTAINER_ID_FORMAT = Pattern
      .compile("(container_\\d+_\\d+_\\d+_\\d+)");

  private boolean traceMode = false;

  private List<ProcessPlugin> plugins;

  private Map<String, PidData> procfsTree = new HashMap<String, PidData>();
  private Map<String, String> pidMap = new HashMap<String, String>();

  private Path storageDir;

  public BuildProcessTree(boolean traceMode, float rate) {
    this.traceMode = traceMode;
    plugins = new ArrayList<ProcessPlugin>();
    //plugins.add(new HdfsFsShel(rate));
    plugins.add(new HdfsReader(rate));
    plugins.add(new HdfsWriter(rate));
    //plugins.add(new FlumeAgent(rate));
    //plugins.add(new MesosLTContainer(rate));
    //plugins.add(new SparkMesosContainer(rate));
    //plugins.add(new YarnContainer(0));
    //plugins.add(new MesosSlave(0));
    storageDir =
        Paths
            .get(System.getProperty("java.io.tmpdir"), "monitoring_containers");
  }

  private void updateProcFsTree(final String pid, final String ppid) {

    if (!procfsTree.containsKey(ppid)) {
      // future parent process
      procfsTree.put(ppid, new PidData(ppid, null));
    }

    if (!procfsTree.containsKey(pid)) {
      procfsTree.put(pid, new PidData(pid, ppid));
    } else {// it is a parent of other process.
      procfsTree.get(pid).setPpid(ppid);
    }
    // Update parent
    procfsTree.get(ppid).getChildren().add(procfsTree.get(pid));
    pidMap.put(pid, ppid);
  }

  public static void main(String[] args) {

    boolean traceMode = false;
    float rate = 0;

    if (args != null) {
      for (int i = 0; i < args.length; i++) {
        if (args[i].equalsIgnoreCase("-trace")) {
          traceMode = true;
          continue;
        }

        if (args[i].equalsIgnoreCase("-rate")) {
          try {
            rate = Float.parseFloat(args[i + 1]);
          } catch (Exception e) {
            ;
          }
        }
      }
    }

    if (rate == 0) {
      traceMode = true;
    }

    if (traceMode) {
      rate = 0;
    }

    LOG.info("traceMode: "
        + traceMode);
    LOG.info("rate: "
        + rate);

    BuildProcessTree main = new BuildProcessTree(traceMode, rate);
    main.start();
  }

  private void registerContainer(ContainerData container) {
    String content =
        String.format("rate=%.2f%npid=%d%n", container.getRate(),
            container.getPid());
    Path containerFile = storageDir.resolve(container.getClsId());
    try {
      Files.write(containerFile, content.getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Thread#run()
   */
  @Override
  public void run() {
    String fileName = "PidCmdline.dat";
    try (FileWriter fw = new FileWriter(fileName, true);
        BufferedWriter bw = new BufferedWriter(fw);
        PrintWriter printWriter = new PrintWriter(bw);) {

      File file = new File(fileName);
      // file.delete();
      if (!file.exists()) {
        file.createNewFile();
      }

      // printWriter.write(newLine + msg);
      while (true) {
        String content = build();
        if (!content.isEmpty()) {
          printWriter.println(content);
          printWriter.flush();
        }
        Thread.sleep(1000l);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (FileNotFoundException e1) {
      e1.printStackTrace();
    } catch (IOException e1) {
      e1.printStackTrace();
    }
  }

  public String build() {
    Set<String> oldPids = new HashSet<>(pidMap.keySet());
    StringBuffer bf = new StringBuffer();

    boolean foundNewProcess = false;
    try (DirectoryStream<Path> directoryStream =
        Files.newDirectoryStream(Paths.get(PROCSROOT))) {
      for (Path dir : directoryStream) {
        if (Files.isDirectory(dir)) {
          String pid = dir.getFileName().toString();
          Matcher pidMatcher = validPid.matcher(pid);
          if (pidMatcher.matches()) {
            if (oldPids.contains(pid)) {
              oldPids.remove(pid);
              continue;
            }

            Path pidStat = Paths.get(PROCSROOT, pid, "stat");

            try (BufferedReader reader =
                Files.newBufferedReader(pidStat, StandardCharsets.UTF_8)) {
              String line = reader.readLine();
              Matcher m = PROCFS_STAT_FILE_FORMAT.matcher(line);
              if (m.find()) {
                foundNewProcess = true;
                String ppid = m.group(3);
                String cmdGrp = m.group(2);
                updateProcFsTree(pid, ppid);
                // if (cmdGrp.equals("(java)")) {
                Path pidCmdline = Paths.get(PROCSROOT, pid, "cmdline");
                try (BufferedReader reader1 =
                    Files.newBufferedReader(pidCmdline, StandardCharsets.UTF_8)) {
                  String cmdline = reader1.readLine();
                  if (cmdline == null) {
                    continue;
                  }
                  
                  cmdline.replaceAll("\0", " ");

                  LOG.debug("Pid: "
                      + pid + ", ppid: " + ppid + ", cmdGrp: " + cmdGrp
                      + ", cmdline: " + cmdline);
                  final int pidInt = Integer.valueOf(pid);

                  for (ProcessPlugin plugin : plugins) {
                    ContainerData container = plugin.apply(cmdline, pidInt);
                    if (container != null) {
                      // checkAndRegisterFirstPid(container);
                      containerMap.put(pid, container);
                      LOG.info("Register container with id: "
                          + container.getClsId() + ", pid: "
                          + container.getPid() + ", rate: "
                          + container.getRate());
                      if (!traceMode
                          && container.getRate() > 0) {
                        registerContainer(container);
                      }
                      break;
                    }
                  }
                }
              }
            } catch (Exception e) {
              ;
            }
          }
        }
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    }

    for (String pid : oldPids) {
      removeOldPid(pid);
    }

    if (foundNewProcess
        || !oldPids.isEmpty()) {
      for (ContainerData container : containerMap.values()) {
        checkAndRegisterFirstPid(container);
      }
    }

    return bf.toString();
  }

  private void checkAndRegisterFirstPid(ContainerData container) {
    LOG.debug("Check pidlist of "
        + container.getClsId());
    boolean updated =
        container.setFirstPid(procfsTree.get(Integer.toString(container
            .getPid())));
    if (updated) {
      LOG.info("Update container with id: "
          + container.getClsId() + ": pidList: "
          + container.getCurrentPIDList());
    }
  }

  private void removeOldPid(String pid) {
    LOG.debug("Clear data of pid: "
        + pid);
    ContainerData container = containerMap.remove(pid);
    if (container != null) {
      LOG.info("Remove container: "
          + container.getClsId() + ", pids: " + container.getCurrentPIDList());
      try {
    	Path containerFile = storageDir.resolve(container.getClsId());  
		Files.deleteIfExists(containerFile);
      } catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
      }
    }

    String ppid = pidMap.remove(pid);
    PidData pTree = procfsTree.get(pid);
    if (pTree != null
        && ppid != null && procfsTree.containsKey(ppid)) {
      procfsTree.get(ppid).getChildren().remove(pTree);
    }

    procfsTree.remove(pid);
  }
}
