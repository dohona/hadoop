package cntic.process;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

public class ContainerData {
  private String clsId;
  private float rate;
  private int pid;

  private PidData processTree;

  private Map<String, PidData> containerPIDTree = new HashMap<>();

  public ContainerData(String clsId, int pid, float rate) {
    this.clsId = clsId;
    this.pid = pid;
    this.rate = rate;
  }

  /**
   * @return the clsId
   */
  public String getClsId() {
    return clsId;
  }

  /**
   * @param clsId
   *          the clsId to set
   */
  public void setClsId(String clsId) {
    this.clsId = clsId;
  }

  /**
   * @return the rate
   */
  public float getRate() {
    return rate;
  }

  /**
   * @param rate
   *          the rate to set
   */
  public void setRate(float rate) {
    this.rate = rate;
  }

  /**
   * @return the pid
   */
  public int getPid() {
    return pid;
  }

  /**
   * @param pid
   *          the pid to set
   */
  public void setPid(int pid) {
    this.pid = pid;
  }

  /**
   * @return the firstPid
   */
  public PidData getFirstPid() {
    return processTree;
  }

  /**
   * @param processTree
   *          the firstPid to set
   */
  public boolean setFirstPid(PidData commonProcessTree) {
    if (commonProcessTree == null) {
      System.err.println("commonProcessTree is null for "
          + pid);
      return false;
    }

    boolean hasChanges = false;
    processTree = commonProcessTree;

    final Set<String> oldPidsList = new HashSet<String>(getCurrentPIDList());

    buildPIDTree();

    if (!oldPidsList.containsAll(getCurrentPIDList())
        || !getCurrentPIDList().containsAll(oldPidsList)) {
      hasChanges = true;
    }

    return hasChanges;
  }

  public void buildPIDTree() {
    containerPIDTree.clear();

    LinkedList<PidData> allChildrenQueue = new LinkedList<>();
    allChildrenQueue.add(getFirstPid());
    // allChildrenQueue.addAll(getFirstPid().getChildren());

    while (!allChildrenQueue.isEmpty()) {
      PidData child = allChildrenQueue.remove();
      String childPid = child.getPid();

      if (!containerPIDTree.containsKey(childPid)) {
        containerPIDTree.put(childPid, child);
      }
      allChildrenQueue.addAll(child.getChildren());
    }
  }

  public Set<String> getCurrentPIDList() {
    return containerPIDTree.keySet();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime
        * result + ((clsId == null) ? 0 : clsId.hashCode());
    return result;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ContainerData other = (ContainerData) obj;
    if (clsId == null) {
      if (other.clsId != null)
        return false;
    } else if (!clsId.equals(other.clsId))
      return false;
    return true;
  }

}
