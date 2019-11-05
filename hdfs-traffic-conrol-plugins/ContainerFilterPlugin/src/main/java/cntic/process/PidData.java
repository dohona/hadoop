package cntic.process;

import java.util.HashSet;
import java.util.Set;

public class PidData {
  String pid;
  String ppid;

  protected Set<PidData> children = new HashSet<>();

  public PidData(String pid, String ppid) {
    this.pid = pid;
    this.ppid = ppid;
  }

  public PidData(String pid) {
    this(pid, null);
  }

  /**
   * @return the ppid
   */
  public String getPpid() {
    return ppid;
  }

  /**
   * @param ppid
   *          the ppid to set
   */
  public void setPpid(String ppid) {
    this.ppid = ppid;
  }

  /**
   * @return the pid
   */
  public String getPid() {
    return pid;
  }

  /**
   * @return the children
   */
  public Set<PidData> getChildren() {
    return children;
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
        * result + ((pid == null) ? 0 : pid.hashCode());
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
    PidData other = (PidData) obj;
    if (pid == null) {
      if (other.pid != null)
        return false;
    } else if (!pid.equals(other.pid))
      return false;
    return true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "PidData [pid="
        + pid + ", ppid=" + ppid + ", children=" + children + "]";
  }

}
