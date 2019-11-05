package cntic.plugin;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cntic.process.ContainerData;

public class FlumeAgent extends ProcessPlugin {

	private static final Pattern CONTAINER_ID_FORMAT = Pattern.compile("(org\\.apache\\.flume\\.node\\.Application)");

	public FlumeAgent(float rate) {
		super(rate);
	}

	@Override
	public ContainerData apply(String cmdLine, int pid) {
		Matcher m = CONTAINER_ID_FORMAT.matcher(cmdLine);
		if (m.find()) {
			String clsId = String.format("FlumeAgent_%d_%d", pid, System.currentTimeMillis());
			System.out.println("Found pattern! clsid: " + clsId);
			return new ContainerData(clsId, pid, rate);
		}
		return null;
	}

	public static void main(String[] args) {
		String cmdline = "/usr/lib/jvm/java-9-oracle/bin/java -Xms2048m -Xmx6144m -Dcom.sun.management.jmxremote -Dflume.monitoring.type=http -Dflume.monitoring.port=5653 -cp /opt/flume/conf:/opt/flume/lib/*:/opt/yarn/conf:/opt/yarn/hadoop/share/hadoop/common/lib/*:/opt/yarn/hadoop/share/hadoop/common/*:/opt/yarn/hadoop/share/hadoop/hdfs:/opt/yarn/hadoop/share/hadoop/hdfs/lib/*:/opt/yarn/hadoop/share/hadoop/hdfs/*:/opt/yarn/hadoop/share/hadoop/mapreduce/*:/opt/yarn/hadoop/share/hadoop/yarn/lib/*:/opt/yarn/hadoop/share/hadoop/yarn/*:/lib/* -Djava.library.path=:/opt/yarn/hadoop/lib/native org.apache.flume.node.Application --conf-file conf/to-hdfs.conf --name agent2";
		Matcher m = CONTAINER_ID_FORMAT.matcher(cmdline);

		if (m.find()) {
			System.out.println("Found pattern!");
		}
	}
}
