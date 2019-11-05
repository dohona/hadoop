package cntic.plugin;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cntic.process.ContainerData;

public class HdfsWriter extends ProcessPlugin {

	private static final Pattern CONTAINER_ID_FORMAT = Pattern
			.compile("(org\\.apache\\.hadoop\\.fs\\.HdfsWriter(FixLength)?)");

	public HdfsWriter(float rate) {
		super(rate);
	}

	@Override
	public ContainerData apply(String cmdLine, int pid) {
		Matcher m = CONTAINER_ID_FORMAT.matcher(cmdLine);
		if (m.find()) {
			String clsId = String.format("HdfsWriter_%d_%d", pid, System.currentTimeMillis());
			System.out.println("Found pattern! clsid: " + clsId);
			return new ContainerData(clsId, pid, rate);
		}
		return null;
	}

	public static void main(String[] args) {
		String cmdline = "/usr/lib/jvm/java-9-oracle/bin/java -Dproc_jar -Djava.net.preferIPv4Stack=true -Djava.net.preferIPv4Stack=true -Dyarn.log.dir=/opt/yarn/logs -Dyarn.log.file=hadoop.log -Dyarn.home.dir=/opt/yarn/hadoop -Dyarn.root.logger=INFO,console -Djava.library.path=/opt/yarn/hadoop/lib/native -Dhadoop.log.dir=/opt/yarn/logs -Dhadoop.log.file=hadoop.log -Dhadoop.home.dir=/opt/yarn/hadoop -Dhadoop.id.str=cntic -Dhadoop.root.logger=INFO,console -Dhadoop.policy.file=hadoop-policy.xml -Dhadoop.security.logger=INFO,NullAppender org.apache.hadoop.util.RunJar hdfs-writer.jar org.apache.hadoop.fs.HdfsWriterFixLength -Ddfs.blocksize=2G -Ddfs.client.cache.readahead=0 -Ddfs.client.cache.drop.behind.reads=true -Ddfs.client.cache.drop.behind.writes=true -Dhdfswriter.total.mb=8192 /io-benchmark/hdfswriter-output";
		Matcher m = CONTAINER_ID_FORMAT.matcher(cmdline);

		if (m.find()) {
			System.out.println("Found pattern!");
		}
	}
}
