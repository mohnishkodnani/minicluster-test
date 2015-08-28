import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;

public class HBaseTestingUtilityInitializer {
        /**
         * This is a utility class. We should not be able to instantiate it.
         */
        private HBaseTestingUtilityInitializer() { }

        /**
         * Fetch an initialized HBaseTestingUtility.
         * @return The initialized HBaseTestingUtility.
         */
        public static HBaseTestingUtility getHBaseTestingUtility() {
            System.out.println("Configuring hbase testing util.");
            Configuration configuration = createInitialMiniClusterConfiguration();
            HBaseTestingUtility testingUtility = new HBaseTestingUtility(configuration);
            loadOSSpecificConfiguration(configuration);
            configuration.reloadConfiguration();
            return testingUtility;
        }

        public static Configuration createInitialMiniClusterConfiguration() {
            //use local jobtracker and local filesystem in the initial configuration
            //change as needed for if dfs and minimr are started
            Configuration initialConfig = HBaseConfiguration.create();

            //So the HBase Master Web UI does not run
            initialConfig.set("hbase.master.info.port", "-1");
            //So the HBase RegsionServer Web UI does not run
            initialConfig.set("hbase.regionserver.info.port", "-1");
            initialConfig.set("hbase.regionserver.codecs", "gz");
            initialConfig.set("hbase.master.logcleaner.plugins", "org.apache.hadoop.hbase.master.cleaner.TimeToLiveLogCleaner");
            initialConfig.set("hbase.zookeeper.quorum", "localhost");
            initialConfig.set("hbase.client.retries.number", "4");
            initialConfig.set("hbase.zookeeper.property.maxClientCnxns", "100");
            initialConfig.set("test.hbase.zookeeper.property.clientPort", "2181");

            initialConfig.set("hadoop.security.authentication", "simple");
            initialConfig.set("hadoop.security.authorization", "false");
            initialConfig.set("hadoop.proxyuser." + System.getProperty("user.name") + ".oozie.groups", "*");
            initialConfig.set("hadoop.proxyuser." + System.getProperty("user.name") + ".oozie.hosts", "*");
            initialConfig.set("hadoop.root.logger", "DEBUG");

            initialConfig.set("yarn.application.classpath", System.getProperty("java.class.path"));
            initialConfig.set("yarn.minicluster.fixed.ports", "true");
            initialConfig.set("yarn.resourcemanager.hostname", "localhost");
            initialConfig.set("yarn.nodemanager.hostname", "localhost");
            initialConfig.set("yarn.scheduler.capacity.root.queues", "default");
            initialConfig.set("yarn.scheduler.capacity.root.default.capacity", "100");
//        initialConfig.setDouble("yarn.scheduler.capacity.maximum-am-resource-percent", 0.5);
            initialConfig.set("yarn.nodemanager.resource.memory-mb", "4096");
            initialConfig.set("yarn.scheduler.minimum-allocation-mb", "512");


            initialConfig.set("mapreduce.map.maxattempts", "1");
            initialConfig.set("mapreduce.reduce.maxattempts", "1");
            initialConfig.set("mapreduce.framework.name", "yarn");
            initialConfig.set("mapred.job.tracker", "local");
            initialConfig.set("mapreduce.map.memory.mb", "1024");
            initialConfig.set("mapreduce.map.java.opts", "-Xmx512m");

            initialConfig.set("fs.defaultFS", "file:///");
            initialConfig.set("dfs.client.block.write.replace-datanode-on-failure.enable", "false");
            initialConfig.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
            initialConfig.set("dfs.support.append", "true");
            initialConfig.setBoolean("dfs.permissions", false);
            initialConfig.setBoolean("dfs.permissions.enabled", false);

            initialConfig.set("zookeeper.session.timeout", "60000");
            initialConfig.set("zookeeper.recovery.retry", "1");

            return initialConfig;
        }

        /**
         * Set configuration parameters specific to each OS.
         * @param conf The configuration.
         */
        private static void loadOSSpecificConfiguration(final Configuration conf) {
            String osName = System.getProperty("os.name");
            if (osName != null && osName.toUpperCase().indexOf("WINDOWS") != -1) {
                //Windows has had issues with certain compression codecs
                conf.set("hbase.regionserver.codecs", "");
            } else {
                conf.set("hbase.regionserver.codecs", "gz");
            }
        }
}
