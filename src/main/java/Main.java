import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class Main {
    private HBaseTestingUtility hBaseTestingUtility;
    private static final String CHANGE_SET_CF_PHYSICAL_NAME = HBaseColumnFamilyConstants.CHANGE_SET_CF.getPhysicalColumnFamilyName();
    private static final String MAIN_CF_PHYSICAL_NAME = HBaseColumnFamilyConstants.MAIN_CF.getPhysicalColumnFamilyName();
    private static final String AUDIT_TRAIL_CF_PHYSICAL_NAME = HBaseColumnFamilyConstants.AUDIT_TRAIL_CF.getPhysicalColumnFamilyName();
    private static final String PARTIAL_DOCUMENT_CF_PHYSICAL_NAME = HBaseColumnFamilyConstants.PARTAIL_DOCUMENT.getPhysicalColumnFamilyName();
    private HBaseAdmin hbaseAdmin;
    private Configuration conf;
    public static void main(String[] args) {
        Main main = new Main();
        try {
            main.setUp();
            main.tearDown();
            main.setUp();
            main.tearDown();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void setUp() throws Exception {
        System.out.println("Inside Setup");
        hBaseTestingUtility = HBaseTestingUtilityInitializer.getHBaseTestingUtility();
        Configuration conf = hBaseTestingUtility.getConfiguration();
        hBaseTestingUtility.startMiniCluster(1);

        byte[][]  families = new byte[][]{
                Bytes.toBytes(MAIN_CF_PHYSICAL_NAME),
                Bytes.toBytes(CHANGE_SET_CF_PHYSICAL_NAME),
                Bytes.toBytes(AUDIT_TRAIL_CF_PHYSICAL_NAME),
                Bytes.toBytes(PARTIAL_DOCUMENT_CF_PHYSICAL_NAME)
        };
        if (hBaseTestingUtility.createTable(Bytes.toBytes("item_local_test"), families, conf) == null) {
            throw new Exception("Could not create table");
        }
        System.out.println("Done Setup");
    }

    public void tearDown() throws Exception {
        System.out.println("Inside tearDown");
        hBaseTestingUtility.shutdownMiniCluster();
        System.out.println("Done tearDown");
    }
}
