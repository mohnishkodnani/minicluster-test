/**
 * Created by mkodnani on 8/28/15.
 */
public enum HBaseColumnFamilyConstants {

    /** Column family containing Change set information **/
    CHANGE_SET_CF ("B"),

    /** Column family containing main information **/
    MAIN_CF ("A"),

    /** Column family containing partial document information **/
    PARTAIL_DOCUMENT ("D"),

    /** Column family containing audit trail information **/
    AUDIT_TRAIL_CF ("C");

    private String physicalColumnFamilyName;

    private HBaseColumnFamilyConstants(String physicalColumnFamilyName) {
        this.physicalColumnFamilyName = physicalColumnFamilyName;
    }

    public String getPhysicalColumnFamilyName() {
        return physicalColumnFamilyName;
    }
}
