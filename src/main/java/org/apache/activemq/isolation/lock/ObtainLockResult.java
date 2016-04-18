package org.apache.activemq.isolation.lock;

public class ObtainLockResult {
    private String keyName;
    private boolean obtained;
    private boolean existing;

    public ObtainLockResult(String keyName, boolean obtained, boolean existing)  {
        // If it isn't obtained, it can't be existing
        assert((!obtained && existing) == false);

        this.keyName = keyName;
        this.obtained = obtained;
        this.existing = existing;
    }

    public String getKeyName() {
        return this.keyName;
    }

    public boolean getObtained() {
        return this.obtained;
    }

    public boolean getExisting() {
        return this.existing;
    }
}
