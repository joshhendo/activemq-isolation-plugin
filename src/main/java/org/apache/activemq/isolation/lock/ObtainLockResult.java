package org.apache.activemq.isolation.lock;

public class ObtainLockResult {
    private Lock lock;
    private boolean obtained;
    private boolean existing;

    public ObtainLockResult(Lock lock, boolean obtained, boolean existing)  {
        // If it isn't obtained, it can't be existing
        assert((!obtained && existing) == false);

        this.lock = lock;
        this.obtained = obtained;
        this.existing = existing;
    }

    public Lock getLock() {
        return this.lock;
    }

    public boolean getObtained() {
        return this.obtained;
    }

    public boolean getExisting() {
        return this.existing;
    }
}
