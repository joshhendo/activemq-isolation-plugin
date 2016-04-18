package test.org.apache.activemq.isolation.lock;

import org.apache.activemq.isolation.lock.LockProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class LockProviderTest {
    private LockProvider lockProvider;

    @Before
    public void setUp() throws Exception {
        this.lockProvider = new LockProvider();
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void obtainLocks() throws Exception {
        //this.lockProvider.obtainLocks();
    }

    @Test
    public void releaseLock() throws Exception {

    }

}