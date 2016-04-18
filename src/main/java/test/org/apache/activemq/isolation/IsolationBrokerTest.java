package test.org.apache.activemq.isolation;

import org.apache.activemq.isolation.IsolationBroker;

import org.apache.activemq.isolation.lock.LockProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class IsolationBrokerTest {
    private IsolationBroker isolationBroker;

    @Before
    public void setUp() throws Exception {
        this.isolationBroker = new IsolationBroker(null, new ArrayList<String>());
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void send() throws Exception {
        assert(true);
    }

}