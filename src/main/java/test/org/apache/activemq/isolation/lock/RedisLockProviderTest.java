package test.org.apache.activemq.isolation.lock;

import org.apache.activemq.isolation.lock.CorrelationIdLocks;
import org.apache.activemq.isolation.lock.RedisLockProvider;
import org.junit.Before;
import org.junit.Test;
import org.redisson.core.RMap;

import java.util.HashMap;

public class RedisLockProviderTest {
    private RedisLockProvider lockProvider;

    @Before
    public void setUp() throws Exception {
        this.lockProvider = new RedisLockProvider();
    }

    @Test
    public void obtainLocksForMessage() {
        String messageId = "message_id";
        String correlationId = "message_id";
        String messageName = "addUser";
        HashMap<String, String> keys = new HashMap<String, String>();
        keys.put("UserId", "123456");

        this.lockProvider.obtainLocksForMessage(messageId, correlationId, messageName, keys);
    }

    @Test
    public void redisTesting() {
        RMap<String, String> testMap = lockProvider.redisson.getMap("TEST_MAP");
        RMap<String, String> otherMap = lockProvider.redisson.getMap("OTHER_MAP");
        testMap.put("test", "value");
        String val = otherMap.get("test");
        System.out.println(val);
    }
}
