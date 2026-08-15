package org.redisson.rx;

import io.reactivex.rxjava3.observers.TestObserver;
import org.junit.jupiter.api.Test;
import org.redisson.RedisDockerTest;
import org.redisson.api.RStream;
import org.redisson.api.RStreamRx;
import org.redisson.api.stream.*;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RedissonStreamRxTest extends BaseRxTest {

    @Test
    public void testReadEmptyRx() {
        RStreamRx<String, String> stream = redisson.getStream("test");
        TestObserver<Map<StreamMessageId, Map<String, String>>> observer = stream.read(
                StreamReadArgs.greaterThan(new StreamMessageId(0, 0)).count(10)).test();

        assertNoValues(observer);
    }

    private static void assertNoValues(TestObserver<?> observer) {
        observer.awaitDone(1, TimeUnit.SECONDS);
        observer.assertNoErrors();
        observer.assertComplete();
        observer.assertNoValues();
    }

    @Test
    public void testReadGroupEmptyRx() {
        RStream<String, String> stream = RedisDockerTest.redisson.getStream("test");
        stream.createGroup(StreamCreateGroupArgs.name("testGroup").makeStream());
        stream.add(StreamAddArgs.entry("1", "1"));
        stream.readGroup("testGroup", "consumer1", StreamReadGroupArgs.neverDelivered());

        RStreamRx<String, String> streamRx = redisson.getStream("test");
        TestObserver<Map<StreamMessageId, Map<String, String>>> observer = streamRx.readGroup(
                "testGroup", "consumer1", StreamReadGroupArgs.neverDelivered()).test();
        assertNoValues(observer);
    }

    @Test
    public void testReadMultiEmptyRx() {
        RStreamRx<String, String> stream = redisson.getStream("test1");
        TestObserver<Map<String, Map<StreamMessageId, Map<String, String>>>> observer = stream.read(
                        StreamMultiReadArgs.greaterThan(new StreamMessageId(0, 0), "test2", new StreamMessageId(0, 0)).count(10))
                .test();
        assertNoValues(observer);
    }

    @Test
    public void testReadGroupMultiEmptyRx() {
        RStream<String, String> stream1 = RedisDockerTest.redisson.getStream("test1");
        RStream<String, String> stream2 = RedisDockerTest.redisson.getStream("test2");
        stream1.createGroup(StreamCreateGroupArgs.name("testGroup").makeStream());
        stream2.createGroup(StreamCreateGroupArgs.name("testGroup").makeStream());
        stream1.add(StreamAddArgs.entry("1", "1"));
        stream2.add(StreamAddArgs.entry("1", "1"));
        stream1.readGroup("testGroup", "consumer1", StreamMultiReadGroupArgs.greaterThan(
                StreamMessageId.NEVER_DELIVERED, "test2", StreamMessageId.NEVER_DELIVERED));

        RStreamRx<String, String> streamRx = redisson.getStream("test1");
        TestObserver<Map<String, Map<StreamMessageId, Map<String, String>>>> observer = streamRx.readGroup(
                        "testGroup", "consumer1", StreamMultiReadGroupArgs.greaterThan(
                                StreamMessageId.NEVER_DELIVERED, "test2", StreamMessageId.NEVER_DELIVERED))
                .test();
        assertNoValues(observer);
    }

}