package org.redisson;

import org.junit.jupiter.api.Test;
import org.redisson.api.RStream;
import org.redisson.api.RStreamReactive;
import org.redisson.api.stream.*;
import reactor.test.StepVerifier;

public class RedissonStreamReactiveTest extends BaseReactiveTest {

    @Test
    public void testReadEmptyReactive() {
        RStreamReactive<String, String> stream = redisson.getStream("test");

        stream.read(StreamReadArgs.greaterThan(new StreamMessageId(0, 0)).count(10))
                .as(StepVerifier::create)
                .verifyComplete();
    }

    @Test
    public void testReadGroupEmptyReactive() {
        RStream<String, String> stream = RedisDockerTest.redisson.getStream("test");
        stream.createGroup(StreamCreateGroupArgs.name("testGroup").makeStream());
        stream.add(StreamAddArgs.entry("1", "1"));
        // Consume the only entry so the next neverDelivered() read has nothing.
        stream.readGroup("testGroup", "consumer1", StreamReadGroupArgs.neverDelivered());

        RStreamReactive<String, String> streamReactive = redisson.getStream("test");

        streamReactive.readGroup("testGroup", "consumer1", StreamReadGroupArgs.neverDelivered())
                .as(StepVerifier::create)
                .verifyComplete();
    }

    @Test
    public void testReadMultiEmptyReactive() {
        RStreamReactive<String, String> stream = redisson.getStream("test1");

        stream.read(StreamMultiReadArgs.greaterThan(new StreamMessageId(0, 0), "test2", new StreamMessageId(0, 0))
                        .count(10))
                .as(StepVerifier::create)
                .verifyComplete();
    }

    @Test
    public void testReadGroupMultiEmptyReactive() {
        RStream<String, String> stream1 = RedisDockerTest.redisson.getStream("test1");
        RStream<String, String> stream2 = RedisDockerTest.redisson.getStream("test2");
        stream1.createGroup(StreamCreateGroupArgs.name("testGroup").makeStream());
        stream2.createGroup(StreamCreateGroupArgs.name("testGroup").makeStream());
        stream1.add(StreamAddArgs.entry("1", "1"));
        stream2.add(StreamAddArgs.entry("1", "1"));
        stream1.readGroup("testGroup", "consumer1", StreamMultiReadGroupArgs.greaterThan(
                StreamMessageId.NEVER_DELIVERED, "test2", StreamMessageId.NEVER_DELIVERED));

        RStreamReactive<String, String> streamReactive = redisson.getStream("test1");

        streamReactive.readGroup("testGroup", "consumer1", StreamMultiReadGroupArgs.greaterThan(
                        StreamMessageId.NEVER_DELIVERED, "test2", StreamMessageId.NEVER_DELIVERED))
                .as(StepVerifier::create)
                .verifyComplete();
    }

}