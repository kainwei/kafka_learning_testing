package com.linuxacademy.ccdak.testing;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;

/**
 *
 * @author will
 */
public class MyProducerTest {

    MockProducer<Integer, String> mockProducer;
    MyProducer myProducer;

    //Contains data sent to System.out during the test
    private ByteArrayOutputStream systemOutContent;
    //Contains data sent to System.err during the test
    private ByteArrayOutputStream systemErrContent;
    private final PrintStream originalSystemOut = System.out;
    private final PrintStream originalSystemErr = System.err;

    @Before
    public void Setup() {
        mockProducer = new MockProducer<>(false, new IntegerSerializer(), new StringSerializer());
        myProducer = new MyProducer();
        myProducer.producer = mockProducer;
    }

    @Before
    public void setUpStream() {
        systemOutContent = new ByteArrayOutputStream();
        systemErrContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(systemOutContent));
        System.setErr(new PrintStream(systemErrContent));
    }

    @After
    public void restoreStreams() {
        System.setOut(originalSystemOut);
        System.setErr(originalSystemErr);
    }

    @Test
    public void testPublishRecord_sent_date() {
        //Perform a simple test to verify that the producer sends the correct data to the correct topic when publishRecord is called.
        myProducer.publishRecord(1, "Test Data");

        mockProducer.completeNext();

        List<ProducerRecord<Integer, String>> records = mockProducer.history();
        Assert.assertEquals(1, records.size());
        ProducerRecord<Integer, String> record = records.get(0);
        Assert.assertEquals("Test Data", record.value());
        Assert.assertEquals("test_topic", record.topic());
        Assert.assertEquals("key=1, value=Test Data\n", systemOutContent.toString());
    }
    
}
