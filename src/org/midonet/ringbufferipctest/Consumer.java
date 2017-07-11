package org.midonet.ringbufferipctest;

import java.io.File;
import java.nio.MappedByteBuffer;

import org.agrona.IoUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;

import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

public class Consumer {

    private static class ConsumerHandler implements MessageHandler {

        public long counter = Long.MIN_VALUE;

        @Override
        public void onMessage(int msgTypeId, MutableDirectBuffer buffer,
                              int index,
                              int length) {
            if (msgTypeId != counter) {
                if (counter == Long.MIN_VALUE)
                    counter = msgTypeId;
               else
                    throw new RuntimeException("Expected id " + counter +" not " + msgTypeId);
            }
            if (length != Conf.MSG_SIZE) {
                throw new RuntimeException("Expected len " + Conf.MSG_SIZE + " not " + length);
            }
            counter ++;
        }
    }

    public static void main(String[] args) {
        try {
            runConsumer(args);
        } catch (Exception e) {
            System.out.print("Execution failed with an exception: ");
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    private static void runConsumer(String[] args) throws InterruptedException {
        File file = new File(Conf.SHARED_FILE_NAME);
        while (!file.exists() || file.length() != Conf.RING_BUFFER_SIZE + TRAILER_LENGTH)
            Thread.sleep(0, 100);
        MappedByteBuffer mapped = IoUtil.mapExistingFile(file, "The Ring Buffer");
        UnsafeBuffer underlying = new UnsafeBuffer(mapped);
        OneToOneRingBuffer buffer = new OneToOneRingBuffer(underlying);

        ConsumerHandler handler = new ConsumerHandler();

        while (buffer.producerPosition() == 0)
            Thread.sleep(0, 100);

        long start = System.nanoTime();
        long end = start;
        for (int zeros = 0; /*zeros < 10000*/;) {
            if (buffer.read(handler) != 0) {
                zeros = 0;
            } else {
                if (zeros == 0) end = System.nanoTime();
                zeros ++;
                //Thread.sleep(0,10);
                if (System.nanoTime() - end > 100_000_000) break;
            }
        }

        double took = (end - start) / 1_000_000_000.0;
        long count = handler.counter - 1;
        System.out.println("Received " + count
                           + " messages in " + took + " s. ("
                           + ((long)count/took) + " msg/s)");
        IoUtil.deleteIfExists(file);
    }
}
