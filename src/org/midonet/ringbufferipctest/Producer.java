package org.midonet.ringbufferipctest;

import java.io.File;
import java.nio.MappedByteBuffer;

import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;

import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

public class Producer {
    public static void main(String[] args) {
        try {
            runProducer(args);
        } catch (Exception e) {
            System.out.print("Execution failed with an exception: ");
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    private static void runProducer(String[] args) throws Exception {
        final int MSGS_PER_SEC = 10000; //Integer.parseInt(args[0]);
        final int TOTAL_NUM_MSGS = 500_000_000; //Integer.parseInt(args[1]);
        final long totalNanos = 1_000_000_000 / MSGS_PER_SEC;
        final long delayMillis = totalNanos / 1_000_000;
        final int delayNanos = (int)(totalNanos % 1_000_000);

        File file = new File(Conf.SHARED_FILE_NAME);
        IoUtil.deleteIfExists(file);
        MappedByteBuffer mapped = IoUtil.mapNewFile(file, Conf.RING_BUFFER_SIZE + TRAILER_LENGTH);
        UnsafeBuffer underlying = new UnsafeBuffer(mapped);
        OneToOneRingBuffer buffer = new OneToOneRingBuffer(underlying);

        byte[] msg = new byte[Conf.MSG_SIZE];
        UnsafeBuffer dbMsg = new UnsafeBuffer(msg, 0, msg.length);

        while (buffer.producerPosition() != buffer.consumerPosition())
            Thread.sleep(1);

        for (int i=0; i<TOTAL_NUM_MSGS;) {
            if (buffer.write(i+1, dbMsg, 0, msg.length)) {
                ++i;
            }
            //Thread.sleep(delayMillis, delayNanos);
        }
    }
}
