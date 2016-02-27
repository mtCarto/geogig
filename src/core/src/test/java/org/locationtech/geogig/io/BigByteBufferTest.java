package org.locationtech.geogig.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

@Ignore
public class BigByteBufferTest {

    private final int partitionSize = 10;

    private final int bufferSize = 100;

    private ByteBufferStore spiedStore;

    private BigByteBuffer buff;

    @Rule
    public ExpectedException ex = ExpectedException.none();

    @Rule
    public TestName testName = new TestName();

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Before
    public void before() throws IOException {
        String testMethodName = testName.getMethodName();
        Path file = tmpFolder.getRoot().toPath()
                .resolve(getClass().getSimpleName() + "." + testMethodName);
        spiedStore = spy(new MappedFileByteBufferStore(file, true));
        buff = new BigByteBuffer(spiedStore, partitionSize, true);
    }

    @After
    public void after() {
        if (buff != null) {
            buff.discard();
        }
    }

    @Test
    public void testPutIntInt() {
        buff.position(0);
        buff.putInt(9);
        assertEquals(4, buff.position());

        buff.position(partitionSize - 2);
        buff.putInt(Integer.MAX_VALUE);
        assertEquals(partitionSize + 2, buff.position());

        assertEquals(9, buff.getInt(0));
        assertEquals(Integer.MAX_VALUE, buff.getInt(partitionSize - 2));

        ex.expect(BufferOverflowException.class);
        ex.expectMessage("pos: " + (buff.capacity() - 3));
        buff.position(buff.capacity() - 3);
        buff.putInt(17);
    }

    @Test
    public void testPutIntLongInt() {
        fail("Not yet implemented");
    }

    @Test
    public void testPutLongLong() {
        fail("Not yet implemented");
    }

    @Test
    public void testPutLongLongLong() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetLong() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetLongLong() {
        fail("Not yet implemented");
    }

    @Test
    public void testGet() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetLong1() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetAndSetPosition() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetByteArrayIntInt() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetLongByteArrayIntInt() {
        fail("Not yet implemented");
    }

    @Test
    public void testPutByte() {
        fail("Not yet implemented");
    }

    @Test
    public void testPutLongByte() {
        fail("Not yet implemented");
    }

    @Test
    public void testPutByteArray() {
        fail("Not yet implemented");
    }

    @Test
    public void testPutByteArrayIntInt() {
        fail("Not yet implemented");
    }

    @Test
    public void testPutLongByteArrayIntInt() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetInt() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetIntLong() {
        fail("Not yet implemented");
    }

    @Test
    public void testSlice() {
        fail("Not yet implemented");
    }

    @Test
    public void testDuplicate() {
        fail("Not yet implemented");
    }

    @Test
    public void testToString() {
        fail("Not yet implemented");
    }

    @Test
    public void testBigBuffer() {
        fail("Not yet implemented");
    }

    @Test
    public void testLimit() {
        fail("Not yet implemented");
    }

    @Test
    public void testCapacity() {
        fail("Not yet implemented");
    }

    @Test
    public void testMark() {
        fail("Not yet implemented");
    }

    @Test
    public void testPosition() {
        fail("Not yet implemented");
    }

    @Test
    public void testRemaining() {
        fail("Not yet implemented");
    }

    @Test
    public void testHasRemaining() {
        fail("Not yet implemented");
    }

    @Test
    public void testReset() {
        fail("Not yet implemented");
    }

    @Test
    public void testClear() {
        fail("Not yet implemented");
    }

    @Test
    public void testFlip() {
        fail("Not yet implemented");
    }

    @Test
    public void testRewind() {
        fail("Not yet implemented");
    }

    @Test
    public void testLimitLong() {
        fail("Not yet implemented");
    }

    @Test
    public void testPositionLong() {
        fail("Not yet implemented");
    }

}
