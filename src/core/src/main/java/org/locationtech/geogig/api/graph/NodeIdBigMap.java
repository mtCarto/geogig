package org.locationtech.geogig.api.graph;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.api.Node;
import org.locationtech.geogig.io.BigByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * Offers {@code java.util.Map} like interface to off-load {@link NodeId} to data offset entries to
 * a {@link BigByteBuffer}.
 * <p>
 * This class is NOT thread safe.
 */
public class NodeIdBigMap {

    private static final Logger LOG = LoggerFactory.getLogger(NodeIdBigMap.class);

    private static final int CONCURRENCY = 8;

    @VisibleForTesting
    BigByteBuffer dataBuffer;

    @VisibleForTesting
    FileHeader fileHeader;

    public NodeIdBigMap(BigByteBuffer dataBuffer) {
        this.dataBuffer = dataBuffer;
        this.fileHeader = FileHeader.create(dataBuffer);
    }

    /**
     * @return {@code -1} if not found, the entry value otherwise ({@link Node} offset)
     */
    public long get(NodeId id) {
        final Entry entry = new Entry(bucketHash(id), nameHash(id));
        final ReadWriteLock lock = fileHeader.getLock(entry);
        lock.readLock().lock();
        try {
            Page page = fileHeader.getRootPage();
            long dataOffset = page.find(entry);
            return dataOffset;
        } finally {
            lock.readLock().unlock();
        }
    }

    static final HashFunction HF1 = Hashing.sipHash24();

    static final HashFunction HF2 = Hashing.murmur3_32();

    public int put(final NodeId id, final long dataOffset) {
        final Entry entry = new Entry(bucketHash(id), nameHash(id), dataOffset);
        return put(entry);
    }

    private int put(final Entry entry) {
        final ReadWriteLock lock = fileHeader.getLock(entry);
        lock.writeLock().lock();
        try {
            Page page = fileHeader.getRootPage();

            int insertionIndex = page.put(entry);

            return insertionIndex;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public long size() {
        long size = fileHeader.getRootPage().size();
        return size;
    }

    long sizeRecursive() {
        long size = fileHeader.getRootPage().sizeRecursive();
        return size;
    }

    public void dispose() {
        BigByteBuffer fileBuffer = this.dataBuffer;
        this.dataBuffer = null;
        if (fileBuffer != null) {
            fileBuffer.discard();
            this.fileHeader = null;
        }
    }

    private byte nameHash(NodeId id) {
        return (byte) id.name.hashCode();
    }

    private/* unsigned */long bucketHash(NodeId id) {
        return ((CanonicalNodeId) id).bucketsByDepth().longValue();
    }

    /**
     * <pre>
     * <code>
     *  - long[TABLE_SEGMENTS]: offsets of pages at each table index. Zero means no page exists for that table entry/
     * </code>
     * </pre>
     */
    static final class FileHeader {

        final ReadWriteLock[] segmentLocks;

        final BigByteBuffer dataBuffer;

        final AtomicLong dataPageCount;

        final AtomicLong bucketPageCount;

        final AtomicLong nextFreePageOffset;

        final AtomicLong dataPagesSize = new AtomicLong(), bucketPagesSize = new AtomicLong();

        final Map<Short, BlockingQueue<Number>> freeDataPageOffsetsByCapacity;

        private final BucketPage rootPage;

        public FileHeader(BigByteBuffer dataBuffer) {
            this.dataBuffer = dataBuffer;
            this.nextFreePageOffset = new AtomicLong();
            this.dataPageCount = new AtomicLong();
            this.bucketPageCount = new AtomicLong();
            this.freeDataPageOffsetsByCapacity = new ConcurrentHashMap<>();

            this.rootPage = createBucketPage(0);

            this.segmentLocks = new ReadWriteLock[CONCURRENCY];
            for (int i = 0; i < this.segmentLocks.length; i++) {
                segmentLocks[i] = new ReentrantReadWriteLock();
            }

        }

        ReadWriteLock getLock(final Entry entry) {
            int index = entry.encodedKey[0] & 0xFF;
            final ReadWriteLock lock = segmentLocks[index % CONCURRENCY];
            return lock;
        }

        static FileHeader create(BigByteBuffer dataBuffer) {
            return new FileHeader(dataBuffer);
        }

        public Page getRootPage() {
            return rootPage;
        }

        private BlockingQueue<Number> getFreeOffsets(final short capacity) {
            final Short key = Short.valueOf(capacity);
            BlockingQueue<Number> queue = this.freeDataPageOffsetsByCapacity.get(key);
            if (queue == null) {
                queue = new ArrayBlockingQueue<Number>(1024);
                BlockingQueue<Number> existing = freeDataPageOffsetsByCapacity.putIfAbsent(key,
                        queue);
                if (existing != null) {
                    queue = existing;
                }
            }

            return queue;
        }

        public void freeDataPage(final DataPage dataPage) {
            checkNotNull(dataPage);
            dataPage.setSize(0);

            final short capacity = dataPage.capacity();
            final BlockingQueue<Number> freeOffsets = getFreeOffsets(capacity);

            final long pageOffset = dataPage.offset;
            Number offset = pageOffset < Integer.MAX_VALUE ? Integer.valueOf((int) pageOffset)
                    : Long.valueOf(pageOffset);
            if (!freeOffsets.offer(offset)) {
                LOG.debug("blocking queue full");
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(format("freed data page at %,d", pageOffset));
            }
        }

        public long newDataPage(final int depth) {
            final short capacity = pageCapacityForDepth(depth);
            final BlockingQueue<Number> freeOffsets = getFreeOffsets(capacity);

            @Nullable
            Number freeOffset = freeOffsets.poll();
            final long pageOffset;
            if (freeOffset == null) {
                final int pageSize = dataPageSizeForDepth(depth);
                pageOffset = nextFreePageOffset.getAndAdd(pageSize);
                this.dataPageCount.incrementAndGet();
                this.dataPagesSize.addAndGet(pageSize);
            } else {
                pageOffset = freeOffset.longValue();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(format("reusing data page at %,d", pageOffset));
                }
                // System.err.println(format("reusing data page at %,d", pageOffset));
            }
            return pageOffset;
        }

        public long newBucketPage(final int depth) {
            final int bucketPageSize = bucketPageSizeForDepth(depth);
            long pageOffset = nextFreePageOffset.getAndAdd(bucketPageSize);
            this.bucketPagesSize.addAndGet(bucketPageSize);
            this.bucketPageCount.incrementAndGet();
            return pageOffset;
        }

        public long dataPages() {
            return dataPageCount.get();
        }

        public int freeDataPages() {
            return freeDataPageOffsetsByCapacity.size();
        }

        public long bucketPages() {
            return this.bucketPageCount.get();
        }

        private static final int[] BRANCH_FACTORS = { 64, 64, 16, 16, 4, 4, 2, 2, 2, 2, 2, 2, 2 };

        public short pageCapacityForDepth(final int depth) {
            return 64;
        }

        public int branchFactor(final int depth) {
            return BRANCH_FACTORS[depth];
        }

        public int bucketPageSizeForDepth(final int depth) {
            final int capacity = branchFactor(depth);
            return BucketPage.pageSize(capacity);
        }

        public int dataPageSizeForDepth(final int depth) {
            return DataPage.pageSize(pageCapacityForDepth(depth));
        }

        BucketPage getBucketPage(final long pageOffset) {
            final BigByteBuffer buffer = this.dataBuffer;

            final int capacity = BucketPage.readCapacity(buffer, pageOffset);
            final int pageSize = BucketPage.pageSize(capacity);

            buffer.limit(pageOffset + pageSize).position(pageOffset);
            BigByteBuffer pageBuffer = buffer.slice();
            BucketPage page = new BucketPage(this, pageBuffer, pageOffset);
            return page;
        }

        DataPage getDataPage(final long pageOffset) {
            final BigByteBuffer buffer = this.dataBuffer;

            final long offsetOfCapacity = pageOffset + DataPage.OFFSET_OF_CAPACITY;
            final short capacity = buffer.getShort(offsetOfCapacity);
            checkState(capacity > 0);
            final int pageSize = DataPage.pageSize(capacity);

            buffer.limit(pageOffset + pageSize).position(pageOffset);
            BigByteBuffer pageBuffer = buffer.slice();
            DataPage page = new DataPage(this, pageBuffer, pageOffset);
            return page;
        }

        public DataPage createDataPage(final int depth) {
            checkArgument(depth >= 0);

            final long newPageOffset = this.newDataPage(depth);
            final int pageSizeBytes = this.dataPageSizeForDepth(depth);
            final short capacity = this.pageCapacityForDepth(depth);

            BigByteBuffer dataBuffer = this.dataBuffer;
            dataBuffer.limit(newPageOffset + pageSizeBytes).position(newPageOffset);
            BigByteBuffer pageBuffer = dataBuffer.slice();
            pageBuffer.putShort(DataPage.OFFSET_OF_CAPACITY, capacity);
            // pageBuffer.putShort(DataPage.OFFSET_OF_SIZE, (short) 0);
            checkState(0 == pageBuffer.getShort(DataPage.OFFSET_OF_SIZE));
            DataPage page = new DataPage(this, pageBuffer, newPageOffset);
            long size = page.size();
            checkState(0 == size, "size: %s", size);
            checkState(capacity == page.capacity());
            return page;
        }

        public BucketPage createBucketPage(final int depth) {
            final long newPageOffset = newBucketPage(depth);
            final int pageSize = bucketPageSizeForDepth(depth);
            final int capacity = branchFactor(depth);
            BigByteBuffer bucketsBuffer = this.dataBuffer;
            bucketsBuffer.limit(newPageOffset + pageSize).position(newPageOffset);
            BigByteBuffer pageBuffer = bucketsBuffer.slice();
            BucketPage.writeCapacity(pageBuffer, capacity);
            BucketPage page = new BucketPage(this, pageBuffer, newPageOffset);
            for (int i = 0; i < capacity; i++) {
                page.setBranchOffset(i, -1L, Page.TYPE_UNSET);
            }
            return page;
        }
    }

    static class Entry {
        static final int KEY_SIZE = 9;

        static final int SIZE_BYTES = KEY_SIZE + Long.BYTES;

        /**
         * 9-byte Big-endian encoded bucketHash followed by nameHash
         */
        final byte[] encodedKey;

        final long value;

        Entry(final/* unsigned */long bucketHash, final byte nameHash, final long dataOffset) {
            this(encode(bucketHash, nameHash), dataOffset);
        }

        Entry(final byte[] encodedKey, final long value) {
            this.encodedKey = encodedKey;
            this.value = value;
        }

        /**
         * Query constructor.
         */
        Entry(long bucketHash, byte nameHash) {
            this(bucketHash, nameHash, -1L);
        }

        private static byte[] encode(long bucketHash, byte nameHash) {
            byte[] encoded = new byte[9];
            for (int i = 7; i >= 0; i--) {
                encoded[i] = (byte) (bucketHash & 0xffL);
                bucketHash >>= 8;
            }
            encoded[8] = nameHash;
            return encoded;
        }
    }

    static abstract class Page {
        static final byte TYPE_UNSET = -1, TYPE_DATA = 1, TYPE_BUCKET = 2;

        final long offset;

        final FileHeader fileHeader;

        final BigByteBuffer pageBuffer;// i.e. the buffer slice covering the page segment

        Page(final FileHeader fileHeader, final BigByteBuffer pageBuffer, final long pageOffset) {
            this.fileHeader = fileHeader;
            this.pageBuffer = pageBuffer;
            this.offset = pageOffset;
        }

        /**
         * Recursively computes the page size, instead of just returning its saved {@link #size()},
         * used merely for correctness assessment in tests
         */
        public abstract long sizeRecursive();

        /**
         * @return tha page size (number of entries)
         */
        public abstract long size();

        /**
         * Finds the value for the given query object in this page or any
         * {@link #getPageForEntry(BigByteBuffer) linked page}.
         * 
         * @param query
         * @return the data offset of the given entry, or {@code -1} if not found
         */
        public final long find(final Entry query) {
            return find(query, 0);
        }

        protected abstract long find(final Entry query, final int depth);

        public final int put(final Entry entry) {
            return put(entry, 0);
        }

        protected abstract int put(final Entry entry, final int depth);
    }

    static class DataPage extends Page {

        private static final int OFFSET_OF_CAPACITY = 0;

        private static final int SIZE_OF_CAPACITY = Short.BYTES;

        private static final int OFFSET_OF_SIZE = OFFSET_OF_CAPACITY + SIZE_OF_CAPACITY;

        private static final int SIZE_OF_SIZE = Short.BYTES;

        static final int HEADER_SIZE = SIZE_OF_CAPACITY + SIZE_OF_SIZE;

        static final int OFFSET_OF_DATA_SECTION = HEADER_SIZE;

        DataPage(final FileHeader fileHeader, final BigByteBuffer pageBuffer, final long offset) {
            super(fileHeader, pageBuffer, offset);

            checkArgument(pageBuffer.capacity() == this.pageSize());
            pageBuffer.clear();
        }

        private int pageSize() {
            final short capacity = capacity();
            return DataPage.pageSize(capacity);
        }

        static int pageSize(final short capacity) {
            return OFFSET_OF_DATA_SECTION + Entry.SIZE_BYTES * capacity;
        }

        @Override
        protected long find(final Entry query, final int depth) {
            final int insertionIndex = binarySearch(query);
            final boolean exists = insertionIndex > -1;
            if (exists) {
                final int entryIndex = insertionIndex;
                return data(entryIndex);
            }
            return -1L;
        }

        long data(final int entryIndex) {
            int dataOffset = entryOffset(entryIndex) + Entry.KEY_SIZE;
            long offset = pageBuffer.getLong(dataOffset);
            return offset;
        }

        int compare(Entry query, final int entryIndex) {
            // in-place compare
            final int entryOffset = entryOffset(entryIndex);
            for (int i = 0; i < Entry.KEY_SIZE; i++) {
                byte queryByte = query.encodedKey[i];
                byte entryByte = pageBuffer.get(entryOffset + i);
                int c = Byte.compare(queryByte, entryByte);
                if (c != 0) {
                    return c;
                }
            }
            return 0;
        }

        private int entryOffset(final int entryIndex) {
            return OFFSET_OF_DATA_SECTION + Entry.SIZE_BYTES * entryIndex;
        }

        /**
         * @param entry
         * @return {@code Integer.MIN_VALUE} if the page is full, entry didn't exist and hence
         *         couldn't replace it; otherwise the index of the query entry, if found, or
         *         <tt>(-(<i>insertion point</i>) - 1)</tt>. The <i>insertion point</i> is defined
         *         as the point at which the key would be inserted into the list: the index of the
         *         first element greater than the key, or <tt>list.size()</tt> if all elements in
         *         the list are less than the specified key. Note that this guarantees that the
         *         return value will be &gt;= 0 if and only if the key is found.
         */
        @Override
        protected int put(final Entry entry, final int depth) {
            final int size = (int) size();
            final int fanOutThreshold = fileHeader.pageCapacityForDepth(depth);
            checkState(size <= fanOutThreshold, "size: %s, max size: %s", size, fanOutThreshold);

            final int insertionPoint = binarySearch(entry);
            final boolean exists = insertionPoint > -1;
            final int insertionIndex = exists ? insertionPoint : -insertionPoint - 1;

            if (exists) {
                set(entry, insertionIndex);
            } else {
                if (size < fanOutThreshold) {
                    insertionSort(entry, insertionIndex, size);
                    setSize(size + 1);
                } else {
                    return Integer.MIN_VALUE;
                }
            }

            return insertionPoint;
        }

        private Entry getEntry(int index) {
            final int offset = entryOffset(index);
            byte[] encodedKey = new byte[Entry.KEY_SIZE];
            pageBuffer.get(offset, encodedKey, 0, Entry.KEY_SIZE);
            long value = pageBuffer.getLong(offset + Entry.KEY_SIZE);
            return new Entry(encodedKey, value);
        }

        void insertionSort(Entry entry, final int insertionIndex, final int size) {
            final int shiftCount = size - insertionIndex;
            if (shiftCount > 0) {
                final int offset = entryOffset(insertionIndex);
                byte[] buff = new byte[Entry.SIZE_BYTES * shiftCount];
                pageBuffer.get(offset, buff, 0, buff.length);
                pageBuffer.put(offset + Entry.SIZE_BYTES, buff, 0, buff.length);
            }

            // set or replace existing
            set(entry, insertionIndex);
        }

        @Override
        public long size() {
            long sizeByte = (long) pageBuffer.getShort(OFFSET_OF_SIZE);
            return sizeByte;
        }

        private void setSize(final int size) {
            checkArgument(size <= Short.MAX_VALUE);
            pageBuffer.putShort(OFFSET_OF_SIZE, (short) size /* (byte) (size + 1) */);
        }

        public short capacity() {
            short capacity = pageBuffer.getShort(OFFSET_OF_CAPACITY);
            return capacity;
        }

        public long sizeRecursive() {
            long size = size();
            return size;
        }

        void set(Entry entry, final int entryIndex) {
            final int offset = entryOffset(entryIndex);
            final int dataOffet = offset + Entry.KEY_SIZE;
            pageBuffer.put(offset, entry.encodedKey, 0, Entry.KEY_SIZE);
            pageBuffer.putLong(dataOffet, entry.value);
        }

        /**
         * @param entry
         * @return the index of the query entry, if found, or
         *         <tt>(-(<i>insertion point</i>) - 1)</tt>. The <i>insertion point</i> is defined
         *         as the point at which the key would be inserted into the list: the index of the
         *         first element greater than the key, or <tt>list.size()</tt> if all elements in
         *         the list are less than the specified key. Note that this guarantees that the
         *         return value will be &gt;= 0 if and only if the key is found.
         */
        int binarySearch(Entry entry) {
            int low = 0;
            int high = (int) (size() - 1);

            while (low <= high) {
                int mid = (low + high) >>> 1;
                int cmp = compare(entry, mid);

                if (cmp > 0) {
                    low = mid + 1;
                } else if (cmp < 0) {
                    high = mid - 1;
                } else {
                    return mid; // key found
                }
            }
            return -(low + 1); // key not found
        }
    }

    /**
     * <pre>
     * <code>
     * - byte[8] (long): accumulated size (recursive number of entries)
     * - byte[8 * N]: N longs with the offsets of the child pages, where N is the bucket's {@link #capacity()}
     * - byte[N]: N bytes with the type of the child pages, where N is the bucket's {@link #capacity()}, and type is
     *  one of {@link Page#TYPE_UNSET}, {@link Page#TYPE_DATA}, or {@link Page#TYPE_BUCKET}
     * </code>
     * </pre>
     */
    static class BucketPage extends Page {

        private static final int OFFSET_OF_SIZE = 0;

        private static final int SIZE_OF_SIZE = Long.BYTES;

        private static final int OFFSET_OF_CAPACITY = SIZE_OF_SIZE;

        private static final int SIZE_OF_CAPACITY = Byte.BYTES;

        private static final int HEADER_SIZE = SIZE_OF_SIZE + SIZE_OF_CAPACITY;

        private static final int RECORD_SIZE = Long.BYTES + Byte.BYTES;

        private static final int OFFSET_OF_BRANCH_OFFSETS = HEADER_SIZE;

        private final int offsetOfPageTypes;

        BucketPage(final FileHeader fileHeader, final BigByteBuffer pageBuffer,
                final long pageOffset) {
            super(fileHeader, pageBuffer, pageOffset);
            final int capacity = capacity();
            checkState(capacity > 0);

            final int sizeOfBranchOffsets = Long.BYTES * capacity;
            this.offsetOfPageTypes = OFFSET_OF_BRANCH_OFFSETS + sizeOfBranchOffsets;
            final int sizeOfPageTypes = Byte.BYTES * capacity;

            checkState(pageBuffer.capacity() == HEADER_SIZE + sizeOfBranchOffsets + sizeOfPageTypes);
            pageBuffer.clear();
        }

        public static int readCapacity(final BigByteBuffer buffer, final long basePageOffset) {
            final int capacity = buffer.get(basePageOffset + OFFSET_OF_CAPACITY) & 0xFF;
            checkState(capacity > 0, "capacity should be > 0: %s", capacity);
            return capacity;
        }

        public static void writeCapacity(final BigByteBuffer pageBuffer, final int capacity) {
            checkArgument(capacity > 0 && capacity < 256);
            pageBuffer.put(BucketPage.OFFSET_OF_CAPACITY, (byte) capacity);
            checkState(capacity == readCapacity(pageBuffer, 0));
        }

        public static int pageSize(final int capacity) {
            return HEADER_SIZE + RECORD_SIZE * capacity;
        }

        @Override
        protected long find(final Entry query, final int depth) {
            final int bucketIndex = bucket(query.encodedKey, depth);
            Optional<Page> page = getBucketPage(bucketIndex);
            if (page.isPresent()) {
                Page p = page.get();
                return p.find(query, depth + 1);
            }
            return -1L;
        }

        @Override
        protected int put(final Entry entry, final int depth) {
            final int bucketIndex = bucket(entry.encodedKey, depth);
            Page page = getOtCreateBucketPage(bucketIndex, depth);

            int retVal;
            try {
                retVal = page.put(entry, depth + 1);
            } catch (RuntimeException e) {
                throw e;
            }
            final boolean dataPageIsFull = Integer.MIN_VALUE == retVal;
            if (dataPageIsFull) {
                Preconditions.checkState(page instanceof DataPage);
                page = branchOut((DataPage) page, bucketIndex, depth + 1);
                retVal = page.put(entry, depth + 1);
                Preconditions.checkState(retVal != Integer.MIN_VALUE);
            }
            final boolean isNew = retVal < 0;
            if (isNew) {
                long size = size() + 1;
                pageBuffer.putLong(BucketPage.OFFSET_OF_SIZE, size);
            }
            return retVal;
        }

        private BucketPage branchOut(final DataPage dataPage, final int bucketIndex,
                final int newDepth) {

            final int fanOutThreshold = fileHeader.pageCapacityForDepth(newDepth);
            final BucketPage bucketPage = fileHeader.createBucketPage(newDepth);
            final long pageSize = dataPage.size();
            Preconditions.checkState(pageSize == fanOutThreshold);
            for (int i = 0; i < (int) pageSize; i++) {
                Entry entry = dataPage.getEntry(i);
                bucketPage.put(entry, newDepth);
            }
            final long pageOffset = bucketPage.offset;
            setBranchOffset(bucketIndex, pageOffset, Page.TYPE_BUCKET);

            fileHeader.freeDataPage(dataPage);
            return bucketPage;
        }

        private long getBranchOffset(final int bucketIndex) {
            final int entryOffset = OFFSET_OF_BRANCH_OFFSETS + Long.BYTES * bucketIndex;
            final long pageOffset = this.pageBuffer.getLong(entryOffset);
            return pageOffset;
        }

        public void setBranchOffset(final int index, final long childPageOffset, final byte pageType) {
            final int entryOffset = OFFSET_OF_BRANCH_OFFSETS + Long.BYTES * index;
            this.pageBuffer.putLong(entryOffset, childPageOffset);

            this.pageBuffer.put(offsetOfPageTypes + Byte.BYTES * index, pageType);
        }

        @Override
        public long size() {
            long size = pageBuffer.getLong(BucketPage.OFFSET_OF_SIZE);
            return size;
        }

        /**
         * @return the max number of child pages this bucket can hold pointers to
         */
        public int capacity() {
            final int capacity = BucketPage.readCapacity(pageBuffer, 0);
            return capacity;
        }

        @Override
        public long sizeRecursive() {
            long size = 0;
            final int capacity = capacity();
            for (int i = 0; i < capacity; i++) {
                Optional<Page> branch = getBucketPage(i);
                if (branch.isPresent()) {
                    size += branch.get().sizeRecursive();
                }
            }
            return size;
        }

        private Optional<Page> getBucketPage(final int bucketIndex) {
            if (bucketIndex < 0 || bucketIndex >= capacity()) {
                throw new IllegalArgumentException("bucketIndex =" + bucketIndex);
            }

            final long pageOffset = getBranchOffset(bucketIndex);
            final byte pageType = getPageType(bucketIndex);
            Page page = null;
            if (pageOffset > -1L) {
                if (Page.TYPE_BUCKET == pageType) {
                    page = fileHeader.getBucketPage(pageOffset);
                } else if (Page.TYPE_DATA == pageType) {
                    page = fileHeader.getDataPage(pageOffset);
                } else {
                    throw new IllegalStateException("Unknown page type: " + (pageType & 0xFF));
                }
            }
            return Optional.ofNullable(page);
        }

        private byte getPageType(final int bucketIndex) {
            final byte pageType = this.pageBuffer.get(offsetOfPageTypes + Byte.BYTES * bucketIndex);
            return pageType;
        }

        private Page getOtCreateBucketPage(final int bucketIndex, final int depth) {
            final int branchingFactor = fileHeader.branchFactor(depth);
            checkState(capacity() == branchingFactor);
            checkArgument(bucketIndex >= 0 && bucketIndex < branchingFactor, "bucketIndex = %s",
                    bucketIndex);
            Optional<Page> page = getBucketPage(bucketIndex);
            if (page.isPresent()) {
                return page.get();
            }
            DataPage newPage = fileHeader.createDataPage(depth + 1);
            setBranchOffset(bucketIndex, newPage.offset, Page.TYPE_DATA);
            return newPage;
        }

        int bucket(final byte[] key, final int depthIndex) {
            checkArgument(depthIndex < 9, "depth too high");

            int byteN = key[depthIndex] & 0xFF; // byteN(depthIndex, hashCodeLong);
            return bucket(byteN, depthIndex);
            // final int buckets = capacity();
            // final int bucketIndex = Hashing
            // .consistentHash(hashCodeLong * (1 + depthIndex), buckets);
            // return bucketIndex;
        }

        private int bucket(final int byteN, final int depthIndex) {
            Preconditions.checkState(byteN >= 0);
            Preconditions.checkState(byteN < 256);

            final int maxBuckets = capacity();

            final int bucket = (byteN * maxBuckets) / 256;
            return bucket;
        }
    }

    // static final class BufferBitSet {
    //
    // final BigByteBuffer buffer;
    //
    // BufferBitSet(BigByteBuffer buffer) {
    // this.buffer = buffer;
    // }
    //
    // /**
    // * Returns the number of bits set to {@code true} in this bitset.
    // */
    // public int cardinality() {
    // int cardinality = 0;
    // for (int i = 0; i < buffer.capacity(); i++) {
    // byte iByte = buffer.get(i);
    // int bitCount = Integer.bitCount(iByte & 0xFF);
    // cardinality += bitCount;
    // }
    // return cardinality;
    // }
    //
    // void put(final int pos, final boolean value) {
    // set(pos, value);
    // }
    //
    // int size() {
    // return (int) (buffer.capacity() * 8);
    // }
    //
    // boolean get(final int pos) {
    // final byte _byte = buffer.get(pos / 8);
    // final int bitPos = pos % 8;
    // return valueOf(_byte, bitPos);
    // }
    //
    // void set(final int pos, boolean value) {
    // final int byteOffset = pos / 8;
    // final byte _byte = buffer.get(byteOffset);
    // final int bitOffset = pos % 8;
    // if (valueOf(_byte, bitOffset) != value) {
    // buffer.put(byteOffset, flip(_byte, bitOffset));
    // }
    // }
    //
    // int nextClearBit(final int fromIndex) {
    // final int size = size();
    // for (int i = fromIndex; i < size; i++) {
    // if (!get(i)) {
    // return i;
    // }
    // }
    // return -1;
    // }
    //
    // int nextSetBit(int fromIndex) {
    // final int size = size();
    // for (int i = fromIndex; i < size; i++) {
    // if (get(i)) {
    // return i;
    // }
    // }
    // return -1;
    // }
    //
    // int lastSetBit() {
    // final int size = size();
    // for (int i = size - 1; i >= 0; i--) {
    // if (get(i)) {
    // return i;
    // }
    // }
    // return -1;
    // }
    //
    // static byte flip(final byte b, final int bitOffset) {
    // byte mask = (byte) (1 << bitOffset);
    // return (byte) (mask ^ b);
    // }
    //
    // static boolean valueOf(byte b, int pos) {
    // return ((b >> pos) & 1) == 1;
    // }
    //
    // }
}
