/* Copyright (c) 2016 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.osm.internal.coordcache;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.io.BigByteBuffer;
import org.locationtech.geogig.io.ByteBufferStore;
import org.locationtech.geogig.io.MappedFileByteBufferStore;
import org.locationtech.geogig.osm.internal.OSMCoordinateSequence;
import org.locationtech.geogig.osm.internal.OSMCoordinateSequenceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.vividsolutions.jts.geom.CoordinateSequence;

/**
 * A fast {@code PointCache} based on sparse memory mapped files.
 *
 */
public class SparseMappedPointCache implements PointCache {

    private static final Logger LOG = LoggerFactory.getLogger(SparseMappedPointCache.class);

    /**
     * Number of records per page
     */
    private static final int RECORDS_PER_PAGE = 10_000;

    /**
     * Page header size, just a bitmap of record presence
     */
    private static final int PAGE_HEADER_SIZE = RECORDS_PER_PAGE / 8;

    /**
     * Record size, just the two ints that make up an ordinate. The record identifier is implicit by
     * the position (go sparse files!)
     */
    private static final int RECORD_SIZE = 8;// 2 * sizeof(int)

    /**
     * The size of a page. I.e. header + page records size.
     */
    private static final int PAGE_SIZE = PAGE_HEADER_SIZE + (RECORDS_PER_PAGE * RECORD_SIZE);

    private static final int PAGES_PER_FILE = 1000 * 1000;

    private static final long MAX_FILE_SIZE = (long) PAGES_PER_FILE * PAGE_SIZE;
    static {
        Preconditions.checkState(MAX_FILE_SIZE > 0, "MAX_FILE_SIZE integer overflow: %s",
                MAX_FILE_SIZE);
        LOG.info(format("Recods per page: %,d", RECORDS_PER_PAGE));
        LOG.info(format("Pages per file: %,d", PAGES_PER_FILE));
        LOG.info(format("Page size: %,d", PAGE_SIZE));
        LOG.info(format("Recods per file: %,d", RECORDS_PER_PAGE * (long) PAGES_PER_FILE));
        LOG.info(format("Max file size: %,d", MAX_FILE_SIZE));
    }

    /**
     * Singleton coordseq factory for {@link #get(List)}
     */
    private static final OSMCoordinateSequenceFactory CSFAC = new OSMCoordinateSequenceFactory();

    private final BigByteBuffer mappedByteBuffer;

    public SparseMappedPointCache(Platform platform) {
        final File tempDir = platform.getTempDir();
        final Path path;
        try {
            path = Files.createTempFile(tempDir.toPath(), "pointCache", ".sparse");
            Files.delete(path);
            Preconditions.checkState(!Files.exists(path));
            System.err.println("writing to " + path);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        try {
            // TODO: improve to handle smaller chunks
            final long fileSize = MAX_FILE_SIZE;
            Preconditions.checkState(!Files.exists(path));
            ByteBufferStore store = new MappedFileByteBufferStore(path, true);
            mappedByteBuffer = new BigByteBuffer(store, fileSize);
            // BigByteBuffer.builder().path(path).sparse().
        } catch (Exception e) {
            try {
                Files.deleteIfExists(path);
            } catch (IOException e1) {
                e1.printStackTrace();
                throw Throwables.propagate(e1);
            }
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void put(Long nodeId, OSMCoordinateSequence coord) {
        checkNotNull(nodeId, "id is null");
        checkNotNull(coord, "coord is null");
        final long id = nodeId.longValue();
        final int recordIndex = (int) (id % RECORDS_PER_PAGE);
        final long offset = PAGE_HEADER_SIZE + (long) RECORD_SIZE * recordIndex;

        try {
            BigByteBuffer page = getPage(id);
            page.position(offset);
            page.putInt(coord.ordinates()[0]);
            page.putInt(coord.ordinates()[1]);
            setPresent(page, recordIndex, true);
        } catch (RuntimeException e) {
            e.printStackTrace();
            System.err.println("nodeid: " + nodeId);
            throw e;
        }
        // {
        // checkState(isPresent(page, recordIndex));
        // int[] target = new int[2];
        // get(id, target);
        // checkState(coord.ordinates()[0] == target[0], "expected %s, got %s",
        // coord.ordinates()[0], target[0]);
        // checkState(coord.ordinates()[1] == target[1]);
        // }
    }

    @Override
    public CoordinateSequence get(List<Long> ids) {
        checkNotNull(ids, "ids is null");
        final int size = ids.size();
        OSMCoordinateSequence cs = CSFAC.create(size);

        int[] ordinateBuff = new int[2];
        for (int i = 0; i < size; i++) {
            long nodeId = ids.get(i).longValue();
            get(nodeId, ordinateBuff);
            cs.setOrdinate(i, 0, ordinateBuff[0]);
            cs.setOrdinate(i, 1, ordinateBuff[1]);
        }
        return cs;
    }

    private void get(long nodeId, int[/* 2 */] target) {
        final int recordIndex = (int) (nodeId % RECORDS_PER_PAGE);
        final long offset = PAGE_HEADER_SIZE + (long) RECORD_SIZE * recordIndex;

        BigByteBuffer page = getPage(nodeId);
        if (isPresent(page, recordIndex)) {
            page.position(offset);
            target[0] = page.getInt();
            target[1] = page.getInt();
        } else {
            throw new IllegalArgumentException("Node " + nodeId + " not found");
        }
    }

    @Override
    public void dispose() {
        mappedByteBuffer.discard();
    }

    /**
     * Updates the page bitmap header to mark the bit at position {@code offset} to be present or
     * absent as indicated by the {@code present} argument.
     */
    private void setPresent(BigByteBuffer page, final int recordIndex, final boolean present) {
        final int byteOffset = recordIndex / 8;
        final int bitOffset = recordIndex % 8;
        byte b = page.get(byteOffset);
        if (present) {
            b |= 1 << bitOffset;
        } else {
            b &= ~(1 << bitOffset);
        }
        page.put(byteOffset, b);
    }

    private boolean isPresent(final BigByteBuffer page, final int recordIndex) {
        final int byteOffset = recordIndex / 8;
        final int bitOffset = recordIndex % 8;
        byte b = page.get(byteOffset);

        byte flag = (byte) (1 << bitOffset);

        boolean present = (b & flag) != 0;
        return present;
    }

    private BigByteBuffer getPage(final long nodeId) {

        final long pageIndex = nodeId / RECORDS_PER_PAGE;

        final long pageOffset = pageIndex * (long) PAGE_SIZE;
        if (pageOffset >= mappedByteBuffer.capacity() - PAGE_SIZE) {
            throw new IllegalArgumentException(String.format(
                    "Node: %,d, pageIndex: %,d, pageOffset: %,d. Capacity: %,d", nodeId, pageIndex,
                    pageOffset, mappedByteBuffer.capacity()));
        }
        mappedByteBuffer.limit(pageOffset + PAGE_SIZE);
        mappedByteBuffer.position(pageOffset);
        return mappedByteBuffer.slice();
    }
}
