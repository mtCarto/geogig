/* Copyright (c) 2015 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.api.graph;

import static org.locationtech.geogig.storage.NodePathStorageOrder.INSTANCE;

import java.util.Arrays;

import com.google.common.primitives.UnsignedLong;

public class CanonicalNodeId extends NodeId {

    private final UnsignedLong bucketsByDepth;

    protected CanonicalNodeId(final UnsignedLong nameHash, final String name) {
        super(name);
        this.bucketsByDepth = nameHash;
    }

    public UnsignedLong bucketsByDepth() {
        return bucketsByDepth;
    }

    @Override
    public boolean equals(Object o) {
        return bucketsByDepth.equals(((CanonicalNodeId) o).bucketsByDepth)
                && name.equals(((CanonicalNodeId) o).name);
    }

    @Override
    public int hashCode() {
        return 31 * bucketsByDepth.hashCode() + name.hashCode();
    }

    @Override
    public int compareTo(NodeId o) {
        int c = bucketsByDepth.compareTo(((CanonicalNodeId) o).bucketsByDepth);
        return c == 0 ? name.compareTo(o.name) : c;
    }

    @Override
    public String toString() {

        int[] bucketIndexes = new int[8];
        for (int i = 0; i < 8; i++) {
            bucketIndexes[i] = bucket(i);
        }

        return getClass().getSimpleName() + "[ name: " + super.name + ", buckets by depth: "
                + Arrays.toString(bucketIndexes) + "]";
    }

    @Override
    public int bucket(int depth) {
        int bucket = INSTANCE.bucket(bucketsByDepth, depth);
        return bucket;
    }
}