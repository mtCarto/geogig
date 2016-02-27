/* Copyright (c) 2012-2014 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.api;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.api.graph.AbstractTreeBuilder;
import org.locationtech.geogig.api.graph.ClusteringStrategyFactory;
import org.locationtech.geogig.api.graph.HeapDAGStorageProviderFactory;
import org.locationtech.geogig.api.graph.MappedFileDAGStorageProviderFactory;
import org.locationtech.geogig.storage.ObjectStore;

import com.google.common.base.Preconditions;
import com.vividsolutions.jts.geom.Envelope;

public class RevTreeBuilder extends AbstractTreeBuilder implements TreeBuilder {

    /**
     * Empty tree constructor, used to create trees from scratch
     * 
     * @param db
     */
    public RevTreeBuilder(final ObjectStore db) {
        super(db);
    }

    /**
     * Copy constructor with tree depth
     * 
     * @param obStore {@link org.locationtech.geogig.storage.ObjectStore ObjectStore} with which to
     *        initialize this RevTreeBuilder.
     * @param copy {@link org.locationtech.geogig.api.RevTree RevTree} to copy.
     */
    public RevTreeBuilder(final ObjectStore obStore, @Nullable final RevTree copy) {
        super(obStore);
        if (copy != null) {
            super.original = copy;
        }
    }

    public static RevTreeBuilder canonical(final ObjectStore target) {
        RevTreeBuilder builder = new RevTreeBuilder(target);
        builder.strategyFactory = ClusteringStrategyFactory.canonical();
        return builder;
    }

    public static RevTreeBuilder quadTree(final ObjectStore target) {
        final Envelope MAX_BOUNDS_WGS84 = new Envelope(-180, 180, -90, 90);
        final int DEFAULT_MAX_DEPTH = 12;
        return quadTree(target, MAX_BOUNDS_WGS84, DEFAULT_MAX_DEPTH);
    }

    public static RevTreeBuilder quadTree(final ObjectStore target, final Envelope maxBounds,
            final int maxDepth) {
        Preconditions.checkNotNull(target);
        Preconditions.checkNotNull(maxBounds);
        Preconditions.checkArgument(maxDepth > 0);
        RevTreeBuilder builder = new RevTreeBuilder(target);
        builder.strategyFactory = ClusteringStrategyFactory.quadtree(maxBounds, maxDepth);
        return builder;
    }

    public RevTreeBuilder original(final RevTree original) {
        Preconditions.checkState(super.state == STATE.NEW);
        Preconditions.checkNotNull(original);
        super.original = original;
        return this;
    }

    public RevTreeBuilder withHeapStorage() {
        Preconditions.checkState(super.state == STATE.NEW);
        super.dagStorageFactory = new HeapDAGStorageProviderFactory(super.target);
        return this;
    }

    public RevTreeBuilder withOnDiskStorage() {
        Preconditions.checkState(super.state == STATE.NEW);
        super.dagStorageFactory = new MappedFileDAGStorageProviderFactory(super.target);
        return this;
    }

}
