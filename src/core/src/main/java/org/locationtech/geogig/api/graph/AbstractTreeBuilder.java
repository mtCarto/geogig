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

import static com.google.common.base.Preconditions.checkNotNull;

import org.locationtech.geogig.api.Node;
import org.locationtech.geogig.api.RevTree;
import org.locationtech.geogig.api.TreeBuilder;
import org.locationtech.geogig.storage.ObjectStore;

import com.google.common.base.Preconditions;

public abstract class AbstractTreeBuilder implements TreeBuilder {

    protected static enum STATE {
        NEW, INITIALIZED;
    };

    protected final ObjectStore target;

    protected ClusteringStrategyFactory strategyFactory;

    protected DAGStorageProviderFactory dagStorageFactory;

    protected STATE state;

    protected RevTree original;

    private ClusteringStrategy clusteringStrategy;

    protected AbstractTreeBuilder(final ObjectStore target) {
        checkNotNull(target);
        this.target = target;
        this.original = RevTree.EMPTY;
        this.state = STATE.NEW;
    }

    /**
     * @deprecated remove once {@link QuadTreeBuilderTest} is finished and does not call this method
     */
    @Deprecated
    public int depth() {
        return clusteringStrategy.depth();
    }

    @Override
    public final AbstractTreeBuilder put(final Node node) {
        checkNotNull(node, "Argument node is null");
        switch (state) {
        case NEW:
            init();
        case INITIALIZED:
            clusteringStrategy.put(node);
            break;
        default:
            throw new IllegalStateException();
        }
        return this;
    }

    @Override
    public final AbstractTreeBuilder remove(final String featureId) {
        checkNotNull(featureId, "Argument featureId is null");
        switch (state) {
        case NEW:
            init();
        case INITIALIZED:
            clusteringStrategy.remove(featureId);
            break;
        default:
            throw new IllegalStateException();
        }
        return this;
    }

    private synchronized void init() {
        if (this.state == STATE.NEW) {
            if (this.strategyFactory == null) {
                this.strategyFactory = ClusteringStrategyFactory.canonical();
            }
            if (this.dagStorageFactory == null) {
                //this.dagStorageFactory = new CachingDAGStorageProviderFactory(this.target);
                this.dagStorageFactory = new HeapDAGStorageProviderFactory(this.target);
            }
            this.clusteringStrategy = strategyFactory.create(this.original, dagStorageFactory);
            this.state = STATE.INITIALIZED;
        }
    }

    @Override
    public RevTree build() {
        RevTree tree;
        if (state == STATE.NEW) {
            tree = original;
        } else {
            tree = DAGTreeBuilder.build(clusteringStrategy, target);
            Preconditions.checkState(target.exists(tree.getId()), "tree not saved %s", tree);
            this.original = tree;
            this.clusteringStrategy.dispose();
            this.clusteringStrategy = null;
            this.state = STATE.NEW;
        }
        return tree;
    }

}
