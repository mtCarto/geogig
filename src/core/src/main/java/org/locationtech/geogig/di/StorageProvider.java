/* Copyright (c) 2015 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.di;

import java.util.ServiceLoader;

import org.locationtech.geogig.storage.GraphDatabase;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.locationtech.geogig.storage.RefDatabase;

import com.google.common.collect.ImmutableList;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;

/**
 * A plug-in mechanism for providers of storage backends for the different kinds of geogig
 * databases.
 * <p>
 * Realizations of this interface are looked up in the classpath using the standard Java
 * {@link ServiceLoader service provider interface (SPI)} mechanism, by loading the classes defined
 * in all {@code META-INF/services/org.locationtech.geogig.di.StorageProvider} text files.
 *
 */
public abstract class StorageProvider {

    /**
     * @return a short name of the storage backend
     */
    public abstract String getName();

    /**
     * @return an application level version for the storage backend, may denote a serialization
     *         format or storage mechanism
     */
    public abstract String getVersion();

    /**
     * @return a human readable description of the storage backend
     */
    public abstract String getDescription();

    public abstract VersionedFormat getObjectDatabaseFormat();

    public abstract VersionedFormat getGraphDatabaseFormat();

    public abstract VersionedFormat getRefsDatabaseFormat();

    protected abstract Class<? extends ObjectDatabase> objectsBinding();

    protected abstract Class<? extends RefDatabase> refsBinding();

    protected abstract Class<? extends GraphDatabase> graphBinding();

    public void bindObjects(MapBinder<VersionedFormat, ObjectDatabase> plugins) {
        if (null != getObjectDatabaseFormat()) {
            plugins.addBinding(getObjectDatabaseFormat()).to(objectsBinding()).in(Scopes.SINGLETON);
        }
    }

    public void bindGraph(MapBinder<VersionedFormat, GraphDatabase> plugins) {
        if (null != getGraphDatabaseFormat()) {
            plugins.addBinding(getGraphDatabaseFormat()).to(graphBinding()).in(Scopes.SINGLETON);
        }
    }

    public void bindRefs(MapBinder<VersionedFormat, RefDatabase> plugins) {
        if (null != getRefsDatabaseFormat()) {
            plugins.addBinding(getRefsDatabaseFormat()).to(refsBinding()).in(Scopes.SINGLETON);
        }
    }

    /**
     * @return the available providers as found by {@link ServiceLoader} under the
     *         {@link StorageProvider} key.
     */
    public static Iterable<StorageProvider> findProviders() {
        ServiceLoader<StorageProvider> loader = ServiceLoader.load(StorageProvider.class);
        return ImmutableList.copyOf(loader.iterator());
    }
}
