/* Copyright (c) 2015 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.storage.sqlite;

import org.locationtech.geogig.di.StorageProvider;
import org.locationtech.geogig.di.VersionedFormat;
import org.locationtech.geogig.storage.GraphDatabase;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.locationtech.geogig.storage.RefDatabase;
import org.locationtech.geogig.storage.fs.FileRefDatabase;

public class XerialStorageProviderV2 extends StorageProvider {

    private static final VersionedFormat VERSION = SQLiteStorage.VERSION_2;

    @Override
    public String getName() {
        return VERSION.getFormat();
    }

    @Override
    public String getVersion() {
        return VERSION.getVersion();
    }

    @Override
    public String getDescription() {
        return "Stores revision and graph objects in SQLite, refs in regular files";
    }

    @Override
    public VersionedFormat getObjectDatabaseFormat() {
        return VERSION;
    }

    @Override
    public VersionedFormat getGraphDatabaseFormat() {
        return VERSION;
    }

    @Override
    public VersionedFormat getRefsDatabaseFormat() {
        return FileRefDatabase.VERSION;
    }

    @Override
    protected Class<? extends ObjectDatabase> objectsBinding() {
        return XerialObjectDatabaseV2.class;
    }

    @Override
    protected Class<? extends RefDatabase> refsBinding() {
        return FileRefDatabase.class;
    }

    @Override
    protected Class<? extends GraphDatabase> graphBinding() {
        return XerialGraphDatabaseV2.class;
    }

}
