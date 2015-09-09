/* Copyright (c) 2015 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.storage.bdbje;

import org.locationtech.geogig.di.StorageProvider;
import org.locationtech.geogig.di.VersionedFormat;
import org.locationtech.geogig.storage.GraphDatabase;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.locationtech.geogig.storage.RefDatabase;
import org.locationtech.geogig.storage.fs.FileRefDatabase;

public class JEStorageProviderV01 extends StorageProvider {

    public static final VersionedFormat VERSION = new VersionedFormat("bdbje", "0.1");

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
        return "Stores revision objects and graph objects in separate BerkeleyDB JE databases, refs in regular files.";
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
        return JEObjectDatabase_v0_1.class;
    }

    @Override
    protected Class<? extends RefDatabase> refsBinding() {
        return FileRefDatabase.class;
    }

    @Override
    protected Class<? extends GraphDatabase> graphBinding() {
        return JEGraphDatabase_v0_1.class;
    }

}
