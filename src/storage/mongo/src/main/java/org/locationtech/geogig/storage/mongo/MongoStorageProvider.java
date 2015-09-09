/* Copyright (c) 2015 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.storage.mongo;

import org.locationtech.geogig.di.StorageProvider;
import org.locationtech.geogig.di.VersionedFormat;
import org.locationtech.geogig.storage.GraphDatabase;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.locationtech.geogig.storage.RefDatabase;
import org.locationtech.geogig.storage.fs.FileRefDatabase;

public class MongoStorageProvider extends StorageProvider {

    static final String NAME = "mongodb";

    static final String VERSION = "0.1";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getVersion() {
        return VERSION;
    }

    @Override
    public String getDescription() {
        return "Stores revision and graph objects in Mongodb, refs in regular files";
    }

    @Override
    public VersionedFormat getObjectDatabaseFormat() {
        return MongoObjectDatabase.VERSION;
    }

    @Override
    public VersionedFormat getGraphDatabaseFormat() {
        return MongoGraphDatabase.VERSION;
    }

    @Override
    public VersionedFormat getRefsDatabaseFormat() {
        return FileRefDatabase.VERSION;
    }

    @Override
    protected Class<? extends ObjectDatabase> objectsBinding() {
        return MongoObjectDatabase.class;
    }

    @Override
    protected Class<? extends RefDatabase> refsBinding() {
        return FileRefDatabase.class;
    }

    @Override
    protected Class<? extends GraphDatabase> graphBinding() {
        return MongoGraphDatabase.class;
    }

}
