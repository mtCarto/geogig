/* Copyright (c) 2014 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.storage.sqlite;

import static java.lang.String.format;
import static org.locationtech.geogig.storage.sqlite.Xerial.log;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.locationtech.geogig.api.ObjectId;
import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.api.RevObject;
import org.locationtech.geogig.storage.BlobStore;
import org.locationtech.geogig.storage.BulkOpListener;
import org.locationtech.geogig.storage.ConfigDatabase;
import org.locationtech.geogig.storage.fs.FileBlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteDataSource;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;

/**
 * Object database based on Xerial SQLite jdbc driver.
 */
public class XerialObjectDatabaseV2SingleConnection extends SQLiteObjectDatabase<Connection> {

    static Logger LOG = LoggerFactory.getLogger(XerialObjectDatabaseV2SingleConnection.class);

    static final String OBJECTS = "objects";

    final int partitionSize = 1_000; // TODO make configurable

    final String dbName;

    private XerialConflictsDatabaseSingleConnection conflicts;

    private FileBlobStore blobStore;

    @Inject
    public XerialObjectDatabaseV2SingleConnection(ConfigDatabase configdb, Platform platform) {
        this(configdb, platform, "objects");
    }

    public XerialObjectDatabaseV2SingleConnection(ConfigDatabase configdb, Platform platform,
            String dbName) {
        super(configdb, platform, SQLiteStorage.VERSION_2);
        this.dbName = dbName;
    }

    @Override
    protected Connection connect(File geogigDir) {
        SQLiteDataSource dataSource = Xerial.newDataSource(new File(geogigDir, dbName + ".db"));
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected void close(Connection ds) {
        try {
            ds.close();
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void init(Connection ds) {
        new DbOp<Void>() {

            @Override
            protected Void doRun(Connection cx) throws SQLException {
                String createTable = format(
                        "CREATE TABLE IF NOT EXISTS %s (inthash int, id varchar, object blob, PRIMARY KEY(inthash, id))",
                        OBJECTS);

                try (Statement stmt = cx.createStatement()) {
                    stmt.execute(log(createTable, LOG));
                }
                return null;
            }
        }.run(ds);

        conflicts = new XerialConflictsDatabaseSingleConnection(ds);
        conflicts.open();
        blobStore = new FileBlobStore(platform);
        blobStore.open();
    }

    @Override
    public XerialConflictsDatabaseSingleConnection getConflictsDatabase() {
        return conflicts;
    }

    @Override
    public BlobStore getBlobStore() {
        return blobStore;
    }

    @Override
    public boolean has(final ObjectId id, Connection ds) {

        return new DbOp<Boolean>() {
            @Override
            protected Boolean doRun(Connection cx) throws SQLException {
                String sql = format(
                        "SELECT EXISTS (SELECT inthash FROM %s WHERE inthash = ? AND id = ?)",
                        OBJECTS);

                int intHash = intHash(id);
                try (PreparedStatement ps = cx.prepareStatement(log(sql, LOG, intHash, id))) {
                    ps.setInt(1, intHash);
                    ps.setString(2, id.toString());

                    try (ResultSet rs = ps.executeQuery()) {
                        boolean exists = rs.next() ? rs.getInt(1) > 0 : false;
                        return exists;
                    }
                }
            }
        }.run(ds);
    }

    @Override
    public Iterable<String> search(final String partialId, Connection ds) {

        final byte[] raw = ObjectId.toRaw(partialId);
        Preconditions.checkArgument(raw.length >= 4,
                "Partial id must be at least 8 characters long: %s", partialId);

        final int inthash = intHash(raw);

        Iterable<String> matches = new DbOp<Iterable<String>>() {
            @Override
            protected Iterable<String> doRun(Connection cx) throws IOException, SQLException {
                String sql = format("SELECT id FROM %s WHERE inthash = ? AND id LIKE ? LIMIT 100",
                        OBJECTS, partialId);
                final String searchKey = partialId + "%";
                try (PreparedStatement ps = cx.prepareStatement(log(sql, LOG, inthash, searchKey))) {
                    ps.setInt(1, inthash);
                    ps.setString(2, searchKey);

                    List<String> matchList = new ArrayList<>();
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            matchList.add(rs.getString(1));
                        }
                    }
                    return matchList;
                }
            }
        }.run(ds);
        return matches;
    }

    @Override
    public InputStream get(final ObjectId id, Connection ds) {
        return new DbOp<InputStream>() {
            @Override
            protected InputStream doRun(Connection cx) throws SQLException {
                String sql = format("SELECT object FROM %s WHERE inthash = ? AND id = ?", OBJECTS);

                int intHash = intHash(id);

                try (PreparedStatement ps = cx.prepareStatement(log(sql, LOG, intHash, id))) {
                    ps.setInt(1, intHash);
                    ps.setString(2, id.toString());

                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) {
                            return null;
                        }
                        byte[] bytes = rs.getBytes(1);
                        return new ByteArrayInputStream(bytes);
                    }
                }
            }
        }.run(ds);
    }

    @Override
    public void put(final ObjectId id, final InputStream obj, Connection ds) {
        new DbOp<Void>() {
            @Override
            protected Void doRun(Connection cx) throws SQLException, IOException {
                String sql = format("INSERT OR IGNORE INTO %s (inthash,id,object) VALUES (?,?,?)",
                        OBJECTS);

                int intHash = intHash(id);

                try (PreparedStatement ps = cx.prepareStatement(log(sql, LOG, intHash, id, obj))) {
                    ps.setInt(1, intHash);
                    ps.setString(2, id.toString());
                    ps.setBytes(3, ByteStreams.toByteArray(obj));
                    ps.executeUpdate();
                    return null;
                }
            }
        }.run(ds);
    }

    @Override
    public boolean delete(final ObjectId id, Connection ds) {
        return new DbOp<Boolean>() {
            @Override
            protected Boolean doRun(Connection cx) throws SQLException {
                String sql = format("DELETE FROM %s WHERE inthash = ? AND id = ?", OBJECTS);

                int intHash = intHash(id);

                try (PreparedStatement ps = cx.prepareStatement(log(sql, LOG, intHash, id))) {
                    ps.setInt(1, intHash);
                    ps.setString(2, id.toString());
                    return ps.executeUpdate() > 0;
                }
            }
        }.run(ds);
    }

    private ExecutorService txExecutor = Executors.newSingleThreadExecutor();

    private <T> Future<T> runTx(final DbOp<T> dbop) {

        Callable<T> task = new Callable<T>() {

            @Override
            public T call() throws Exception {
                return dbop.run(cx);
            }
        };

        Future<T> future = txExecutor.submit(task);
        return future;
    }

    /**
     * Override to optimize batch insert.
     */
    @Override
    public void putAll(final Iterator<? extends RevObject> objects, final BulkOpListener listener) {
        Preconditions.checkState(isOpen(), "No open database connection");

        DbOp<Void> dbop = new DbOp<Void>() {
            @Override
            protected boolean isAutoCommit() {
                return false;
            }

            @Override
            protected Void doRun(Connection cx) throws SQLException, IOException {
                // use INSERT OR IGNORE to deal with duplicates cleanly
                String sql = format(
                        "INSERT OR IGNORE INTO %s (inthash, id, object) VALUES (?,?,?)", OBJECTS);
                try (PreparedStatement stmt = cx.prepareStatement(log(sql, LOG))) {

                    // partition the objects into chunks for batch processing
                    @SuppressWarnings({ "unchecked", "rawtypes" })
                    Iterator<List<? extends RevObject>> it = (Iterator) Iterators.partition(
                            objects, partitionSize);

                    while (it.hasNext()) {
                        List<? extends RevObject> objs = it.next();
                        for (RevObject obj : objs) {
                            ObjectId id = obj.getId();
                            stmt.setInt(1, intHash(id));
                            stmt.setString(2, id.toString());
                            stmt.setBytes(3, ByteStreams.toByteArray(writeObject(obj)));
                            stmt.addBatch();
                        }

                        int[] batchResults = stmt.executeBatch();
                        notifyInserted(batchResults, objs, listener);
                        stmt.clearParameters();
                    }
                    cx.commit();
                } catch (SQLException e) {
                    cx.rollback();
                    e.printStackTrace();
                    throw e;
                }
                return null;
            }
        };

        Future<Void> res = runTx(dbop);
        try {
            res.get();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ExecutionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    void notifyInserted(int[] inserted, List<? extends RevObject> objects, BulkOpListener listener) {
        for (int i = 0; i < inserted.length; i++) {
            if (inserted[i] > 0) {
                listener.inserted(objects.get(i).getId(), null);
            }
        }
    }

    /**
     * Override to optimize batch delete.
     */
    @Override
    public long deleteAll(final Iterator<ObjectId> ids, final BulkOpListener listener) {
        Preconditions.checkState(isOpen(), "No open database connection");
        return new DbOp<Long>() {
            @Override
            protected boolean isAutoCommit() {
                return false;
            }

            @Override
            protected Long doRun(Connection cx) throws SQLException, IOException {
                String sql = format("DELETE FROM %s WHERE inthash = ? AND id = ?", OBJECTS);

                long count = 0;
                try (PreparedStatement stmt = cx.prepareStatement(log(sql, LOG))) {

                    // partition the objects into chunks for batch processing
                    Iterator<List<ObjectId>> it = Iterators.partition(ids, partitionSize);

                    while (it.hasNext()) {
                        List<ObjectId> l = it.next();
                        for (ObjectId id : l) {
                            stmt.setInt(1, intHash(id));
                            stmt.setString(2, id.toString());
                            stmt.addBatch();
                        }

                        count += notifyDeleted(stmt.executeBatch(), l, listener);
                        stmt.clearParameters();
                    }
                    cx.commit();
                }
                return count;
            }
        }.run(cx);
    }

    long notifyDeleted(int[] deleted, List<ObjectId> ids, BulkOpListener listener) {
        long count = 0;
        for (int i = 0; i < deleted.length; i++) {
            if (deleted[i] > 0) {
                count++;
                listener.deleted(ids.get(i));
            }
        }
        return count;
    }

    public static int intHash(ObjectId id) {
        int hash1 = (id.byteN(0) << 24) //
                + (id.byteN(1) << 16) //
                + (id.byteN(2) << 8) //
                + (id.byteN(3) << 0);
        return hash1;
    }

    public static int intHash(byte[] id) {
        int hash1 = (id[0] << 24) //
                + (id[1] << 16) //
                + (id[2] << 8) //
                + (id[3] << 0);
        return hash1;
    }
}
