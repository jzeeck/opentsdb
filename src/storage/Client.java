// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.storage;

import com.stumbleupon.async.Deferred;
import org.hbase.async.*;


import java.util.ArrayList;

/**
 * A interface defining the functions any database used with TSDB must implement.
 * Another requirement is tha the database connection has to be asynchronous.
 */
public interface Client {

    @Deprecated
    public Deferred<Long> atomicIncrement(AtomicIncrementRequest air);

    @Deprecated
    public Deferred<Long> bufferAtomicIncrement(final AtomicIncrementRequest request);

    @Deprecated
    public Deferred<Boolean> compareAndSet(final PutRequest edit, final byte[] expected);

    @Deprecated
    public Deferred<Object> delete(final DeleteRequest request);

    /**
     * Verifies that the data and UID tables exist in data-source and optionally the
     * tree and meta data tables if the user has enabled meta tracking or tree
     * building
     * @return An ArrayList of objects to wait for
     * @throws TableNotFoundException
     * @since 2.0
     */
    @Deprecated
    public Deferred<ArrayList<Object>> ensureTableExists();

    @Deprecated
    public Deferred<Object> flush();

    @Deprecated
    public Deferred<ArrayList<KeyValue>> get(final GetRequest request);

    @Deprecated
    long getFlushInterval();

    @Deprecated
    public Scanner newScanner(final byte[] table);

    @Deprecated
    public Deferred<Object> put(final PutRequest request);

    @Deprecated
    void setFlushInterval(short aShort);

    public Deferred<Object> close();

    @Deprecated
    public ClientStats stats();

    void atomicIncrement(byte[] table, byte[] maxidRow, byte[] idFamily, byte[] bytes, long diff);

}
