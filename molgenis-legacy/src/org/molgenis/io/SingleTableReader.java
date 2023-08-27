package org.molgenis.io;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

public class SingleTableReader implements TableReader {
    private final TupleReader tupleReader;
    private final String tableName;

    public SingleTableReader(TupleReader tupleReader, String tableName) {
        if (tupleReader == null) throw new IllegalArgumentException("tuple reader is null");
        if (tableName == null) throw new IllegalArgumentException("table name is null");
        this.tupleReader = tupleReader;
        this.tableName = tableName;
    }

    @Override
    public Iterator<TupleReader> iterator() {
        return Collections.singletonList(tupleReader).iterator();
    }

    @Override
    public void close() throws IOException {
        tupleReader.close();
    }

    @Override
    public TupleReader getTupleReader(String tableName) {
        return this.tableName.equals(tableName) ? tupleReader : null;
    }

    @Override
    public Iterable<String> getTableNames() {
        return Collections.singletonList(tableName);
    }
}

