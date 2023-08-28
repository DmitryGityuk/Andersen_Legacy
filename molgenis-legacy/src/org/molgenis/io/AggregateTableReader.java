package org.molgenis.io;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AggregateTableReader implements TableReader {
    private final List<TableReader> tableReaders;
    private final Map<String, TupleReader> tupleReaders;

    AggregateTableReader() {
        tableReaders = new ArrayList<>();
        tupleReaders = new LinkedHashMap<>();
    }

    @Override
    public Iterator<TupleReader> iterator() {
        return Collections.unmodifiableCollection(tupleReaders.values()).iterator();
    }

    public void addTableReader(TableReader tableReader) throws IOException {
        tableReaders.add(tableReader);
        tableReader.getTableNames().forEach(tableName -> {
            try {
                tupleReaders.put(tableName, tableReader.getTupleReader(tableName));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void close() throws IOException {
        tableReaders.forEach(IOUtils::closeQuietly);
    }

    @Override
    public TupleReader getTupleReader(String tableName) {
        return tupleReaders.get(tableName);
    }

    @Override
    public Iterable<String> getTableNames() {
        return Collections.unmodifiableSet(tupleReaders.keySet());
    }
}
