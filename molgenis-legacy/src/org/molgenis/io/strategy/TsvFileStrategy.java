package org.molgenis.io.strategy;

import org.apache.commons.io.FilenameUtils;
import org.molgenis.io.SingleTableReader;
import org.molgenis.io.TableReader;
import org.molgenis.io.csv.CsvReader;

import java.io.File;
import java.io.IOException;

public class TsvFileStrategy implements CreateReaderStrategy {

    @Override
    public TableReader createTableReader(File file, String name) throws IOException {
        String tableName = FilenameUtils.getBaseName(name);
        return new SingleTableReader(new CsvReader(file, '\t'), tableName);
    }
}
