package org.molgenis.io.strategy;

import org.molgenis.io.TableReader;
import org.molgenis.io.excel.ExcelReader;

import java.io.File;
import java.io.IOException;

public class XlsOrXlsxFileStrategy implements CreateReaderStrategy {

    @Override
    public TableReader createTableReader(File file, String fileName) throws IOException {
        return new ExcelReader(file);
    }
}
