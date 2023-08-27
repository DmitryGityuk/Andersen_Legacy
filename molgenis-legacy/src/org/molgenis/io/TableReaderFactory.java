package org.molgenis.io;

import org.molgenis.io.strategy.CreateStrategyContext;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

public final class TableReaderFactory {
    private final static CreateStrategyContext CREATE_STRATEGY_CONTEXT = new CreateStrategyContext();

    private TableReaderFactory() {
    }

    public static TableReader create(File file) throws IOException {
        return createTableReader(file);
    }

    public static TableReader create(List<File> files) throws IOException {
        files.forEach(TableReaderFactory::checkFile);

        AggregateTableReader tableReader = new AggregateTableReader();
        files.forEach(file -> {
            try {
                tableReader.addTableReader(createTableReader(file));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        return tableReader;
    }

    private static TableReader createTableReader(File file) throws IOException {
        checkFile(file);

        return CREATE_STRATEGY_CONTEXT.executeCreationStrategy(file);
    }

    private static void checkFile(File file) {
        Predicate<File> isNull = Objects::isNull;
        Predicate<File> isNotFile = f -> !f.isFile();

        if (isNull.or(isNotFile).test(file)) {
            String errorMessage = isNull.test(file) ? "file is null"
                    : "file is not a file: " + file.getName();
            throw new IllegalArgumentException(errorMessage);
        }
    }
}
