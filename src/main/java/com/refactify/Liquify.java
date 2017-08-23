package com.refactify;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.refactify.arguments.ConversionArguments;
import com.refactify.arguments.ConversionArgumentsParser;
import com.refactify.arguments.TargetFileNameBuilder;
import com.refactify.printer.SqlFormattingWriter;
import com.refactify.printer.UsagePrinter;

import liquibase.change.Change;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.ObjectQuotingStrategy;
import liquibase.database.core.MySQLDatabase;
import liquibase.exception.MigrationFailedException;
import liquibase.exception.SetupException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.executor.LoggingExecutor;
import liquibase.parser.ChangeLogParser;
import liquibase.parser.ChangeLogParserFactory;
import liquibase.resource.FileSystemResourceAccessor;
import liquibase.resource.ResourceAccessor;

public class Liquify {
    private final static ConversionArgumentsParser parser = new ConversionArgumentsParser();
    private final static UsagePrinter usagePrinter = new UsagePrinter();
    private final static TargetFileNameBuilder targetFileNameBuilder = new TargetFileNameBuilder();

    public static void main(final String[] args) {
        ConversionArguments conversionArguments = parser.parseArguments(args);
        if(conversionArguments.areValid()) {
            String targetFileName = targetFileNameBuilder.buildFilename(conversionArguments);

            if (!conversionArguments.getOverwrite() && new File(targetFileName).exists()) {
                System.out.println("Target file " + targetFileName + " already exists, remove the existing file or pass '--overwrite' to allow overwriting");
                System.exit(1);
            }

            try (Writer writer = new FileWriter(targetFileName)) {
                Executor realExecutor = ExecutorService.getInstance().getExecutor(mysql());
                Executor loggingExecutor = new LoggingExecutor(realExecutor, new SqlFormattingWriter(writer), mysql());
                ExecutorService.getInstance().setExecutor(mysql(), loggingExecutor);

                executeChangeLog(conversionArguments, writer);
            } catch (Exception e) {
                deleteTargetFile(targetFileName);
                throw new RuntimeException(e);
            }
        }
        else {
            usagePrinter.printUsage();
        }
    }

    private static void executeChangeLog(final ConversionArguments conversionArguments, Writer writer) throws Exception {
        ResourceAccessor resourceAccessor = new FileSystemResourceAccessor(System.getProperty("user.dir"));
        ChangeLogParser parser = ChangeLogParserFactory.getInstance().getParser(conversionArguments.getSource(), resourceAccessor);
        DatabaseChangeLog changeLog = parser.parse(conversionArguments.getSource(), new ChangeLogParameters(), resourceAccessor);

        if (changeLog.getObjectQuotingStrategy() != null && changeLog.getObjectQuotingStrategy() != ObjectQuotingStrategy.LEGACY) {
            System.out.println("XML changelog has a top-level quoting strategy set, this isn't supported by SQL changelog");
            System.exit(1);
        }

        writer.write("--liquibase formatted sql");
        if (!changeLog.getFilePath().equals(changeLog.getPhysicalFilePath())) {
            writer.write(" logicalFilePath:" + changeLog.getLogicalFilePath());
        }
        writer.write("\n\n");

        for (ChangeSet changeSet : changeLog.getChangeSets()) {
            writer.write(changeSetDeclaration(changeSet));
            for (Change change : changeSet.getChanges()) {
                try {
                    change.finishInitialization();
                } catch (SetupException se) {
                    throw new MigrationFailedException(changeSet, se);
                }
            }

            for (Change change : changeSet.getChanges()) {
                mysql().executeStatements(change, changeLog, changeSet.getSqlVisitors());
            }
        }
    }

    private static String changeSetDeclaration(ChangeSet changeSet) {
        String author = changeSet.getAuthor()
                .replaceAll("\\s+", "_")
                .replace("_(generated)","");
        String id = changeSet.getId().replaceAll("\\s+", "_");

        List<String> parts = new ArrayList<>();
        parts.add("--changeset");
        parts.add(author + ":" + id);

        if (changeSet.getContexts() != null && !changeSet.getContexts().isEmpty()) {
            parts.add("context:" + join(changeSet.getContexts().getContexts(), ","));
        }

        if (changeSet.getLabels() != null && !changeSet.getLabels().isEmpty()) {
            parts.add("labels:" + join(changeSet.getLabels().getLabels(), ","));
        }

        if (changeSet.getDbmsSet() != null && !changeSet.getDbmsSet().isEmpty()) {
            parts.add("dbms:" + join(changeSet.getDbmsSet(), ","));
        }

        if (changeSet.isRunOnChange()) {
            parts.add("runOnChange:true");
        }

        if (changeSet.isAlwaysRun()) {
            parts.add("runAlways:true");
        }

        if (changeSet.getFailOnError() != null && !changeSet.getFailOnError()) {
            parts.add("failOnError:false");
        }

        if (changeSet.getOnValidationFail() != ChangeSet.ValidationFailOption.HALT) {
            parts.add("onValidationFail:" + changeSet.getOnValidationFail().name());
        }

        if (!changeSet.isRunInTransaction()) {
            parts.add("runInTransaction:false");
        }

        if (changeSet.getObjectQuotingStrategy() != null && changeSet.getObjectQuotingStrategy() != ObjectQuotingStrategy.LEGACY) {
            parts.add("objectQuotingStrategy:" + changeSet.getObjectQuotingStrategy().name());
        }

        return join(parts, " ") + "\n";
    }

    private static String join(Collection<String> strings, String separator) {
        String joined = "";

        Iterator<String> iterator = strings.iterator();
        if (!iterator.hasNext()) {
            return joined;
        }
        joined += iterator.next();

        while (iterator.hasNext()) {
            joined += separator;
            joined += iterator.next();
        }

        return joined;
    }

    private static void deleteTargetFile(final String targetFileName) {
        try {
            Files.deleteIfExists(Paths.get(targetFileName));
        }
        catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    private static Database mysql() {
        for (Database database : DatabaseFactory.getInstance().getImplementedDatabases()) {
            if (database instanceof MySQLDatabase) {
                return database;
            }
        }

        throw new RuntimeException();
    }
}
