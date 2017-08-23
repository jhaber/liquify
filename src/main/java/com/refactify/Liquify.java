package com.refactify;

import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;

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
import liquibase.database.core.MySQLDatabase;
import liquibase.exception.LiquibaseException;
import liquibase.exception.MigrationFailedException;
import liquibase.exception.SetupException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.executor.LoggingExecutor;
import liquibase.parser.ChangeLogParser;
import liquibase.parser.ChangeLogParserFactory;
import liquibase.resource.FileSystemResourceAccessor;
import liquibase.resource.ResourceAccessor;
import liquibase.serializer.ChangeLogSerializer;
import liquibase.serializer.ChangeLogSerializerFactory;

public class Liquify {
    private final static ConversionArgumentsParser parser = new ConversionArgumentsParser();
    private final static UsagePrinter usagePrinter = new UsagePrinter();
    private final static TargetFileNameBuilder targetFileNameBuilder = new TargetFileNameBuilder();

    public static void main(final String[] args) {
        //ChangeLogSerializerFactory.getInstance().register(new FormattedSqlChangeLogSerializer());

        ConversionArguments conversionArguments = parser.parseArguments(args);
        if(conversionArguments.areValid()) {
            String targetFileName = targetFileNameBuilder.buildFilename(conversionArguments);

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

        writer.write("--liquibase formatted sql\n\n");

        for (ChangeSet changeSet : changeLog.getChangeSets()) {
            String author = changeSet.getAuthor()
                    .replaceAll("\\s+", "_")
                    .replace("_(generated)","");

            writer.write("--changeset " + author + ":" + changeSet.getId() + "\n");
            for (Change change : changeSet.getChanges()) {
                try {
                    change.init();
                } catch (SetupException se) {
                    throw new MigrationFailedException(changeSet, se);
                }
            }

            for (Change change : changeSet.getChanges()) {
                mysql().executeStatements(change, changeLog, changeSet.getSqlVisitors());
            }
        }
    }

    private static void convertDatabaseChangeLog(final ConversionArguments conversionArguments) {
        String targetFileName = targetFileNameBuilder.buildFilename(conversionArguments);
        try {
            ResourceAccessor resourceAccessor = new FileSystemResourceAccessor(System.getProperty("user.dir"));
            ChangeLogParser parser = ChangeLogParserFactory.getInstance().getParser(conversionArguments.getSource(), resourceAccessor);
            DatabaseChangeLog changeLog = parser.parse(conversionArguments.getSource(), new ChangeLogParameters(), resourceAccessor);
            ChangeLogSerializer serializer = ChangeLogSerializerFactory.getInstance().getSerializer(targetFileName);
            for (ChangeSet set : changeLog.getChangeSets()) {
                setFilePath(set, targetFileName);
            }
            serializer.write(changeLog.getChangeSets(), new FileOutputStream(targetFileName));
        }
        catch (LiquibaseException e) {
            System.out.println("There was a problem parsing the source file.");
            deleteTargetFile(targetFileName);
        }
        catch (IOException e) {
            System.out.println("There was a problem serializing the source file.");
            deleteTargetFile(targetFileName);
        }
        catch(IllegalStateException e) {
            System.out.println(String.format("Database generator for type '%s' was not found.",
                    conversionArguments.getDatabase()));
            deleteTargetFile(targetFileName);
        }
    }

    private static void deleteTargetFile(final String targetFileName) {
        try {
            Files.deleteIfExists(Paths.get(targetFileName));
        }
        catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    private static void setFilePath(ChangeSet changeSet, String filePath) {
        try {
            Field f = ChangeSet.class.getDeclaredField("filePath");
            f.setAccessible(true);
            f.set(changeSet, filePath);
        } catch (Exception e) {
            throw new RuntimeException(e);
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
