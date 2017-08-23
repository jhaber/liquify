package com.refactify.serializer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import liquibase.change.Change;
import liquibase.change.ColumnConfig;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.serializer.ChangeLogSerializer;
import liquibase.sql.Sql;
import liquibase.sql.visitor.SqlVisitor;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.SqlStatement;

public class FormattedSqlChangeLogSerializer  implements ChangeLogSerializer {
  private static Pattern fileNamePatter = Pattern.compile(".*\\.(\\w+)\\.sql");

  @Override
  public String[] getValidFileExtensions() {
    return new String[] {
            "sql"
    };
  }

  @Override
  public String serialize(DatabaseChangeLog databaseChangeLog) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String serialize(ChangeSet changeSet) {
    StringBuilder builder = new StringBuilder();

    Database database = getTargetDatabase(changeSet);

    String author = (changeSet.getAuthor()).replaceAll("\\s+", "_");
    author = author.replace("_(generated)","");

    builder.append("--changeset ").append(author).append(":").append(changeSet.getId()).append("\n");
    for (Change change : changeSet.getChanges()) {
      for (SqlStatement sqlStatement : change.generateStatements(database)) {
        Sql[] sqls = SqlGeneratorFactory.getInstance().generateSql(sqlStatement, database);
        if (sqls != null) {
          for (Sql sql : sqls) {
            builder.append(sql.toSql()).append(sql.getEndDelimiter()).append("\n");
          }
        }
      }
    }

    return builder.toString();
  }

  @Override
  public String serialize(Change change) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String serialize(SqlVisitor visitor) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String serialize(ColumnConfig columnConfig) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(List<ChangeSet> changeSets, OutputStream out) throws IOException {
    StringBuilder builder = new StringBuilder();
    builder.append("--liquibase formatted sql\n\n");

    for (ChangeSet changeSet : changeSets) {
      builder.append(serialize(changeSet));
      builder.append("\n");
    }

    out.write(builder.toString().getBytes("UTF-8"));
  }

  protected Database getTargetDatabase(ChangeSet changeSet) {
    String filePath = changeSet.getFilePath();
    if (filePath == null) {
      throw new UnexpectedLiquibaseException("You must specify the changelog file name as filename.DB_TYPE.sql. Example: changelog.mysql.sql");
    }
    Matcher matcher = fileNamePatter.matcher(filePath);
    if (!matcher.matches()) {
      throw new UnexpectedLiquibaseException("Serializing changelog as sql requires a file name in the format *.databaseType.sql. Example: changelog.h2.sql. Passed: "+filePath);
    }
    String shortName = matcher.replaceFirst("$1");

    for (Database database : DatabaseFactory.getInstance().getImplementedDatabases()) {
      if (database.getTypeName().equals(shortName)) {
        return database;
      }
    }

    throw new UnexpectedLiquibaseException("Serializing changelog as sql requires a file name in the format *.databaseType.sql. Example: changelog.h2.sql. Unknown databaes type: "+shortName);
  }
}
