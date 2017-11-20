package com.refactify.printer;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

public class SqlFormattingWriter extends Writer {
  private static final String WHITESPACE = " \n\r\f\t";

  private final Writer delegate;
  private final StringBuilder buffer;

  public SqlFormattingWriter(Writer delegate) {
    this.delegate = delegate;
    this.buffer = new StringBuilder();
  }

  @Override
  public void write(char[] cbuf, int off, int len) throws IOException {
    for (int i = off; i < off + len; i++) {
      char c = cbuf[i];

      if (buffer.length() == 0 && WHITESPACE.indexOf(c) > -1) {
        delegate.write(c);
      } else {
        buffer.append(c);
        if (c == ';') {
          flushSql();
        }
      }

    }
  }

  @Override
  public void flush() throws IOException {
    delegate.flush();
  }

  @Override
  public void close() throws IOException {
    if (buffer.length() > 0) {
      delegate.write(buffer.toString());
      buffer.setLength(0);
    }
    delegate.close();
  }

  private void flushSql() throws IOException {
    String unformatted = buffer.toString();
    String formatted = formatSql(unformatted);

    delegate.write(formatted);
    buffer.setLength(0);
  }

  private String formatSql(String unformatted) {
    if (!unformatted.startsWith("CREATE TABLE")) {
      return unformatted;
    }

    StringBuilder formatted = new StringBuilder();
    int openParen = unformatted.indexOf('(');
    if (openParen == -1) {
      return unformatted;
    }
    formatted.append(unformatted.substring(0, openParen + 1));

    int closeParen = unformatted.lastIndexOf(')');

    String tableDef = unformatted.substring(openParen + 1, closeParen);

    formatted.append('\n');
    for (String line : splitLines(tableDef)) {
      formatted.append("  ").append(line).append("\n");
    }

    formatted.append(')');
    // force a space after the close paren
    if (unformatted.charAt(closeParen + 1) != ' ' && unformatted.charAt(closeParen + 1) != ';') {
      formatted.append(' ');
    }
    formatted.append(unformatted.substring(closeParen + 1));

    return formatted.toString();
  }

  // don't split enum defs onto separate lines
  private List<String> splitLines(String tableDef) {
    List<String> lines = new ArrayList<>();
    StringBuilder currentLine = new StringBuilder();
    int openParens = 0;

    for (char c : tableDef.toCharArray()) {
      if (currentLine.length() == 0 && WHITESPACE.indexOf(c) > -1) {
        continue;
      }

      currentLine.append(c);

      if (c == '(') {
        openParens++;
      } else if (c == ')') {
        openParens--;
      } else if (c == ',' && openParens == 0) {
        lines.add(currentLine.toString());
        currentLine = new StringBuilder();
      }
    }

    if (currentLine.length() > 0) {
      lines.add(currentLine.toString());
      currentLine = null;
    }

    return lines;
  }
}
