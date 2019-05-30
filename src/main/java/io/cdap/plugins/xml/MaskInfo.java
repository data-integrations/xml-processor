package io.cdap.plugins.xml;

import org.unix4j.Unix4j;
import org.unix4j.builder.Unix4jCommandBuilder;

/**
*
*/
class MaskInfo {
  private final String fullPath;
  private final String regex;

  MaskInfo(String fullPath, String regex) {
    this.fullPath = fullPath;
    this.regex = regex;
    // Verify if the provided regex is valid.
    Unix4jCommandBuilder buider = Unix4j.echo("").sed(regex);
    buider.toExitValue();
  }

  public String getFullPath() {
    return fullPath;
  }

  public String doReplace(String original) {
    Unix4jCommandBuilder builder = Unix4j.echo(original).sed(regex);
    if (builder.toExitValue() == 0) {
      return builder.toStringResult();
    }
    return original;
  }
}
