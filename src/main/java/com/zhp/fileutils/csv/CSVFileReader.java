package com.zhp.fileutils.csv;

import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVReader;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

public class CSVFileReader implements Closeable {

  private FileInputStream fis;
  private InputStreamReader isr;
  private CSVReader csvReader;

  public CSVFileReader(String fileName) throws IOException {
    this.fis = new FileInputStream(fileName);
    this.isr = new InputStreamReader(fis, "UTF-8");
    this.csvReader = new CSVReader(isr, CSVParser.DEFAULT_SEPARATOR,
        CSVParser.DEFAULT_QUOTE_CHARACTER, true);
  }

  public CSVFileReader(InputStream inputStream) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
    this.csvReader = new CSVReader(reader, CSVParser.DEFAULT_SEPARATOR,
        CSVParser.DEFAULT_QUOTE_CHARACTER, false);
  }

  public List<String[]> readAll() throws IOException {
    List<String[]> csvLines = this.csvReader.readAll();
    ArrayList<String[]> ans = new ArrayList<>(csvLines.size());
    for (String[] line : csvLines) {
      if (isEmptyLine(line)) {
        continue;
      }
      ans.add(line);
    }
    return ans;
  }

  private boolean isEmptyLine(String[] line) {
    return line == null || (line.length == 1 && StringUtils.isEmpty(line[0]));
  }

  public String[] readNext() throws IOException {
    return this.csvReader.readNext();
  }

  @Override
  public void close() throws IOException {
    IOUtils.closeQuietly(this.csvReader);
    IOUtils.closeQuietly(this.isr);
    IOUtils.closeQuietly(this.fis);
  }
}
