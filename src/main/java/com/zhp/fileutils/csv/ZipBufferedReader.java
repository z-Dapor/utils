package com.zhp.fileutils.csv;

import static org.apache.commons.compress.utils.CharsetNames.UTF_8;

import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.io.IOUtils;

/**
 * @author
 * @desc read zip csv format file
 */
public class ZipBufferedReader implements Closeable {

  FileInputStream fileInputStream;
  ZipArchiveInputStream zipInputStream;
  CSVReader csvReader;

  public ZipBufferedReader(String fileName) throws IOException {
    this(new File(fileName));
  }

  public ZipBufferedReader(File fileName) throws IOException {
    this.fileInputStream = new FileInputStream(fileName);
    this.zipInputStream = new ZipArchiveInputStream(this.fileInputStream);
    nextEntryReader();
  }

  public String[] readLine() throws IOException {
    if (csvReader == null) {
      return null;
    }
    String[] line = csvReader.readNext();
    if (line != null) {
      return line;
    } else {
      nextEntryReader();
      return readLine();
    }
  }

  @Override
  public void close() throws IOException {
    IOUtils.closeQuietly(csvReader);
    IOUtils.closeQuietly(zipInputStream);
    IOUtils.closeQuietly(fileInputStream);
  }

  private void nextEntryReader() throws IOException {
    ArchiveEntry zipEntry = zipInputStream.getNextEntry();
    System.out.println("read entry:" + (zipEntry != null ? zipEntry.getName() : "null"));
    csvReader = zipEntry == null ? null :
        new CSVReader(
            new InputStreamReader(this.zipInputStream, UTF_8),
            CSVParser.DEFAULT_SEPARATOR,
            CSVParser.DEFAULT_QUOTE_CHARACTER,
            false);
  }

  public static void main(String[] args) throws IOException {
    File file = new File("D:/hadoop/album1000.zip");
    ZipBufferedReader reader = new ZipBufferedReader(file);
    System.out.println(reader.readLine()[0]);
    System.out.println(reader.readLine()[0]);
    IOUtils.closeQuietly(reader);
  }

}