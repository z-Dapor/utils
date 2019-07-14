/**
 * FileName: FileUnCompress
 * Date:     2019/7/8 12:06
 * Description: unCompress
 * History:
 * <author>          <time>          <version>          <desc>
 * 臧浩鹏           12:06           v0.1              unCompress
 */

package com.zhp.fileutils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyInputStream;

public class FileUnCompress {

  Logger log = LoggerFactory.getLogger(getClass());
  private final byte[] BUFFER_SIZE = new byte[1024 * 1024 * 8];

  public String useSnappyUnCompressByte(byte[] s) {
    String res = null;
    try {
      res = new String(Snappy.uncompress(s));
    } catch (IOException e) {
      log.warn("{}... uncompress error ", s);
      e.printStackTrace();
    }
    return res;
  }

  public void useSnappyCompressFile(File in, File out) {
    FileInputStream fi = null;
    FileOutputStream fo = null;
    SnappyInputStream sin = null;
    try {
      fo = new FileOutputStream(out);
      fi = new FileInputStream(in);
      sin = new SnappyInputStream(fi);
      while (true) {
        int count = sin.read(BUFFER_SIZE, 0, BUFFER_SIZE.length);
        if (count == -1) {
          break;
        }
        fo.write(BUFFER_SIZE, 0, count);
      }
      fo.flush();
    } catch (IOException ex) {
      log.warn("uncompress file failed: {}",ex.getMessage());
      ex.printStackTrace();
    } finally {
      IOUtils.closeQuietly(sin);
      IOUtils.closeQuietly(fi);
      IOUtils.closeQuietly(fo);
    }
  }

  public void useGzipUnCompressFile(String pathIn, String pathOut) {
    InputStream fin = null;
    OutputStream out = null;
    GzipCompressorInputStream gzIn = null;
    try {
      fin = Files.newInputStream(Paths.get(pathIn));
      BufferedInputStream in = new BufferedInputStream(fin);
      out = Files.newOutputStream(Paths.get(pathOut));
      gzIn = new GzipCompressorInputStream(in);
      int n = 0;
      while (-1 != (n = gzIn.read(BUFFER_SIZE))) {
        out.write(BUFFER_SIZE, 0, n);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }finally {
      IOUtils.closeQuietly(out);
      IOUtils.closeQuietly(gzIn);
    }
  }

  public static void main(String[] args) {
    FileUnCompress fileUnCompress = new FileUnCompress();
    fileUnCompress.useSnappyCompressFile(new File("D:/hadoop/phone_md5.snappy"),new File("D:/hadoop/phone_md5_uncompress"));
  }

}
