/**
 * FileName: FileCompress
 * Date:     2019/7/8 11:07
 * Description: 文件压缩方式
 * History:
 * <author>          <time>          <version>          <desc>
 * 臧浩鹏           11:07           v0.1              文件压缩方式
 */

package com.zhp.fileutils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyOutputStream;

public class FileCompress {

  Logger log = LoggerFactory.getLogger(getClass());
  private final byte[] BUFFER_SIZE = new byte[1024 * 1024 * 8];


  public byte[] useSnappyCompressCharSequence(String s) {
    byte[] res = null;
    try {
      res = Snappy.compress(s);
    } catch (IOException e) {
      log.warn("{}... compress error ", s.subSequence(0, 10));
      e.printStackTrace();
    }
    return res;
  }

  public void useGzipCompressFile(String pathIn, String pathOut) {
    InputStream in = null;
    OutputStream fout = null;
    GzipCompressorOutputStream gzOut = null;
    try {
      in = Files.newInputStream(Paths.get(pathIn));
      fout = Files.newOutputStream(Paths.get(pathOut));
      BufferedOutputStream out = new BufferedOutputStream(fout);
      gzOut = new GzipCompressorOutputStream(out);
      int n = 0;
      while (-1 != (n = in.read(BUFFER_SIZE))) {
        gzOut.write(BUFFER_SIZE, 0, n);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }finally {
      IOUtils.closeQuietly(fout);
      IOUtils.closeQuietly(gzOut);
    }
  }

  public void useSnappyCompressFile(File in, File out) {
    //File file = new File("...");
    //File out = new File("./", file.getName() + ".snappy");

    FileInputStream fi = null;
    FileOutputStream fo = null;
    SnappyOutputStream sout = null;
    try {
      fi = new FileInputStream(in);
      fo = new FileOutputStream(out);
      sout = new SnappyOutputStream(fo);
      while (true) {
        int count = fi.read(BUFFER_SIZE, 0, BUFFER_SIZE.length);
        if (count == -1) {
          break;
        }
        sout.write(BUFFER_SIZE, 0, count);
      }
      sout.flush();
    } catch (IOException ex) {
      log.warn("compress file failed: {}",ex.getMessage());
      ex.printStackTrace();
    } finally {
      IOUtils.closeQuietly(sout);
      IOUtils.closeQuietly(fi);
      IOUtils.closeQuietly(fo);
    }
  }

  public static void main(String[] args) {
    FileCompress compress = new FileCompress();
    compress.useSnappyCompressFile(new File("C:/Users/51594/.IntelliJIdea2017.1/system/log/idea.log.1"),new File("D:/hadoop/phone_md5.snappy"));
  }


}
