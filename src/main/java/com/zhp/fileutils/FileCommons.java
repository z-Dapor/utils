/**
 * FileName: FileCommons
 * Date:     2019/7/8 14:08
 * Description: common file utils
 * History:
 * <author>          <time>          <version>          <desc>
 * 臧浩鹏           14:08           v0.1              common file utils
 */

package com.zhp.fileutils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;

public class FileCommons {
  public static String getMD5(String file) throws IOException {
    return getMD5(new File(file));
  }

  public static String getMD5(File file) throws IOException {
    String md5;
    FileInputStream fis = null;
    try {
      fis = new FileInputStream(file);
      md5 = DigestUtils.md5Hex(fis);
    } finally {
      IOUtils.closeQuietly(fis);
    }
    return md5;
  }

  public static void writeToLocal(String[] lines, String fileName) {
    FileOutputStream fos = null;
    OutputStreamWriter osw = null;
    PrintWriter writer = null;
    try {
      File file = new File(fileName);
      file.getParentFile().mkdirs();
      fos = new FileOutputStream(file, false);
      osw = new OutputStreamWriter(fos, "UTF-8");
      writer = new PrintWriter(osw);
      for (String line : lines) {
        writer.println(line);
      }
      IOUtils.closeQuietly(writer);
      IOUtils.closeQuietly(osw);
      IOUtils.closeQuietly(fos);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void copyStream(InputStream in, OutputStream out){
    try {
      IOUtils.copy(in,out);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      IOUtils.closeQuietly(out);
      IOUtils.closeQuietly(in);
    }
  }

}
