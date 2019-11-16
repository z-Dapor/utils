package com.zhp.fileutils.ftp;

import com.zhp.common.MyParameterTool;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.net.ftp.FtpDirEntry;

public class FtpClient implements Closeable {

  private String ip;
  private int port;
  private String username;
  private String password;
  private int connectTimeout;
  private int connectRetrytimes;
  private int connectRetryDelay;
  private int readTimeout;
  private int downloadTimeout;
  private int downloadRetryTimes;
  private int downloadRetryInterval;
  private int bufferSize;
  private String home;
  static Logger log = LoggerFactory.getLogger(FtpClient.class);
  sun.net.ftp.FtpClient ftpClient;
  private static MyParameterTool parameterTool;

  static {
    try {
      parameterTool = MyParameterTool.fromPropertiesFile(
          FtpClient.class.getClass().getClassLoader()
              .getResourceAsStream("MyUtil.properties"));
    } catch (IOException e) {
      log.warn("load util properties error: {}", e.getMessage());
    }
  }

  public FtpClient(String propertyFile) {
    this.ip = parameterTool.get(propertyFile + ".ftp.ip");
    log.info("ftp ip:{}", ip);
    this.port = parameterTool.getInt(propertyFile + ".ftp.port");
    log.info("ftp port:{}", port);
    this.username = parameterTool.get(propertyFile + ".ftp.username");
    log.info("ftp username:{}", username);
    this.password = parameterTool.get(propertyFile + ".ftp.password");
    log.info("ftp password:{}", password);
    this.home = parameterTool.get(propertyFile + ".ftp.home");
    log.info("ftp home:{}", home);
    this.connectTimeout = parameterTool.getInt(propertyFile + ".ftp.connect.timeout");
    log.info("ftp connectTimeout:{}", connectTimeout);
    this.connectRetrytimes = parameterTool.getInt(propertyFile + ".ftp.connect.retry.times");
    log.info("ftp connectRetrytimes:{}", connectRetrytimes);
    this.connectRetryDelay = parameterTool
        .getInt(propertyFile + ".ftp.connect.retry.interval.delay");
    log.info("ftp connectRetryDelay:{}", connectRetryDelay);
    this.readTimeout = parameterTool.getInt(propertyFile + ".ftp.read.timeout");
    log.info("ftp readTimeout:{}", readTimeout);
    this.downloadTimeout = parameterTool.getInt(propertyFile + ".ftp.download.timeout");
    log.info("ftp downloadTimeout:{}", downloadTimeout);
    this.downloadRetryTimes = parameterTool.getInt(propertyFile + ".ftp.download.retry.times");
    log.info("ftp downloadRetryTimes:{}", downloadRetryTimes);
    this.downloadRetryInterval = parameterTool
        .getInt(propertyFile + ".ftp.download.retry.interval.delay");
    log.info("ftp downloadRetryInterval:{}", downloadRetryInterval);
    this.bufferSize = parameterTool.getInt(propertyFile + ".ftp.download.buffer.size");
    log.info("ftp bufferSize:{}", bufferSize);
    reinit();
  }

  private void reinit(int retryTimes) {
    log.info("reconnect:{}", retryTimes);
    try {
      IOUtils.closeQuietly(ftpClient);
      InetSocketAddress address = new InetSocketAddress(ip, port);
      ftpClient = sun.net.ftp.FtpClient.create();
      ftpClient.setConnectTimeout(connectTimeout);
      ftpClient.setReadTimeout(readTimeout);
      ftpClient.connect(address, port);
      ftpClient.login(username, password.toCharArray());
      ftpClient.changeDirectory(home);
    } catch (Exception e) {
      if (retryTimes > 0) {
        try {
          Thread.sleep(connectRetryDelay);
        } catch (InterruptedException e1) {
          e1.printStackTrace();
        }
        reinit(retryTimes - 1);
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  private void reinit() {
    reinit(connectRetrytimes);
  }

  public FtpClient() {
    this("ftp");
  }

  public void downloadFile(String ftpPath, String localPath) throws TimeoutException, IOException {
    downloadFile(ftpPath, localPath, downloadTimeout, downloadRetryTimes, downloadRetryInterval);
  }

  public void downloadFile(String ftpPath, String localPath, int timeout, int retryTimes,
      int intervalDelay) throws TimeoutException, IOException {
    try {
      log.info("start download {}", ftpPath);
      downloadFile(ftpPath, localPath, timeout);
      log.info("download to {} success", localPath);
    } catch (TimeoutException | IOException e) {
      if (retryTimes < 1 || e.getCause() instanceof FileNotFoundException) {
        throw e;
      }
      try {
        log.info("retry:{} sleep..", retryTimes);
        Thread.sleep(intervalDelay);
        log.info("reinit ftp client ...");
        reinit();
      } catch (Exception e1) {
        e1.printStackTrace();
      }
      downloadFile(ftpPath, localPath, timeout, retryTimes - 1, intervalDelay);
    }
  }

  public void downloadFile(String ftpPath, String localPath, int timeout)
      throws IOException, TimeoutException {
    InputStream inputStream = null;
    FileOutputStream fileOutputStream = null;
    try {
      final InputStream tmpInputStream = ftpClient.getFileStream(ftpPath);
      inputStream = tmpInputStream;

      new File(localPath).getParentFile().mkdirs();
      final FileOutputStream tmpFileOutputStream = new FileOutputStream(localPath);
      fileOutputStream = tmpFileOutputStream;

      final MutableBoolean isFinished = new MutableBoolean(false);
      Runnable run = new Runnable() {
        @Override
        public void run() {
          byte[] buffer = new byte[bufferSize];
          int EOF = -1;
          int n = 0;
          try {
            while (EOF != (n = tmpInputStream.read(buffer))) {
              if (Thread.interrupted()) {
                log.info("download thread interrupted");
                throw new InterruptedIOException("download thread interrupted");
              }
              tmpFileOutputStream.write(buffer, 0, n);
            }
            isFinished.setValue(true);
          } catch (IOException e) {
            return;
          } finally {
            IOUtils.closeQuietly(tmpInputStream);
            IOUtils.closeQuietly(tmpFileOutputStream);
          }
        }
      };
      Thread ftpThread = new Thread(run);
      ftpThread.start();
      try {
        ftpThread.join(timeout);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      ftpThread.interrupt();
      if (!isFinished.booleanValue()) {
        throw new TimeoutException(
            "download file from ftp:" + ftpPath + " to local:" + localPath + " timeout in "
                + timeout + " msecs");
      }
    } catch (TimeoutException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      IOUtils.closeQuietly(inputStream);
      IOUtils.closeQuietly(fileOutputStream);
    }
  }

  public sun.net.ftp.FtpClient getOriginalFtpClient() {
    return ftpClient;
  }

  @Override
  public void close() throws IOException {
    IOUtils.closeQuietly(ftpClient);
  }

  public static void main(String[] args) throws Exception {
    FtpClient ftpClient = new FtpClient();
    System.out.println(ftpClient.ftpClient.getWorkingDirectory());
    for (Iterator<FtpDirEntry> it = ftpClient.ftpClient.listFiles("."); it.hasNext(); ) {
      FtpDirEntry f = it.next();
      System.out.println(f.getName());
    }
    ftpClient.downloadFile("/tmp/ftp/MPS/CF/20171204/39764155d9ab9e0e2ac90f4503f14625.gz",
        "C:\\Users\\99577\\Desktop\\base_path3\\39764155d9ab9e0e2ac90f4503f14625.gz", 40000);
  }
}
