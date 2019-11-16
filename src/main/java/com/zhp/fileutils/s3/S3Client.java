/**
 * FileName: S3Client
 * Date:     2019/6/24 9:53
 * Description: init S3 client
 * History:
 * <author>          <time>          <version>          <desc>
 * 臧浩鹏           9:53           v0.1              init S3 client
 */

package com.zhp.fileutils.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.Transfer.TransferState;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.zhp.common.MyParameterTool;
import com.zhp.fileutils.csv.CSVFileReader;
import com.zhp.fileutils.ftp.FtpClient;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Objects;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3Client implements Closeable {

  private String endpoint;
  private String accessKey;
  private String secretKey;
  private String bucketName;
  private String signingRegion;
  private int connectTimeout;
  private int connectRetrytimes;
  private int connectRetryDelay;
  private int readTimeout;
  private int downloadTimeout;
  private int downloadRetryTimes;
  private int downloadRetryInterval;
  private int requestTimeOut;
  static Logger log = LoggerFactory.getLogger(S3Client.class);
  public AmazonS3 s3Client;
  private TransferManager transferManager;
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

  public S3Client() {
    this("s3");
  }

  public S3Client(String propertyFile) {
    this.endpoint = parameterTool.get(propertyFile + ".s3.endpoint");
    log.info("S3 endpoint:{}", endpoint);
    this.accessKey = parameterTool.get(propertyFile + ".s3.accessKey");
    log.info("S3 accessKey:{}", accessKey);
    this.secretKey = parameterTool.get(propertyFile + ".s3.secretKey");
    log.info("S3 secretKey:{}", secretKey);
    this.bucketName = parameterTool.get(propertyFile + ".s3.bucketName");
    log.info("S3 bucketName:{}", bucketName);
    this.signingRegion = parameterTool.get(propertyFile + ".s3.signingRegion");
    log.info("S3 signingRegion:{}", signingRegion);
    this.connectTimeout = parameterTool.getInt(propertyFile + ".s3.connect.timeout");
    log.info("s3 connectTimeout:{}", connectTimeout);
    this.connectRetrytimes = parameterTool.getInt(propertyFile + ".s3.connect.retry.times");
    log.info("s3 connectRetrytimes:{}", connectRetrytimes);
    this.connectRetryDelay = parameterTool
        .getInt(propertyFile + ".s3.connect.retry.interval.delay");
    log.info("s3 connectRetryDelay:{}", connectRetryDelay);
    this.readTimeout = parameterTool.getInt(propertyFile + ".s3.read.timeout");
    log.info("s3 readTimeout:{}", readTimeout);
    this.downloadTimeout = parameterTool.getInt(propertyFile + ".s3.download.timeout");
    log.info("s3 downloadTimeout:{}", downloadTimeout);
    this.downloadRetryTimes = parameterTool.getInt(propertyFile + ".s3.download.retry.times");
    log.info("s3 downloadRetryTimes:{}", downloadRetryTimes);
    this.downloadRetryInterval = parameterTool
        .getInt(propertyFile + ".s3.download.retry.interval.delay");
    log.info("s3 downloadRetryInterval:{}", downloadRetryInterval);
    this.requestTimeOut = parameterTool.getInt(propertyFile + ".s3.request.timeout");
    log.info("s3 requestTimeOut:{}", requestTimeOut);
    reinit();
  }

  private void reinit(int retryTimes) {
    log.info("reconnect:{}", retryTimes);
    try {
      if (s3Client != null) {
        s3Client.shutdown();
      }
      BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
      ClientConfiguration conf = new ClientConfiguration();
      conf.setConnectionTimeout(connectTimeout);
      conf.setMaxErrorRetry(3);
      conf.setSocketTimeout(requestTimeOut);

      s3Client = AmazonS3ClientBuilder.standard()
          .withEndpointConfiguration(new EndpointConfiguration(endpoint, signingRegion))
          .withClientConfiguration(conf)
          .withCredentials(new AWSStaticCredentialsProvider(credentials)).build();
    } catch (Exception e) {
      log.warn("reinit s3 client...");
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

  public TransferManager getTransferManager(S3Client client) {
    TransferManagerBuilder transferBuilder = TransferManagerBuilder.standard();
    if (client.s3Client == null) {
      reinit();
    }
    transferBuilder.setS3Client(client.s3Client);
    transferManager = transferBuilder.build();
    return transferManager;
  }

  public void downloadFile(String bucketName, String key, String localPath, int timeout,
      int retryTimes, int intervalDelay) throws IOException {
    log.info("start download {} {}", bucketName, key);
    try {
      Download download = getTransferManager(this)
          .download(bucketName, key, new File(localPath), timeout);
      download.waitForCompletion();
      log.info("download to local path {} success", localPath);

    } catch (AmazonClientException e) {
      if (retryTimes < 1 || e.getCause() instanceof FileNotFoundException) {
        throw e;
      }
      try {
        log.info("retry:{} sleep..", retryTimes);
        Thread.sleep(intervalDelay);
        log.info("reinit s3 client ...");
        reinit();
      } catch (Exception e1) {
        e1.printStackTrace();
      }
      downloadFile(bucketName, key, localPath, timeout, retryTimes - 1, intervalDelay);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void downloadFile(String bucketName, String key, String localPath) throws IOException {
    downloadFile(bucketName, key, localPath, downloadTimeout, downloadRetryTimes,
        downloadRetryInterval);
  }

  public void downloadFile(String key, String localPath) throws IOException {
    downloadFile(this.bucketName, key, localPath, downloadTimeout, downloadRetryTimes,
        downloadRetryInterval);
  }

  public String readHbaseTableFile(String key) {
    return readHbaseTableFile(this.bucketName, key);
  }

  public String readHbaseTableFile(String bucketName, String key) {
    S3ObjectInputStream input = null;
    BufferedReader reader = null;
    String res = "";
    try {
      input = this.s3Client.getObject(bucketName, key).getObjectContent();
      reader = new BufferedReader(new InputStreamReader(input));
      res = reader.readLine();
    } catch (Exception e) {
      log.warn("read hbase table name failed: {}" + e.getMessage());
    } finally {
      IOUtils.closeQuietly(input);
      IOUtils.closeQuietly(reader);
    }
    return res;
  }

  public Boolean updateFileByKey(String key, String body) {
    return updateFile(this.bucketName, key, body);
  }

  public Boolean updateFile(String bucketName, String key, String body) {
    ByteArrayInputStream inputStream = new ByteArrayInputStream(
        body.getBytes(Charset.forName("UTF-8")));
    try {
      Upload upload = getTransferManager(this)
          .upload(bucketName, key, inputStream, new ObjectMetadata());
      upload.waitForCompletion();
      return Objects.equals(upload.getState().name(), TransferState.Completed.name());
    } catch (InterruptedException | AmazonClientException e) {
      log.warn("update file failed: {}", e.getMessage());
    } finally {
      IOUtils.closeQuietly(inputStream);
    }
    return false;
  }

  public S3Object getS3ObjectByKey(String key) {
    return this.s3Client.getObject(this.bucketName, key);
  }

  @Override
  public void close() throws IOException {
    if (transferManager != null) {
      transferManager.shutdownNow(true);
    } else {
      s3Client.shutdown();
    }
  }

  public static void main(String[] args) throws IOException {
    S3Client s3Client = new S3Client();
    /*
    解析s3 uri
    String url = "https://zhp.s3-xian.hypers.cc/hpm/test/data2";
    if (url.startsWith("https")) {
      url = url.substring("https://".length(), url.length());
    }
    String[] split = url.split("\\.");
    String s = "/" + url.split("/", 2)[1];
    System.out.println("bucketName: " + split[0] + "  key: " + s);*/
    /*String tableName = s3Client
        .readHbaseTableFile("/zhp", "hpm/test/aaa.txt");
    System.out.println(tableName);*/
    //read tagInfo
    S3ObjectInputStream content = s3Client.s3Client.getObject("/hfpcdntest", "config/batch/tags-20190701.csv")
        .getObjectContent();
    CSVFileReader reader = new CSVFileReader(content);
    List<String[]> s = reader.readAll();
    for (String[] strings : s) {
      int i = 0;
      for (String string : strings) {
        System.out.print(string + " ");
        i++;
      }
      System.out.println(i);
    }
    IOUtils.closeQuietly(reader);
    //read DataBankTagDTO
    /*S3ObjectInputStream input = s3Client.s3Client
        .getObject("/hfpcdntest", "config/update-tags-20190627.txt").getObjectContent();
    BufferedReader reader = new BufferedReader(new InputStreamReader(input));
    List<DataBankTagDTO> records = new ArrayList<>();
    String line = reader.readLine();
    while (line != null) {
      if (StringUtils.isNotEmpty(line)) {
        records.add(JsonUtils.readValue(line, DataBankTagDTO.class));
      }
      line = reader.readLine();
    }
    for (DataBankTagDTO record : records) {
      System.out.println(record);
    }*/
    IOUtils.closeQuietly(s3Client);
  }


}
