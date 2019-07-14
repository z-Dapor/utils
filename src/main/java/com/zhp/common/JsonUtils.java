package com.zhp.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.scala.DefaultScalaModule;
import java.io.File;
import java.io.IOException;

public class JsonUtils {

  private static ThreadLocal<ObjectMapper> om = new ThreadLocal<ObjectMapper>() {
    @Override
    protected ObjectMapper initialValue() {
      ObjectMapper mapper = new ObjectMapper();
      return mapper;
    }
  };

  public static <T> T readValue(String str, Class<T> cla) throws IOException {
    return om.get().readValue(str, cla);
  }

  public static <T> T readValue(byte[] bytes, Class<T> cla) throws IOException {
    return om.get().readValue(bytes, cla);
  }

  public static <T> T readValueIgnoreUnKonwn(String str, Class<T> cla) {
    om.get().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false);
    try {
      return om.get().readValue(str, cla);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public static <T> T readValueFromFile(String fileName, Class<T> cla) throws IOException {
    return om.get().readValue(new File(fileName), cla);
  }

  public static <T> T convertValue(Object fromValue, Class<T> toValueType) {
    return om.get().convertValue(fromValue, toValueType);
  }

  public static byte[] writeValueAsBytes(Object obj) throws JsonProcessingException {
    return om.get().writeValueAsBytes(obj);
  }

  public static String writeValueAsString(Object obj) throws JsonProcessingException {
    return om.get().writeValueAsString(obj);
  }

  public static void writeValueToFile(String fileName, Object obj) throws IOException {
    File file = new File(fileName);
    file.getParentFile().mkdirs();
    om.get().writeValue(file, obj);
  }
  public static <T> T readScalaValue(String jsonStr, Class<T> valueType) {
    om.get().registerModule(new DefaultScalaModule());
    om.get().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    try {
      return om.get().readValue(jsonStr, valueType);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public static ObjectMapper getOM() {
    return om.get();
  }

}
