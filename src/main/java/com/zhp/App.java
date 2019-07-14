package com.zhp;

/**
 * Hello world!
 */
public class App {

  public static void main(String[] args) {
    System.out.println("Hello World!");

    boolean test = test("yyxbtsrs", "yyyyxbbtssrs");
    System.out.println(test);
  }

  /**
   *"yyxbtsrs"
    "yyyyxbbtssrs"
   */

  public static boolean test(String a, String b) {
    if (a == null || b == null) {
      return false;
    }
    char[] chars = a.toCharArray();
    char[] chars2 = b.toCharArray();
    if (chars.length > chars2.length) {
      return false;
    }
    int i = 0;
    int j = i;
    int len = chars.length;
    boolean res = true;
    while (i < len) {
      if (j == chars2.length) {
        if (i < len) {
          res = false;
        }
        break;
      }
      if (chars[i] == chars2[j]) {
        i++;
        j++;
      } else {
        if (chars[i -1] == chars2[j]){
          j++;
        }else {
          return false;
        }
        if (j == chars2.length || chars[i] != chars2[j]) {
          return false;
        }
      }
    }
    return res;
  }
}
