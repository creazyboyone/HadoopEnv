package com.feng;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 判断输入的按分隔符的字符串是否包含另外的一个字符串
 *
 * @author fqy
 * @since 2020/12/7
 */
@Description(name = "inSplit",
        value = "split content and judge if exits",
        extended = "inSplit('computer|writer', computer) = True")

public class InSplit extends UDF {
    public Boolean evaluate(String content, String where) {
        if (null == content) {
            return false;
        }
        if (!content.contains("|")) {
            return content.equals(where);
        }
        for (String s : content.split("\\|")) {
            if (s.equals(where)) {
                return true;
            }
        }
        return false;
    }

    public static void main(String[] args) {
        InSplit split = new InSplit();
        System.out.println(split.evaluate("广州|深圳", "广州"));
    }
}
