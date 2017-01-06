package com.sunteng.monitor.utils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * Created by Victor on 16-12-5.
 */
public class ToolUtils {
    public static Calendar CAL = Calendar.getInstance();
    public static final SimpleDateFormat DF = new SimpleDateFormat("yyyyMMdd");
    public static final SimpleDateFormat MF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /*
     * 生成一个uuid
     */
    public static String getUUID() {
        UUID uuid = UUID.randomUUID();
        String str = uuid.toString();
        // 去掉"-"符号
        String temp = str.substring(0, 8) + str.substring(9, 13) + str.substring(14, 18) + str.substring(19, 23) + str.substring(24);
        return temp;
    }

    public static String getDayBeforeFormat(int theLastDay) {
        CAL.setTime(new Date());
        CAL.add(Calendar.DAY_OF_MONTH, theLastDay);
        return DF.format(CAL.getTime());
    }

    public static List<String> getDaysBeforeFormat(int day) {
        List<String> dts = new ArrayList<String>();
        CAL.setTime(new Date());
        for (int d = day; d > 0; d--) {
            CAL.add(Calendar.DAY_OF_MONTH, -1);
            dts.add(DF.format(CAL.getTime()));
        }
        Collections.sort(dts);
        return dts;
    }
    public static List<String> getDaysAfterFormat(int day) {
        List<String> dts = new ArrayList<String>();
        CAL.setTime(new Date());
        for (int d = day; d > 0; d--) {
            CAL.add(Calendar.DAY_OF_MONTH, 1);
            dts.add(DF.format(CAL.getTime()));
        }
        Collections.sort(dts);
        return dts;
    }

    public static void main(String[] args) throws Exception {
        /*String ll = "20161111";
        System.out.print(DF.parse(ll).getTime());
        String p = "1481040000000";
        System.out.print(MF.format(Long.valueOf(p)));*/
        String s = "";
        System.out.print(Float.parseFloat(s));
    }
}
