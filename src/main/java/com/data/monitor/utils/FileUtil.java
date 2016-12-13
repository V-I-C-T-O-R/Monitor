package com.data.monitor.utils;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Victor on 16-12-2.
 */
public class FileUtil {
    private static final Pattern regex = Pattern.compile("\"@extend:.*?\"");
    private static final Logger logger = Logger.getLogger(FileUtil.class);

    public static String readJsonFile(String path) throws IOException {
        File configFile = new File(path);
        if (!configFile.isFile() && !configFile.canRead()) {
            throw new RuntimeException(String.format("json file[%s] invalid!", path));
        }
        String dir = configFile.getParent();
        String content = IOUtils.toString(new FileInputStream(configFile));

        Matcher matcher = regex.matcher(content);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String reg = matcher.group();
            String replaceFile = StringUtils.removeEnd(StringUtils.removeStart(reg, "\"@extend:"), "\"").trim();
            if (!replaceFile.startsWith("/")) {
                replaceFile = dir + "/" + replaceFile;
            }
            String replaceJson = readJsonFile(replaceFile);
            matcher.appendReplacement(sb, replaceJson);
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    public static void writeTXTtoFile(String content, String filePath) throws Exception {
        if (content == null || content.length() == 0) {
            throw new Exception("写入的内容为空");
        }
        if (filePath == null || filePath.length() == 0) {
            throw new Exception("文件路径为空");
        }

        File file = new File(filePath);
        try {
            if(file.exists()){
                file.delete();
            }
            file.createNewFile();
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(file), "utf-8"));
            writer.write(content);
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean deleteFile(String fileName) {
        File file = new File(fileName);
        if (!file.exists()) {
            logger.info("删除文件失败：" + fileName + "文件不存在");
            return false;
        } else {
            file.delete();
            logger.info("删除文件成功：" + fileName);
            return true;
        }
    }
}
