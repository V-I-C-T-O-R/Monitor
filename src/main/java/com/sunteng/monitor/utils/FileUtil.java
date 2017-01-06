package com.sunteng.monitor.utils;

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
        try {
            createFile(filePath);
            File file = new File(filePath);
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(file), "utf-8"));
            writer.write(content);
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean createFile(String destFileName){
        File file = new File(destFileName);
        if(file.exists()) {
            logger.info("创建单个文件" + destFileName + "失败，目标文件已存在！");
            return false;
        }
        if (destFileName.endsWith(File.separator)) {
            logger.info("创建单个文件" + destFileName + "失败，目标文件不能为目录！");
            return false;
        }
        //判断目标文件所在的目录是否存在
        if(!file.getParentFile().exists()) {
            //如果目标文件所在的目录不存在，则创建父目录
            logger.info("目标文件所在目录不存在，准备创建它！");
            if(!file.getParentFile().mkdirs()) {
                logger.info("创建目标文件所在目录失败！");
                return false;
            }
        }
        //创建目标文件
        try {
            if (file.createNewFile()) {
                logger.info("创建单个文件" + destFileName + "成功！");
                return true;
            } else {
                logger.info("创建单个文件" + destFileName + "失败！");
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.info("创建单个文件" + destFileName + "失败！" + e.getMessage());
            return false;
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
