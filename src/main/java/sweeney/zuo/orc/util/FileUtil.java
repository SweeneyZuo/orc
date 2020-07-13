package sweeney.zuo.orc.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Properties;
import java.util.StringJoiner;

/**
 * @author SweeneyZuo
 * @version 1.0
 * @date 2020/7/6 19:39
 */
public class FileUtil {
    private static Configuration hdfsConf;

    private static Log log = LogFactory.getLog(FileUtil.class);

    static {
        hdfsConf = getHdfsConfiguration();
    }

    public static FileSystem getHdfsFileSystem() {
        try {
            return FileSystem.get(hdfsConf);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("get HdfsFileSystem fail.");
        }
    }

    public static FileSystem getLocalFileSystem() {
        try {
            return FileSystem.get(new Configuration());
        } catch (Exception e) {
            e.printStackTrace();
            log.error("get LocalFileSystem fail !", e);
            throw new RuntimeException("get LocalFileSystem fail.");
        }
    }

    public static Configuration getHdfsConfiguration() {
        Configuration conf = new Configuration();
        String hadoopHome = System.getenv("HADOOP_HOME");
        String hadoopUserName = System.getenv("HADOOP_USER_NAME");
        if (hadoopUserName == null) {
            System.setProperty("HADOOP_USER_NAME", "hadoop");
        }
//        System.out.println("HADOOP_HOME:" + hadoopHome);
//        System.out.println("HADOOP_USER_NAME:" + System.getProperty("HADOOP_USER_NAME"));
        if (hadoopHome == null) {
            hadoopHome = "/usr/local/pe/hadoop";
        }
        try {
            conf.addResource(new FileInputStream(hadoopHome + "/etc/hadoop/core-site.xml"));
            conf.addResource(new FileInputStream(hadoopHome + "/etc/hadoop/hdfs-site.xml"));
            conf.addResource(new FileInputStream(hadoopHome + "/etc/hadoop/yarn-site.xml"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            log.error("", e);
        }
        return conf;
    }


    public static Properties loadResouce() {
        Properties p = new Properties();
        File[] properties = new File(System.getProperty("proc.home")).listFiles((File dir, String name) -> name.endsWith(".properties"));
        for (File property : properties) {
            try {
                p.load(new BufferedReader(new InputStreamReader(new FileInputStream(property), StandardCharsets.UTF_8)));
            } catch (IOException e) {
                e.printStackTrace();
                log.error("deal path fail!", e);
            }
        }
        return p;
    }

    public static void main(String[] args) {
        StringJoiner sj = new StringJoiner("\n");
        sj.add("fdalk");
        sj.add("dfkjaljfsl");
        System.out.println(sj.toString());
        System.out.println(sj.length());
        File file = new File(".");
        System.out.println(Arrays.toString(file.list()));
    }
}
