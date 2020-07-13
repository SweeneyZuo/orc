package sweeney.zuo.orc.core;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.StringUtils;
import org.apache.orc.mapred.OrcStruct;
import sweeney.zuo.orc.reader.CommonOrcReder;
import sweeney.zuo.orc.util.FileUtil;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAdder;

//import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
//import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
//import org.apache.hadoop.hive.serde2.objectinspector.StructField;
//import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * @author SweeneyZuo
 * @version 1.0
 * @date 2020/7/6 14:06
 */


public class OrcReader {
    private final static int THREAD_SIZE = 2;
    private final static int DEFAULT_OUTPUT_BUFFER_SIZE = 1024 * 1024 * 10;
    private final List<Path> paths;
    private int[] columns;
    private boolean text;
    private boolean info;
    private boolean count;
    private boolean optionColumns;
    private BufferedWriter consoleOut;
    private boolean readWithMultithreading = false;

    private Log log = LogFactory.getLog(getClass());

    //    private CountDownLatch count;
    public OrcReader(String[] args) {
        paths = new ArrayList<>();
        parseArgs(args);
        init();
//        count = new CountDownLatch(paths.size());
    }

    public static void hiveOrc(String[] args) throws Exception {
//        JobConf conf = new JobConf();
//        Path testFilePath = new Path(args[0]);
//        Properties p = new Properties();
//        OrcSerde serde = new OrcSerde();
//        p.setProperty("columns.types", "string:string:string:string");
//        serde.initialize(conf, p);
//        StructObjectInspector inspector = (StructObjectInspector) serde.getObjectInspector();
//        InputFormat<NullWritable, org.apache.hadoop.hive.ql.io.orc.OrcStruct> in = new OrcInputFormat();
//        FileInputFormat.setInputPaths(conf, testFilePath.toString());
//        InputSplit[] splits = in.getSplits(conf, 1);
//        System.out.println("splits.length==" + splits.length);
//
//        conf.set("hive.io.file.readcolumn.ids", "1");
//        org.apache.hadoop.mapred.RecordReader<NullWritable, org.apache.hadoop.hive.ql.io.orc.OrcStruct> reader =
//                in.getRecordReader(splits[0], conf, Reporter.NULL);
//        NullWritable key = reader.createKey();
//        org.apache.hadoop.hive.ql.io.orc.OrcStruct value = reader.createValue();
//        List<? extends StructField> fields = inspector.getAllStructFieldRefs();
//        long offset = reader.getPos();
//        while (reader.next(key, value)) {
//            StringJoiner sj = new StringJoiner("|");
//            for (StructField field : fields) {
//                Object obj = inspector.getStructFieldData(value, field);
//                sj.add(obj.toString());
//                offset = reader.getPos();
//            }
//            System.out.println(sj.toString());
//        }
//        reader.close();
    }

    public static void main(String[] args) throws Exception {
//        args = new String[]{"-text", "file:///C:\\Users\\Administrator\\Desktop\\tmp\\lalala", "--cols" ,"1"};
//        args = new String[]{"-text", "/user/jqm2dp/zuo", "--cols" ,"1"};
        OrcReader orcReader = new OrcReader(args);
        long start = System.currentTimeMillis();
//        System.out.println(orcReader);
        orcReader.start();
//        System.out.println((System.currentTimeMillis() - start )/1000);
//        System.out.println(orcReader.toString());
    }

    private void init() {
        Properties properties = FileUtil.loadResouce();
        int bufferSize = Integer.parseInt(properties.getProperty("console.output.buffer.size", String.valueOf(DEFAULT_OUTPUT_BUFFER_SIZE)));
        consoleOut = new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8),
                bufferSize);
        this.readWithMultithreading = Boolean.getBoolean(properties.getProperty("read.orc.file.multi.thread", "false"));
    }

    private void printOrc(Path path) {
        CommonOrcReder cor = null;
        try {
            cor = new CommonOrcReder(path);
            for (OrcStruct os : cor) {
                int numFields = os.getNumFields();
                StringJoiner sj = new StringJoiner("|");
                if (optionColumns) {
                    for (int column : columns) {
                        sj.add(os.getFieldValue(column).toString());
                    }
                } else {
                    for (int i = 0; i < numFields; i++) {
                        sj.add(os.getFieldValue(i).toString());
                    }
                }
                consoleOut.write(sj.toString());
                consoleOut.newLine();
            }
            consoleOut.flush();
        } catch (Exception e) {
            e.printStackTrace();
            log.error("read orc fail!", e);
        } finally {
            if (cor != null) {
                cor.close();
            }
//            count.countDown();
        }

    }


    private void printRowCount() {
        LongAdder longAdder = new LongAdder();
        CountDownLatch countDownLatch = new CountDownLatch(paths.size());
        for (Path path : paths) {
            new Thread(() -> {
                longAdder.add(rowCount(path));
                countDownLatch.countDown();
            }).start();
        }
        try {
            countDownLatch.await();
            System.out.println(longAdder.longValue());
        } catch (InterruptedException e) {
            e.printStackTrace();
            log.error("count orc file fail!", e);
        }

    }

    public long rowCount(Path path) {
        CommonOrcReder cor = null;
        long num = 0;
        try {
            cor = new CommonOrcReder(path);
            num = cor.rowCount();
        } catch (Exception e) {
            e.printStackTrace();
            log.error("count orc file fail!", e);
        } finally {
            if (cor != null) {
                cor.close();
            }
        }
        return num;
    }

    public void readCommonOrc() {
//        ScheduledExecutorService pool = Executors.newScheduledThreadPool(THREAD_SIZE, (Runnable r) -> {
//            Thread t = new Thread(r);
//            t.setDaemon(true);
//            t.setName("[orc read thread]");
//            return t;
//        });
        try {
            for (Path path : paths) {
                if (readWithMultithreading) {
                    new Thread(() -> printOrc(path)).start();
                } else {
                    printOrc(path);
                }
//                pool.submit(() -> printOrc(path));
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("read orc file fail!", e);
        }
//        finally {
//            try {
//                count.await();
////                System.exit(0);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
////                System.exit(-1);
//            }
//        }
    }

    private void showInfo() {
        try {
            for (Path path : paths) {
                CommonOrcReder cor = null;
                try {
                    System.out.println(path.toString());
                    cor = new CommonOrcReder(path);
                    System.out.println(cor.getSchema());
                } catch (Exception ioe) {
                    ioe.printStackTrace();
                    log.error("show orc file info fail!", ioe);
                } finally {
                    if (cor != null) {
                        cor.close();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("show orc file info fail!", e);
        }
    }


    private FileSystem getFileSystem(Path path) {
        if (path.toString().startsWith("file:")) {
            return FileUtil.getLocalFileSystem();
        }
        return FileUtil.getHdfsFileSystem();
    }

    public void start() {
        if (text) {
            readCommonOrc();
        } else if (info) {
            showInfo();
        } else if (count) {
            printRowCount();
        }
    }

    private void parseArgs(String[] args) {
        for (String arg : args) {
            if (arg.equals("-text")) {
                this.text = true;
                continue;
            } else if (arg.equals("-info")) {
                this.info = true;
                continue;
            } else if (arg.equals("-count")) {
                this.count = true;
                continue;
            } else if (arg.equals("--cols")) {
                this.optionColumns = true;
                continue;
            }
            if ((text && !optionColumns) || info || count) {
                addPath(arg);
                continue;
            }
            if (optionColumns) {
                String[] split = StringUtils.split(arg, ',');
                this.columns = new int[split.length];
                for (int i = 0; i < split.length; i++) {
                    this.columns[i] = Integer.parseInt(split[i]);
                }
                continue;
            }
        }
    }

    private void addPath(String pathStr) {
        FileSystem fileSystem = null;
        try {
            fileSystem = getFileSystem(new Path(pathStr));
            Path path = new Path(pathStr);
            if (fileSystem.isDirectory(path)) {
                RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(path, false);
                while (iterator.hasNext()) {
                    LocatedFileStatus next = iterator.next();
                    if (next.isDirectory() || !next.getPath().toString().endsWith("orc")) {
                        continue;
                    }
                    this.paths.add(next.getPath());
                }
            } else {
                if (path.toString().endsWith("orc")) {
                    this.paths.add(path);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("deal path fail!", e);
        } finally {
            if (fileSystem != null) {
                try {
                    fileSystem.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    log.error("deal path fail!", e);
                }
            }
        }
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", OrcReader.class.getSimpleName() + "[", "]")
                .add("paths=" + paths)
                .add("columns=" + Arrays.toString(columns))
                .add("text=" + text)
                .add("info=" + info)
                .add("count=" + count)
                .add("optionColumns=" + optionColumns)
                .toString();
    }
}