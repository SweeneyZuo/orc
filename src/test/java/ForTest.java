import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;
import sweeney.zuo.orc.util.FileUtil;

import java.io.IOException;

/**
 * @author SweeneyZuo
 * @version 1.0
 * @date 2020/7/29 18:56
 */
public class ForTest {

    @Test
    public void testHdfs() throws IOException {
        FileSystem hdfsFileSystem = FileUtil.getHdfsFileSystem();
        RemoteIterator<LocatedFileStatus> iterator = hdfsFileSystem.listFiles(new Path("/user/jqm2dp/zuo"), true);
        while
        (iterator.hasNext()) {
            System.out.println(iterator.next().getPath());
        }
    }

    private void a (String...args){
        for (String arg : args) {
            System.out.println(arg);
        }
    }

    @Test
    public void testString()   {
        System.out.println(String.format("a:%s", 1));
        a(new String[]{"1","2"});
    }
}
