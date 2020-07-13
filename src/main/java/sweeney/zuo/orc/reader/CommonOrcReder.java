package sweeney.zuo.orc.reader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcMapredRecordReader;
import org.apache.orc.mapred.OrcStruct;
import sweeney.zuo.orc.util.FileUtil;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * @author SweeneyZuo
 * @version 1.0
 * @date 2020/7/6 19:38
 */
public class CommonOrcReder implements Iterable<OrcStruct> {
    private final Reader reader;
    private int[] columns;
    private Log log = LogFactory.getLog(getClass());

    public CommonOrcReder(Path path) throws IOException {
        OrcFile.ReaderOptions readerOptions = new OrcFile.ReaderOptions(FileUtil.getHdfsConfiguration());
        this.reader = OrcFile.createReader(path, readerOptions);
    }

    public static void main(String[] args) throws Exception {
        // test
        Configuration conf = new Configuration();
        Reader reader = OrcFile.createReader(new Path(""), OrcFile.readerOptions(conf));
        RecordReader rows = reader.rows();
        TypeDescription schema = reader.getSchema();
        List<TypeDescription> children = schema.getChildren();
        VectorizedRowBatch batch = schema.createRowBatch();
        while (rows.nextBatch(batch)) {
            for (int r = 0; r < batch.size; r++) {
                OrcStruct result = new OrcStruct(schema);
                for (int i = 0; i < batch.numCols; ++i) {
                    result.setFieldValue(i, OrcMapredRecordReader.nextValue(batch.cols[i], r,
                            children.get(i), result.getFieldValue(i)));
                }
                System.out.println(result);
            }
        }
        rows.close();
    }

    public TypeDescription getSchema() {
        return reader.getSchema();
    }

    public void close() {

    }

    public void setColumns(int[] columns) {
        this.columns = columns;
    }

    public long rowCount() {
        return reader.getNumberOfRows();
    }

    @Override
    public Iterator<OrcStruct> iterator() {
        try {
            return new OrcResultIterator(reader.rows());
        } catch (IOException e) {
            e.printStackTrace();
            log.error("", e);
            throw new RuntimeException("");
        }
    }

    public class OrcResultIterator implements Iterator<OrcStruct> {
        private final org.apache.orc.RecordReader recordReader;
        private final VectorizedRowBatch batch = getSchema().createRowBatch();
        private boolean closed;
        private int row;
        private boolean first = true;

        private OrcResultIterator(org.apache.orc.RecordReader reader) {
            this.recordReader = reader;
        }

        @Override
        public boolean hasNext() {
            try {
                if (!first) {
                    if (row < batch.size) {
                        return true;
                    } else {
                        row = 0;
                    }
                }
                first = false;
                return recordReader.nextBatch(batch);
            } catch (IOException e) {
                e.printStackTrace();
                log.error("", e);
                try {
                    this.recordReader.close();
                    closed = true;
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                    log.error("", ioException);
                }
                throw new RuntimeException();
            }
        }

        @Override
        public OrcStruct next() {
            if (closed) {
                throw new RuntimeException("reader has closed!");
            }
            TypeDescription schema = getSchema();
            List<TypeDescription> children = schema.getChildren();
//            if (columns != null) {
//                List<TypeDescription>  t = new ArrayList<>();
//                for (int column : columns) {
//                    t.add(children.get(column));
//                }
//                children = t;
//            }
            int numberOfChildren = children.size();
            OrcStruct result = new OrcStruct(schema);
            if (row < batch.size) {
                for (int i = 0; i < numberOfChildren; ++i) {
                    result.setFieldValue(i, OrcMapredRecordReader.nextValue(batch.cols[i], row,
                            children.get(i), result.getFieldValue(i)));
                }
                row++;
            }
//            System.out.println(result);
            return result;
        }
    }
}
