package Engine.Operators.Scan.HDFSFileScan;

import Engine.Common.AmberTuple.Tuple;
import Engine.Common.TableMetadata;
import Engine.Common.TupleProducer;
import Engine.Operators.Scan.BufferedBlockReader;
import com.google.common.base.Splitter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.net.URI;

public class HDFSFileScanTupleProducer implements TupleProducer{

    private String host;
    private String hdfsPath;
    private int[] indicesToKeep;
    private char separator;
    private TableMetadata metadata;
    private BufferedBlockReader reader = null;
    private long startOffset;
    private long endOffset;

    HDFSFileScanTupleProducer(String host, String hdfsPath, long startOffset, long endOffset, char delimiter, int[] indicesToKeep, TableMetadata metadata){
        this.host = host;
        this.hdfsPath = hdfsPath;
        this.separator = delimiter;
        this.indicesToKeep = indicesToKeep;
        this.metadata = metadata;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    @Override
    public void initialize() throws Exception {
        FileSystem fs = FileSystem.get(new URI(host),new Configuration());
        FSDataInputStream stream = fs.open(new Path(hdfsPath));
        stream.seek(startOffset);
        reader = new BufferedBlockReader(new BufferedInputStream(stream),endOffset-startOffset,separator);
        if(startOffset > 0)
            reader.readLine();
    }

    @Override
    public boolean hasNext() {
        return reader.hasNext();
    }

    @Override
    public Tuple next() throws Exception {
        if(metadata != null) {
            if (indicesToKeep != null) {
                return Tuple.fromJavaStringArray(reader.readLine(),indicesToKeep, metadata.tupleMetadata().fieldTypes());
            } else {
                return Tuple.fromJavaStringArray(reader.readLine(), metadata.tupleMetadata().fieldTypes());
            }
        }else{
            if (indicesToKeep != null) {
                return Tuple.fromJavaStringArray(reader.readLine(),indicesToKeep);
            } else {
                return Tuple.fromJavaArray(reader.readLine());
            }
        }
    }

    @Override
    public void dispose() throws Exception {
        reader.close();
    }
}
