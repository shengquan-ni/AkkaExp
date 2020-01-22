package Engine.Operators.Scan.HDFSFileScan;

import Engine.Common.AmberTuple.Tuple;
import Engine.Common.TableMetadata;
import Engine.Common.TupleProducer;
import Engine.Operators.Scan.BufferedBlockReader;
import com.google.common.base.Splitter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.InputStream;
import java.net.URI;

public class HDFSFileScanTupleProducer implements TupleProducer{

    private String host;
    private String hdfsPath;
    private int[] indicesToKeep;
    private String separator;
    private TableMetadata metadata;
    private BufferedBlockReader reader = null;
    private Splitter splitter = null;
    private long startOffset;
    private long endOffset;

    HDFSFileScanTupleProducer(String host, String hdfsPath, long startOffset, long endOffset, String delimiter, int[] indicesToKeep, TableMetadata metadata){
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
        System.out.println("??????????");
        FileSystem fs = FileSystem.get(new URI(host),new Configuration());
        InputStream stream = fs.open(new Path(hdfsPath));
        splitter = Splitter.on(separator);
        reader = new BufferedBlockReader(stream,endOffset-startOffset);
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
                return Tuple.fromJavaStringIterable(splitter.split(reader.readLine()),indicesToKeep, metadata.tupleMetadata().fieldTypes());
            } else {
                return Tuple.fromJavaStringIterable(splitter.split(reader.readLine()), metadata.tupleMetadata().fieldTypes());
            }
        }else{
            if (indicesToKeep != null) {
                return Tuple.fromJavaStringIterable(splitter.split(reader.readLine()),indicesToKeep);
            } else {
                return Tuple.fromJavaStringIterable(splitter.split(reader.readLine()));
            }
        }
    }

    @Override
    public void dispose() throws Exception {
        reader.close();
    }
}
