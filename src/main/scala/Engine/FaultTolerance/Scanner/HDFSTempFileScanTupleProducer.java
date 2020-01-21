package Engine.FaultTolerance.Scanner;

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

public class HDFSTempFileScanTupleProducer implements TupleProducer{

    private String host;
    private String hdfsPath;
    private String separator;
    private TableMetadata metadata;
    private BufferedBlockReader reader = null;
    private Splitter splitter = null;

    public HDFSTempFileScanTupleProducer(String host, String hdfsPath, String delimiter, TableMetadata metadata){
        this.host = host;
        this.hdfsPath = hdfsPath;
        this.separator = delimiter;
        this.metadata = metadata;
    }

    @Override
    public void initialize() throws Exception {
        FileSystem fs = FileSystem.get(new URI(host),new Configuration());
        long endOffset =fs.getFileStatus(new Path(hdfsPath)).getLen();
        InputStream stream = fs.open(new Path(hdfsPath));
        splitter = Splitter.on(separator);
        reader = new BufferedBlockReader(stream,endOffset);
    }

    @Override
    public boolean hasNext() {
        return reader.hasNext();
    }

    @Override
    public Tuple next() throws Exception {
        if(metadata != null) {
            return Tuple.fromJavaStringIterable(splitter.split(reader.readLine()), metadata.tupleMetadata().fieldTypes());
        }else{
            return Tuple.fromJavaStringIterable(splitter.split(reader.readLine()));
        }
    }

    @Override
    public void dispose() throws Exception {
        reader.close();
    }
}
