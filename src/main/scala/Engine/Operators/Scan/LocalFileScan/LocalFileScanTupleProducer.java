package Engine.Operators.Scan.LocalFileScan;

import Engine.Common.AmberTuple.AmberTuple;
import Engine.Common.AmberTuple.Tuple;
import Engine.Common.TableMetadata;
import Engine.Common.TupleProducer;
import Engine.Operators.Scan.BufferedBlockReader;
import com.google.common.base.Splitter;
import org.tukaani.xz.SeekableFileInputStream;

import java.io.IOException;


public class LocalFileScanTupleProducer implements TupleProducer {

    private String localPath;
    private int[] indicesToKeep;
    private char separator;
    private TableMetadata metadata;
    private BufferedBlockReader reader = null;
    private long startOffset;
    private long endOffset;


    private String[] shrinkStringArray(String[] array, int[] indicesToKeep){
        String[] res = new String[indicesToKeep.length];
        for(int i=0;i<indicesToKeep.length;++i)
            res[i] = array[indicesToKeep[i]];
        return res;
    }

    LocalFileScanTupleProducer(String localPath, long startOffset,long endOffset, char delimiter, int[] indicesToKeep, TableMetadata metadata){
        this.localPath = localPath;
        this.separator = delimiter;
        this.indicesToKeep = indicesToKeep;
        this.metadata = metadata;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    @Override
    public void initialize() throws Exception {
        SeekableFileInputStream stream = new SeekableFileInputStream(localPath);
        stream.seek(startOffset);
        reader= new BufferedBlockReader(stream,endOffset-startOffset,separator);
        if(startOffset > 0)
            reader.readLine();
    }

    @Override
    public boolean hasNext() {
        return reader.hasNext();
    }

    @Override
    public Tuple next() throws IOException {
        if(metadata != null) {
            if (indicesToKeep != null) {
                return Tuple.fromJavaStringArray(reader.readLine(),indicesToKeep, metadata.tupleMetadata().fieldTypes());
            } else {
                return Tuple.fromJavaStringArray(reader.readLine(),metadata.tupleMetadata().fieldTypes());
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
    public void dispose() throws IOException {
        reader.close();
    }
}
