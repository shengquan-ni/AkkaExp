package Engine.FaultTolerance.Scanner;

import Engine.Common.AmberTuple.Tuple;
import Engine.Common.TableMetadata;
import Engine.Common.TupleProducer;
import Engine.Operators.Scan.BufferedBlockReader;
import com.google.common.base.Splitter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

public class HDFSFolderScanTupleProducer implements TupleProducer{

    private String host;
    private String hdfsPath;
    private char separator;
    private TableMetadata metadata;
    private BufferedBlockReader reader = null;
    private RemoteIterator<LocatedFileStatus> files = null;
    //private Iterator<String> files;
    private FileSystem fs = null;

    public HDFSFolderScanTupleProducer(String host, String hdfsPath, char delimiter, TableMetadata metadata){
        this.host = host;
        this.hdfsPath = hdfsPath;
        this.separator = delimiter;
        this.metadata = metadata;
    }

    private void ReadNextFileIfExists() throws IOException {
        if(files.hasNext()) {
            Path current = files.next().getPath();
            long endOffset = fs.getFileStatus(current).getLen();
            InputStream stream = fs.open(current);
//            String fileName = "D:\\"+hdfsPath.substring(0, hdfsPath.indexOf('/'))+"\\0\\"+files.next();
//            long endOffset = new File(fileName).length();
//            InputStream stream = new FileInputStream(fileName);
            reader = new BufferedBlockReader(stream,endOffset,separator,null);
        }
    }

    @Override
    public void initialize() throws Exception {
        fs = FileSystem.get(new URI(host),new Configuration());
//        files = fs.listFiles(new Path(hdfsPath),true);
//        files = Arrays.asList(Objects.requireNonNull(new File("D:\\"+hdfsPath.substring(0, hdfsPath.indexOf('/'))+"\\0").list())).iterator();
//        ReadNextFileIfExists();
    }

    @Override
    public boolean hasNext() throws IOException {
        if(files == null){
            System.out.println("open file system = "+"/amber-akka-tmp/"+hdfsPath);
            try{
                files = fs.listFiles(new Path("/amber-akka-tmp/"+hdfsPath),true);
            }catch(Exception e){
                e.printStackTrace();
            }
            //files = Arrays.asList(Objects.requireNonNull(new File("D:\\"+hdfsPath.substring(0, hdfsPath.indexOf('/'))+"\\0").list())).iterator();
            System.out.println("get file");
            ReadNextFileIfExists();
            System.out.println("got file");
        }
        if(reader == null){
            ReadNextFileIfExists();
        }
        return reader != null && reader.hasNext();
    }

    @Override
    public Tuple next() throws Exception {
        String[] res = reader.readLine();
        if(res == null){
            return null;
        }
        if(metadata != null) {
            return Tuple.fromJavaStringArray(res, metadata.tupleMetadata().fieldTypes());
        }else{
            return Tuple.fromJavaArray(res);
        }
    }

    @Override
    public void dispose() throws Exception {
        reader.close();
    }
}
