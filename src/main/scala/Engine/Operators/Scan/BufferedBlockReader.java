package Engine.Operators.Scan;


import com.google.common.primitives.Ints;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class BufferedBlockReader {
    private InputStream input;
    private long blockSize;
    private long currentPos;
    private int cursor;
    private int bufferSize;
    private byte[] buffer = new byte[4096]; //4k buffer
    private ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    private List<String> fields = new ArrayList<>();
    private HashSet<Integer> keptFields = null;
    private char delimiter;

    public BufferedBlockReader(InputStream input, long blockSize, char delimiter, int[] kept){
        this.input = input;
        this.blockSize = blockSize;
        this.delimiter = delimiter;
        if(kept != null){
            this.keptFields = new HashSet<>(Ints.asList(kept));
        }
    }

    public String[] readLine() throws IOException {
        outputStream.reset();
        fields.clear();
        int index = 0;
        while(true) {
            if (cursor >= bufferSize) {
                fillBuffer();
                if (bufferSize == -1) {
                    return fields.isEmpty()? null: fields.toArray(new String[0]);
                }
            }
            int start = cursor;
            while (cursor < bufferSize) {
                if (buffer[cursor] == delimiter) {
                    if(keptFields == null || keptFields.contains(index)){
                        outputStream.write(buffer,start,cursor-start);
                        fields.add(outputStream.toString());
                    }
                    outputStream.reset();
                    currentPos += cursor - start + 1;
                    start = cursor+1;
                    index++;
                }else if(buffer[cursor] == '\n'){
                    if(keptFields == null || keptFields.contains(index)){
                        outputStream.write(buffer,start,cursor-start);
                        fields.add(outputStream.toString());
                    }
                    currentPos += cursor - start + 1;
                    cursor++;
                    return fields.toArray(new String[0]);
                }
                cursor++;
            }
            outputStream.write(buffer, start, bufferSize - start);
            currentPos += bufferSize - start;
        }
    }

    private void fillBuffer() throws IOException {
        bufferSize = input.read(buffer);
        cursor = 0;
    }

    public boolean hasNext(){
        try {
            return currentPos < blockSize || (currentPos == blockSize && (bufferSize>cursor || input.available() > 0));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public void close() throws IOException {
        input.close();
    }
}
