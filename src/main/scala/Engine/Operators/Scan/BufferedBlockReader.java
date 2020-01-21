package Engine.Operators.Scan;

import scala.Array;
import scala.reflect.internal.Chars;

import java.io.IOException;
import java.io.InputStream;

public class BufferedBlockReader {
    private InputStream input;
    private long blockSize;
    private long currentPos;
    private int cursor;
    private int bufferSize;
    private byte[] buffer = new byte[4096]; //4k buffer
    private int tempLength = 0;
    private byte[] temp = new byte[1024];

    public BufferedBlockReader(InputStream input, long blockSize){
        this.input = input;
        this.blockSize = blockSize;
    }

    public CharSequence readLine() throws IOException {
        tempLength = 0;
        while(true) {
            if (cursor >= bufferSize) {
                fillBuffer();
                if (bufferSize == -1) {
                    return new String(temp, 0, tempLength).trim();
                }
            }
            int start = cursor;
            while (cursor < bufferSize) {
                if (buffer[cursor] == '\n') {
                    keepBuffer(start, cursor - start);
                    currentPos += cursor - start + 1;
                    cursor++;
                    return new String(temp, 0, tempLength).trim();
                }
                cursor++;
            }
            keepBuffer(start, bufferSize - start);
            currentPos += bufferSize - start;
        }
    }

    private void fillBuffer() throws IOException {
        bufferSize = input.read(buffer);
        cursor = 0;
    }

    private void keepBuffer(int start, int length){
        if(tempLength+length>temp.length){
            byte[] biggerTemp = new byte[temp.length*2];
            Array.copy(temp,0,biggerTemp,0,tempLength);
        }
        Array.copy(buffer,start,temp,tempLength,length);
        tempLength+=length;
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
