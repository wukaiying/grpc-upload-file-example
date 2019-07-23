package cn.edu.sjtu.ops.grpcdemo;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileCreation {

    public static void main(String[] args) throws IOException {
        createFixLengthFile(new File("src/main/resources/file-10M"), 100*1024*1024);
    }

    public static void createFixLengthFile(File file, long length) throws IOException {
        long start = System.currentTimeMillis();
        FileOutputStream fos = null;
        FileChannel output = null;
        try {
            fos = new FileOutputStream(file);
            output = fos.getChannel();
            output.write(ByteBuffer.allocate(1), length-1);
        } finally {
            try {
                if (output != null) {
                    output.close();
                }
                if (fos != null) {
                    fos.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("total times "+(end-start));
    }
}
