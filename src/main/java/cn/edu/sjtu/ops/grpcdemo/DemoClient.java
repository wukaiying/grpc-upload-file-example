package cn.edu.sjtu.ops.grpcdemo;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import cn.edu.sjtu.ops.grpcdemo.DemoServiceGrpc.*;
import io.grpc.stub.StreamObserver;

import java.io.*;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import static cn.edu.sjtu.ops.grpcdemo.DemoServiceGrpc.newBlockingStub;
import static cn.edu.sjtu.ops.grpcdemo.DemoServiceGrpc.newStub;

public class DemoClient {

    private final ManagedChannel channel;
    private final DemoServiceBlockingStub blockingStub;
    private final DemoServiceStub asyncStub;
    private static Logger logger;

    public DemoClient(String host, int port) throws IOException {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
    }

    public DemoClient(ManagedChannelBuilder<?> channelBuilder) throws IOException {
        logger = Logger.getLogger("logger.info");
        logger.setLevel(Level.INFO);

        File logFile = new File("src/main/log/client.log");
        FileHandler fileHandler = new FileHandler(logFile.getAbsolutePath(), 10240, 1, true);
        fileHandler.setLevel(Level.INFO);
        fileHandler.setFormatter(new DemoServer.MyLogFormatter());
        logger.addHandler(fileHandler);

        channel = channelBuilder.build();
        blockingStub = newBlockingStub(channel);
        asyncStub = newStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void uploadFile(String filename, final int chunkSize) throws InterruptedException {
        final CountDownLatch finishLatch = new CountDownLatch(1);
        StreamObserver<UploadStatus> responseObserver = new StreamObserver<UploadStatus>() {
            public void onNext(UploadStatus uploadStatus) {
                System.out.println("uploadFile: status: "+ String.valueOf(uploadStatus.getCode().getNumber()));
            }

            public void onError(Throwable throwable) {
                System.out.println("uploadFile Error!");
            }

            public void onCompleted() {
                logger.info("finish upload (chunk size: " + String.valueOf(chunkSize) + ")");
                System.out.println("uploadFile Completed!");
                finishLatch.countDown();
            }
        };

        logger.info("start upload (chunk size: " + String.valueOf(chunkSize) + ")");
        StreamObserver<Chunk> requestObserver = asyncStub.upload(responseObserver);
        try {
            FileInputStream is = new FileInputStream(new File("src/main/resources/" + filename));
            ByteArrayOutputStream bos = new ByteArrayOutputStream(chunkSize);
            byte[] b = new byte[chunkSize];
            int n;
            while ((n = is.read(b)) != -1) {
                if (n == chunkSize) {
                    requestObserver.onNext(Chunk.newBuilder().setContent(ByteString.copyFrom(b)).build());
                } else {
                    bos.write(b, 0, n);
                    requestObserver.onNext(Chunk.newBuilder().setContent(ByteString.copyFrom(bos.toByteArray())).build());
                }
                if (finishLatch.getCount() == 0) {
                    // RPC completed or errored before we finished sending.
                    // Sending further requests won't error, but they will just be thrown away.
                    return;
                }
            }
            is.close();
            bos.close();
        } catch (RuntimeException e) {
            // Cancel RPC
            requestObserver.onError(e);
            throw e;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Mark the end of requests
        requestObserver.onCompleted();

        // Receiving happens asynchronously
        if (!finishLatch.await(5, TimeUnit.MINUTES)) {
            System.out.println("operation can not finish within 5 minutes");
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        String hostname = "127.0.0.1";
        int chunkSize = Integer.parseInt("100");
        DemoClient client = new DemoClient(hostname, 8980);
//        client.uploadFile("testfile.txt", 10);
        Date start = new Date();
        client.uploadFile("testfile.mp4", chunkSize * 1024);
        Date end = new Date();
        float diff = end.getTime() - start.getTime();
        System.out.println(String.valueOf(diff / 1000) + "s");
        client.shutdown();
    }
}


/*
64KB : 8.617s
128KB: 4.523s
256KB: 2.778s
512KB: 2.041s
1MB  : 1.609s
2MB  : 1.344s
3MB  : 1.536s
4000KB: 1.207s
4MB  : RESOURCE_EXHAUSTED: gRPC message exceeds maximum size 4194304: 4194309
 */