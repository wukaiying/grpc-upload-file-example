package cn.edu.sjtu.ops.grpcdemo;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.*;
import java.util.UUID;
import java.util.logging.*;

public class DemoServer {

    private final int port;
    private final Server server;
    private static Logger logger;

    private static final int CHUNKSIZE = 1;

    public DemoServer(int port) throws IOException {
        logger = Logger.getLogger("logger.info");
        logger.setLevel(Level.INFO);

        File logFile = new File("src/main/log/server.log");
        FileHandler fileHandler = new FileHandler(logFile.getAbsolutePath(), 10240, 1, true);
        fileHandler.setLevel(Level.INFO);
        fileHandler.setFormatter(new MyLogFormatter());
        logger.addHandler(fileHandler);

        this.port = port;
        ServerBuilder sb = ServerBuilder.forPort(port);
        this.server = sb.addService(new DemoService()).build();
    }

    /**
     * Start serving requests.
     */
    public void start() throws IOException {
        logger.info("************ START *************");

        server.start();
        System.out.println("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may has been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                DemoServer.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    /**
     * Stop serving requests and shutdown resources.
     */
    public void stop() {
        if (server != null) {
            server.shutdown();
        }
        logger.info("************ FINISH ************");
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Main method.  This comment makes the linter happy.
     */
    public static void main(String[] args) throws Exception {


        DemoServer server = new DemoServer(8980);
        server.start();
        server.blockUntilShutdown();

    }

    public static class DemoService extends DemoServiceGrpc.DemoServiceImplBase {
        @Override
        public StreamObserver<Chunk> upload(final StreamObserver<UploadStatus> responseObserver) {
            return new StreamObserver<Chunk>() {
//                ByteArrayOutputStream bos = new ByteArrayOutputStream(CHUNKSIZE);
                String filename = UUID.randomUUID().toString();
                int count = 0;
                public void onNext(Chunk chunk) {
                    count++;
                    logger.info("chunk-" + String.valueOf(count) + " start");
//                    count += 1;
//                    if (count % 1 == 0)
//                        System.out.println(String.valueOf(count));
//                    BufferedInputStream bis = new BufferedInputStream(chunk.getContent().newInput());
//                    byte[] b = new byte[CHUNKSIZE];
//                    int n;
//                    try {
//                        while ((n = bis.read(b, 0, CHUNKSIZE)) != -1) {
//                            bos.write(b, 0, n);
//                        }
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }

                    BufferedOutputStream bos = null;
                    FileOutputStream fos = null;
                    File file = null;
                    try {
                        file = new File("src/main/resources/" + filename);
                        fos = new FileOutputStream(file, true);
                        bos = new BufferedOutputStream(fos);
                        bos.write(chunk.getContent().toByteArray());
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        if (bos != null) {
                            try {
                                bos.close();
                            } catch (IOException e1) {
                                e1.printStackTrace();
                            }
                        }
                        if (fos != null) {
                            try {
                                fos.close();
                            } catch (IOException e1) {
                                e1.printStackTrace();
                            }
                        }
                    }
                    logger.info("chunk-" + String.valueOf(count) + " finish");
                }


                public void onError(Throwable throwable) {
                    System.out.println("error!!!!");
                }

                public void onCompleted() {
//                    FileOutputStream fos = null;
//                    BufferedOutputStream fbos = null;
//                    try {
//                        fos = new FileOutputStream(new File("src/main/resources/received.txt"));
//                        fbos = new BufferedOutputStream(fos);
//                        fbos.write(bos.toByteArray());
//                    } catch (FileNotFoundException e) {
//                        e.printStackTrace();
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    } finally {
//                        if (fbos != null) {
//                            try {
//                                fbos.close();
//                            } catch (IOException e1) {
//                                e1.printStackTrace();
//                            }
//                        }
//                        if (fos != null) {
//                            try {
//                                fos.close();
//                            } catch (IOException e1) {
//                                e1.printStackTrace();
//                            }
//                        }
//                    }
                    System.out.println("complete!!!!!");
                    logger.info("File Transfer Completed\n");
                    responseObserver.onNext(UploadStatus.newBuilder().setCodeValue(1).build());
                    responseObserver.onCompleted();
                }
            };
        }
    }

    static class  MyLogFormatter extends Formatter {
        @Override
        public String format(LogRecord record) {
            return record.getMillis() +  ": " + record.getMessage() + "\n";
        }
    }
}
