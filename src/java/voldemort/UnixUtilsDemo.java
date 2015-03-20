package voldemort;
import org.apache.log4j.Logger;
import voldemort.utils.UnixUtils;
import java.net.*;
import java.io.*;

public class UnixUtilsDemo {

    private static final Logger logger = Logger.getLogger(UnixUtilsDemo.class);

    private static void demoCurrentTimeNanos() {
	logger.info("About to sleep");
	long before = UnixUtils.currentTimeNanos();
	try {
	    Thread.sleep(1000);
	} catch (InterruptedException ex) {
	    logger.info("Interrupted!!");
	}
	long after = UnixUtils.currentTimeNanos();
	logger.info("Slept for " + (after - before) + " nanoseconds");
    }

    private static final int PORT = 6666;

    private static class ServerThread implements Runnable {
	public ServerSocket rendezvous;
	public Socket server;

	public void run() {
	    logger.info("Start of server thread");
	    try {
		rendezvous = new ServerSocket(PORT);
		server = rendezvous.accept();
	    } catch (Exception e) {
		logger.error("Failed creating server socket", e);
	    }
	    logger.info("End of server thread");
	}
    }

    private static void demoSocketQueues() {
	logger.info("About to create socket pair");
	Socket server;
	Socket client;
	try {
	    // Ugly hack to create a connected socket
	    // pair, using a 2nd thread to avoid having
	    // to do non-blocking IO.
	    ServerThread serverThread = new ServerThread();
	    Thread t = new Thread(serverThread);
	    t.start();
	    Thread.sleep(200);	// hack: avoid race condition
	    client = new Socket("localhost", PORT);
	    t.join(10000);
	    server = serverThread.server;
	} catch (Exception e) {
	    logger.error("Failed creating connected socket pair", e);
	    return;
	}
	logger.info("Have socket pair");

	try {
	    int serverInput = UnixUtils.getFileDescriptor(server.getInputStream());
	    int serverOutput = UnixUtils.getFileDescriptor(server.getOutputStream());
	    int clientInput = UnixUtils.getFileDescriptor(client.getInputStream());
	    int clientOutput = UnixUtils.getFileDescriptor(client.getOutputStream());

	    logger.info("Server input fd: " + serverInput);
	    logger.info("Server output fd: " + serverOutput);
	    logger.info("Client input fd: " + clientInput);
	    logger.info("Client output fd: " + clientOutput);

	    logger.info("Server input queue length: " + UnixUtils.socketGetInputQueueLength(serverInput));
	    logger.info("Server output queue length: " + UnixUtils.socketGetOutputQueueLength(serverOutput));
	    logger.info("Client input queue length: " + UnixUtils.socketGetInputQueueLength(clientInput));
	    logger.info("Client output queue length: " + UnixUtils.socketGetOutputQueueLength(clientOutput));

	    logger.info("Writing 12 bytes to client output");
	    PrintStream ps = new PrintStream(client.getOutputStream());
	    ps.print("Hello World\n");

	    logger.info("Server input queue length: " + UnixUtils.socketGetInputQueueLength(serverInput));
	    logger.info("Server output queue length: " + UnixUtils.socketGetOutputQueueLength(serverOutput));
	    logger.info("Client input queue length: " + UnixUtils.socketGetInputQueueLength(clientInput));
	    logger.info("Client output queue length: " + UnixUtils.socketGetOutputQueueLength(clientOutput));
	} catch (Exception e) {
	    logger.error("Failed getting queue lengths", e);
	}
    }

    public static void main(String args[]) {
	demoCurrentTimeNanos();
	demoSocketQueues();

    }
}
