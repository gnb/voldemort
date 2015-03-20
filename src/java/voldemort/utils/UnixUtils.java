package voldemort.utils;

import org.apache.log4j.Logger;

import com.sun.jna.Platform;
import com.sun.jna.LastErrorException;
import com.sun.jna.ptr.IntByReference;
import java.nio.LongBuffer;
import com.sun.jna.Native;
import com.sun.jna.NativeLibrary;
import com.sun.jna.Library;
import java.lang.reflect.Method;
import com.sun.jna.FunctionMapper;
import java.util.HashMap;
import sun.misc.SharedSecrets;
import java.io.*;


/**
 * Provides access to Unix-specific system features.
 *
 * Hardcoded for x86_64 Linux.
 */
public class UnixUtils {

    private static final Logger logger = Logger.getLogger(UnixUtils.class);

    /* ioctls. These values correct for x86_64 Linux */
    private static final int SIOCINQ = 0x541B;
    private static final int SIOCOUTQ = 0x5411;
    private static final int SIOCGSTAMP = 0x8906;
    private static final int SIOCGSTAMPNS = 0x8907;

    /* levels for [sg]etsockopt */
    private static final int SOL_SOCKET = 1;
    /* option names for [sg]etsockopt */
    private static final int SO_TIMESTAMP = 29;
    private static final int SO_TIMESTAMPNS = 35;

    /* clock ids for clock_gettime() */
    private static final int CLOCK_REALTIME = 0;

    private static final int NANOSEC_PER_SEC = 1000000000;

    private static Boolean isSupported = false;

    static {
	String arch = System.getProperty("os.arch").toLowerCase();
	if(!(Platform.isLinux() && arch.equals("x86_64"))) {
	    logger.info("UnixUtils not implemented on this platform, sorry");
	} else {
	    try {
		HashMap<String, Object> options = new HashMap<String, Object>();

		// This is a horrible hack which allows two different native
		// methods to both call the libc ioctl() function, whose
		// last argument is actually a void* pointing to a userspace
		// structure or primitive which is handled in-kernel, and cannot
		// be represented by a single native method using JNA. What
		// can I say, in the 1970s we didn't know how to design APIs.
		FunctionMapper functionMapper = new FunctionMapper() {
		    public String getFunctionName(NativeLibrary library, Method method) {
			String name = method.getName();
			if(name.startsWith("ioctl"))
			    return "ioctl";
			else
			    return name;
		    }
		};
		options.put(Library.OPTION_FUNCTION_MAPPER, functionMapper);

		// Bring in -lrt, because clock_gettime lives there in Linux.
		NativeLibrary.getInstance("rt");

		// Connect up the methods marked native in this class
		// with their actual C counterparts.
		// We map the current process (passing null as the library name)
		// rather than explicitly mapping libc, on the fairly safe
		// assumption that libc is already loaded.  Also we want to
		// connect at least one function in a library that isn't libc.
		Native.register(UnixUtils.class, NativeLibrary.getInstance(null, options));

		isSupported = true;
	    } catch(NoClassDefFoundError e) {
		logger.info("Could not locate JNA classes", e);
	    } catch(UnsatisfiedLinkError e) {
		logger.info("Failed to link to native library", e);
	    } catch(NoSuchMethodError e) {
		logger.warn("Older version of JNA. Please upgrade to 3.2.7+", e);
	    }
	}
    }

    private static native int ioctl_returns_int(int fd, int cmd, IntByReference valuep) throws LastErrorException;
    private static native int ioctl_returns_longs(int fd, int cmd, LongBuffer valuep) throws LastErrorException;
    private static native int setsockopt(int fd, int level, int opt, IntByReference valuep, int valuelen) throws LastErrorException;
    private static native int clock_gettime(int clk_id, LongBuffer tp) throws LastErrorException;

    /**
     * Get the underlying Unix file descriptor from a
     * java.io.FileDescriptor object.
     */
    public static int getFileDescriptor(FileDescriptor fdes) throws java.io.IOException {
	return SharedSecrets.getJavaIOFileDescriptorAccess().get(fdes);
    }
    /**
     * Get the underlying Unix file descriptor from some
     * derived classes of java.io.InputStream object.
     */
    public static int getFileDescriptor(InputStream is) throws java.io.IOException {
	return getFileDescriptor(((FileInputStream)is).getFD());
    }
    /**
     * Get the underlying Unix file descriptor from some
     * derived classes of java.io.OutputStream object.
     */
    public static int getFileDescriptor(OutputStream os) throws java.io.IOException {
	return getFileDescriptor(((FileOutputStream)os).getFD());
    }

    /**
     * Get the length in bytes of the kernel input queue for the
     * given socket (file descriptor).
     */
    public static int socketGetInputQueueLength(int fd) {
	if(isSupported) {
	    try {
		IntByReference value = new IntByReference();
		int r = ioctl_returns_int(fd, SIOCINQ, value);
		return value.getValue();
	    } catch(Exception e) {
		logger.error("Unexpected error during ioctl(SIOCINQ)", e);
	    }
	}
	return 0;
    }

    /**
     * Get the length in bytes of the kernel output queue for the
     * given socket (file descriptor).
     */
    public static int socketGetOutputQueueLength(int fd) {
	if(isSupported) {
	    try {
		IntByReference value = new IntByReference();
		int r = ioctl_returns_int(fd, SIOCOUTQ, value);
		return value.getValue();
	    } catch(Exception e) {
		logger.error("Unexpected error during ioctl(SIOCOUTQ)", e);
	    }
	}
	return 0;
    }

    /**
     * Enable the collection of timestamps on received packets for the
     * given socket.
     *
     * This method must be called once on each socket before calling
     * socketGetReceiveTimestamp().  Note that in Linux, once any
     * socket enables timestamps, ALL incoming packets on the machine
     * have a timestamp recorded on them regardless of which socket
     * they are destined for.  This may have a performance impact.
     */
    public static void socketEnableReceiveTimestamps(int fd) {
	if (!isSupported)
	    return;
        try {
	    IntByReference value = new IntByReference();
	    value.setValue(1/*true*/);
	    // Actually it doesn't matter whether we use SO_TIMESTAMP
	    // or SO_TIMESTAMPNS here, that only matters when we receive
	    // the timestamp via a cmsg rather using the ioctl.
	    // Internally each packet is stamped with a ktime which is
	    // nanoseconds precision.
	    int r = setsockopt(fd, SOL_SOCKET, SO_TIMESTAMPNS, value, /*sizeof(int)*/4);
        } catch(Exception e) {
	    logger.error("Unexpected error during ioctl(SO_TIMESTAMPNS)", e);
        }
    }

    /**
     * Get the timestamp of the received TCP segment which contained
     * the last byte returned from this socket by read().
     *
     * The timestamp records when the packet was received by the TCP/IP
     * stack. The timestamp is formatted as nanoseconds since the UNIX
     * epoch. You must call the socketEnableReceiveTimestamps() method
     * before calling this method, or the timestamp reported will be the
     * current time instead of the packet receive time.
     */
    public static long socketGetReceiveTimestampNanos(int fd) {
	if (isSupported) {
	    try {
		// SIOCGSTAMPNS actually returns a struct timespec which
		// on x86_64 Linux defines as {long tv_sec;long tv_nsec}
		LongBuffer value = LongBuffer.allocate(2);
		int r = ioctl_returns_longs(fd, SIOCGSTAMPNS, value);
		return value.get(0) * NANOSEC_PER_SEC + value.get(1);
	    } catch(Exception e) {
		logger.error("Unexpected error during ioctl(SIOCGSTAMPNS)", e);
	    }
	}
	// Default to a lower-precision sample of the same clock.
	return System.currentTimeMillis() * 1000000;
    }

    /**
     * Get the current time formatted as nanoseconds since the
     * UNIX epoch.
     *
     * The method returns a value using the same realtime clock source
     * and format as the timestamp returned by socketGetReceiveTimestampNanos(),
     * so that latencies can be measured by subtracting the two results.
     */
    public static long currentTimeNanos() {
	if (isSupported) {
	    try {
		// clock_gettime actually returns a struct timespec which
		// on x86_64 Linux defines as {long tv_sec;long tv_usec}
		// Note, this is the same shape as a struct timeval but
		// the 2nd field is interpreted as nanoseconds rather
		// than microseconds.
		LongBuffer value = LongBuffer.allocate(2);
		int r = clock_gettime(CLOCK_REALTIME, value);
		return value.get(0) * NANOSEC_PER_SEC + value.get(1);
	    } catch(Exception e) {
		logger.error("Unexpected error during clock_gettime()", e);
	    }
        }
	// Default to a lower-precision sample of the same clock.
	return System.currentTimeMillis() * 1000000;
    }
}
