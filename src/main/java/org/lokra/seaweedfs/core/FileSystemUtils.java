package org.lokra.seaweedfs.core;

import java.io.IOException;

public class FileSystemUtils {
	public static FileSource fileSource;

	static {
		fileSource = new FileSource();
		fileSource.setHost("weedfs");
		fileSource.setPort(9333);
	}

	public static void startup() throws IOException, InterruptedException {
		if (fileSource.getConnection() == null) {
			fileSource.startup();
			Thread.sleep(3000);
		}
	}

	public static void shutdown() {
		fileSource.shutdown();
	}
}
