/*
 * Copyright (c) 2016 Lokra Studio
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.lokra.seaweedfs.core;

import org.junit.Assert;
import org.junit.Test;
import org.lokra.seaweedfs.core.file.FileHandleStatus;
import org.lokra.seaweedfs.core.http.StreamResponse;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author Chiho Sin
 */
public class EasyHttpHelperTemplateTest {

	@Test
	public void upload() throws Exception { 
		File file = new File("/Users/hehj/Downloads/b5bcbf54-2b10-48d2-bfaf-1c9924c7c32e.mp4");
		InputStream in = new FileInputStream(file);
		FileHandleStatus fileHandleStatus = EasyHttpHelper.uploadFile("b5bcbf54-2b10-48d2-bfaf-1c9924c7c32e.mp4", in);
		System.out.println("fid=" + fileHandleStatus.getFileId());
		Assert.assertTrue(fileHandleStatus.getSize() > 0);
	}

	@Test
	public void check() throws Exception {
		File file = new File("/Users/hehj/Downloads/b5bcbf54-2b10-48d2-bfaf-1c9924c7c32e.mp4");
		InputStream in = new FileInputStream(file);
		FileHandleStatus fileHandleStatus = EasyHttpHelper.uploadFile("b5bcbf54-2b10-48d2-bfaf-1c9924c7c32e.mp4", in);
		System.out.println("fid=" + fileHandleStatus.getFileId());
		Assert.assertTrue(EasyHttpHelper.checkFileExist(fileHandleStatus.getFileId()));
	}

	@Test
	public void download() throws Exception {
		File file = new File("/Users/hehj/Downloads/b5bcbf54-2b10-48d2-bfaf-1c9924c7c32e.mp4");
		InputStream in = new FileInputStream(file);
		FileHandleStatus fileHandleStatus = EasyHttpHelper.uploadFile("b5bcbf54-2b10-48d2-bfaf-1c9924c7c32e.mp4", in);
		System.out.println("fid=" + fileHandleStatus.getFileId());
		StreamResponse resp = EasyHttpHelper.getFileStream(fileHandleStatus.getFileId());
		InputStream ins = resp.getInputStream();
		File downFile = new File("/Users/hehj/Downloads/download.mp4");
		try {
			OutputStream os = new FileOutputStream(downFile);
			int bytesRead = 0;
			byte[] buffer = new byte[1024];
			while ((bytesRead = ins.read(buffer, 0, 1024)) != -1) {
				os.write(buffer, 0, bytesRead);
			}
			os.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		Assert.assertTrue(downFile.exists());
	}

}