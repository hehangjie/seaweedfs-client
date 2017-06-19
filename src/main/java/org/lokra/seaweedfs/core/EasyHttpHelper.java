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

/**
 * @Description: 放弃复杂的CachingHttpClient,使用简易的http请求封装
 * @Copyright: 卓智网络科技有限公司 (c)2017
 * @Created Date : 2017年6月19日
 * @author hehangjie
 * @vesion 1.0
 */
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.CharsetUtils;
import org.apache.http.util.EntityUtils;
import org.lokra.seaweedfs.core.contect.AssignFileKeyResult;
import org.lokra.seaweedfs.core.file.FileHandleStatus;
import org.lokra.seaweedfs.core.http.HeaderResponse;
import org.lokra.seaweedfs.core.http.JsonResponse;
import org.lokra.seaweedfs.core.http.StreamResponse;
import org.lokra.seaweedfs.exception.SeaweedfsException;
import org.lokra.seaweedfs.exception.SeaweedfsFileNotFoundException;
import org.lokra.seaweedfs.util.RequestPathStrategy;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Map;

public class EasyHttpHelper {

	static final String LOOKUP_VOLUME_CACHE_ALIAS = "lookupVolumeCache";

	private static final Log log = LogFactory.getLog(EasyHttpHelper.class);

	private static ObjectMapper objectMapper = new ObjectMapper();

	static AssignFileKeyResult assignFileKey() throws IOException {
		final String url = "http://" + FileSystemUtils.fileSource.getHost() + ":" + FileSystemUtils.fileSource.getPort()
				+ RequestPathStrategy.assignFileKey;
		HttpGet request = new HttpGet(url);
		JsonResponse jsonResponse = fetchJsonResultByRequest(request);
		return objectMapper.readValue(jsonResponse.json, AssignFileKeyResult.class);
	}

	/**
	 * uploadFile
	 * 
	 * @param fileName
	 * @param stream
	 * @return
	 * @throws IOException
	 */
	public static FileHandleStatus uploadFile(String fileName, InputStream stream) throws IOException {
		HttpPost request;
		AssignFileKeyResult assign = assignFileKey();
		request = new HttpPost(assign.getPublicUrl() + "/" + assign.getFid());

		MultipartEntityBuilder builder = MultipartEntityBuilder.create();
		builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE).setCharset(CharsetUtils.get("UTF-8"));
		builder.addBinaryBody("upload", stream, contentType(fileName), fileName);
		HttpEntity entity = builder.build();
		request.setEntity(entity);
		JsonResponse jsonResponse = fetchJsonResultByRequest(request);

		FileHandleStatus status = new FileHandleStatus(assign.getFid(),
				(Integer) objectMapper.readValue(jsonResponse.json, Map.class).get("size"));

		return status;
	}

	/**
	 * check
	 * @param fid
	 * @return
	 * @throws IOException
	 */
	public static boolean checkFileExist(String fid) throws IOException {
		HttpHead request = new HttpHead("http://" + FileSystemUtils.fileSource.getHost() + ":9000/" + fid);
		final int statusCode = fetchStatusCodeByRequest(request);
		try {
			convertResponseStatusToException(statusCode, fid, false, true, false, false);
			return true;
		} catch (SeaweedfsFileNotFoundException e) {
			return false;
		}
	}
	
	/**
	 * download getFileStream
	 * @param url
	 * @param fid
	 * @return
	 * @throws IOException
	 */
	public static StreamResponse getFileStream(String fid) throws IOException {
        HttpGet request = new HttpGet("http://" + FileSystemUtils.fileSource.getHost() + ":9000/" + fid);
        StreamResponse cache = fetchStreamByRequest(request);
        convertResponseStatusToException(cache.getHttpResponseStatusCode(), fid, false, false, false, false);
        return cache;
    }
	
	/**
	 * down file
	 * @param fid
	 * @param downloadFile
	 * @throws IOException
	 */
	public static void getFile(String fid, File downloadFile) throws IOException {
        HttpGet request = new HttpGet("http://" + FileSystemUtils.fileSource.getHost() + ":9000/" + fid);
        StreamResponse resp = fetchStreamByRequest(request);
        convertResponseStatusToException(resp.getHttpResponseStatusCode(), fid, false, true, false, false);
		InputStream ins = resp.getInputStream();
		try {
			OutputStream os = new FileOutputStream(downloadFile);
			int bytesRead = 0;
			byte[] buffer = new byte[1024];
			while ((bytesRead = ins.read(buffer, 0, 1024)) != -1) {
				os.write(buffer, 0, bytesRead);
			}
			os.close();
		} catch (Exception e) {
			log.error(e.toString());
		}
    }

	/**
	 * Fetch http API json result.
	 *
	 * @param request
	 *            Http request.
	 * @return Json fetch by http response.
	 * @throws IOException
	 *             Http connection is fail or server response within some error
	 *             message.
	 */
	static JsonResponse fetchJsonResultByRequest(HttpRequestBase request) throws IOException {
		CloseableHttpResponse response = null;
		request.setHeader("Connection", "close");
		JsonResponse jsonResponse = null;
		CloseableHttpClient httpclient = HttpClients.createDefault();

		try {
			response = httpclient.execute(request, HttpClientContext.create());
			HttpEntity entity = response.getEntity();
			jsonResponse = new JsonResponse(EntityUtils.toString(entity), response.getStatusLine().getStatusCode());
			EntityUtils.consume(entity);
		} finally {
			if (response != null) {
				try {
					response.close();
				} catch (IOException ignored) {
				}
			}
			request.releaseConnection();
		}

		if (jsonResponse.json.contains("\"error\":\"")) {
			Map map = objectMapper.readValue(jsonResponse.json, Map.class);
			final String errorMsg = (String) map.get("error");
			if (errorMsg != null)
				throw new SeaweedfsException(errorMsg);
		}

		return jsonResponse;
	}

	/**
	 * Fetch http API status code.
	 *
	 * @param request
	 *            Only http method head.
	 * @return Status code.
	 * @throws IOException
	 *             Http connection is fail or server response within some error
	 *             message.
	 */
	static int fetchStatusCodeByRequest(HttpHead request) throws IOException {
		CloseableHttpResponse response = null;
		CloseableHttpClient httpclient = HttpClients.createDefault();
		request.setHeader("Connection", "close");
		int statusCode;
		try {
			response = httpclient.execute(request, HttpClientContext.create());
			statusCode = response.getStatusLine().getStatusCode();
		} finally {
			if (response != null) {
				try {
					response.close();
				} catch (IOException ignored) {
					log.error(ignored.toString());
				}
			}
			request.releaseConnection();
		}
		return statusCode;
	}

	/**
	 * Fetch http API input stream cache.
	 *
	 * @param request
	 *            Http request.
	 * @return Stream fetch by http response.
	 * @throws IOException
	 *             Http connection is fail or server response within some error
	 *             message.
	 */
	static StreamResponse fetchStreamByRequest(HttpRequestBase request) throws IOException {
		CloseableHttpClient httpclient = HttpClients.createDefault();
		CloseableHttpResponse response = null;
		request.setHeader("Connection", "close");
		StreamResponse cache;

		try {
			response = httpclient.execute(request, HttpClientContext.create());
			HttpEntity entity = response.getEntity();
			cache = new StreamResponse(entity.getContent(), response.getStatusLine().getStatusCode());
			EntityUtils.consume(entity);
		} finally {
			if (response != null) {
				try {
					response.close();
				} catch (IOException ignored) {
					log.error(ignored.toString());
				}
			}
			request.releaseConnection();
		}
		return cache;
	}

	/**
	 * Fetch http API hearers with status code(in array).
	 *
	 * @param request
	 *            Only http method head.
	 * @return Header fetch by http response.
	 * @throws IOException
	 *             Http connection is fail or server response within some error
	 *             message.
	 */
	HeaderResponse fetchHeaderByRequest(HttpHead request) throws IOException {
		CloseableHttpClient httpclient = HttpClients.createDefault();
		CloseableHttpResponse response = null;
		request.setHeader("Connection", "close");
		HeaderResponse headerResponse;

		try {
			response = httpclient.execute(request, HttpClientContext.create());
			headerResponse = new HeaderResponse(response.getAllHeaders(), response.getStatusLine().getStatusCode());
		} finally {
			if (response != null) {
				try {
					response.close();
				} catch (IOException ignored) {
					log.error(ignored.toString());
				}
			}
			request.releaseConnection();
		}
		return headerResponse;
	}

	/**
	 * @Description: 取得不同文件的contentType
	 * @Create: 2017年6月18日 下午3:30:45
	 * @author hehangjie
	 * @return
	 */
	public static ContentType contentType(String key) {

		try {
			if (key.toLowerCase().endsWith(".m3u8")) {
				return ContentType.create("application/x-mpegURL", CharsetUtils.get("UTF-8"));
			}

			if (key.toLowerCase().endsWith(".ts")) {
				return ContentType.create("video/MP2T", CharsetUtils.get("UTF-8"));
			}

			if (key.toLowerCase().endsWith(".bmp")) {
				return ContentType.create("image/bmp", CharsetUtils.get("UTF-8"));
			}

			if (key.toUpperCase().endsWith(".gif")) {
				return ContentType.create("image/gif", CharsetUtils.get("UTF-8"));
			}

			if (key.toLowerCase().endsWith(".jpeg") || key.endsWith(".JPG") || key.endsWith(".jpg")
					|| key.endsWith(".PNG") || key.endsWith(".png")) {
				return ContentType.create("image/jpeg", CharsetUtils.get("UTF-8"));
			}

			if (key.toLowerCase().endsWith(".html")) {
				return ContentType.create("text/html", CharsetUtils.get("UTF-8"));
			}

			if (key.toLowerCase().endsWith(".txt")) {
				return ContentType.create("text/plain", CharsetUtils.get("UTF-8"));
			}

			if (key.toLowerCase().endsWith(".vsd")) {
				return ContentType.create("application/vnd.visio", CharsetUtils.get("UTF-8"));
			}

			if (key.toLowerCase().endsWith(".pptx") || key.toLowerCase().endsWith(".ppt")) {
				return ContentType.create("application/vnd.ms-powerpoint", CharsetUtils.get("UTF-8"));
			}

			if (key.toLowerCase().endsWith(".docx") || key.toLowerCase().endsWith(".doc")) {
				return ContentType.create("application/msword", CharsetUtils.get("UTF-8"));
			}

			if (key.toLowerCase().endsWith(".xml")) {
				return ContentType.create("text/xml", CharsetUtils.get("UTF-8"));
			}

			if (key.toLowerCase().endsWith(".mp4")) {
				return ContentType.create("video/mpeg4", CharsetUtils.get("UTF-8"));
			}

			if (key.toLowerCase().endsWith(".mp3")) {
				return ContentType.create("audio/mp3", CharsetUtils.get("UTF-8"));
			}

			if (key.toLowerCase().endsWith("flv")) {
				return ContentType.create("video/x-flv", CharsetUtils.get("UTF-8"));
			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		return null;
	}

	private static void convertResponseStatusToException(int statusCode, String fid, boolean ignoreNotFound,
			boolean ignoreRedirect, boolean ignoreRequestError, boolean ignoreServerError) throws SeaweedfsException {

		switch (statusCode / 100) {
		case 1:
			return;
		case 2:
			return;
		case 3:
			if (ignoreRedirect)
				return;
			throw new SeaweedfsException(
					"fetch file from [" + fid + "] is redirect, " + "response stats code is [" + statusCode + "]");
		case 4:
			if (statusCode == 404 && ignoreNotFound)
				return;
			else if (statusCode == 404)
				throw new SeaweedfsFileNotFoundException(
						"fetch file from [" + fid + "] is not found, " + "response stats code is [" + statusCode + "]");
			if (ignoreRequestError)
				return;
			throw new SeaweedfsException(
					"fetch file from [" + fid + "] is request error, " + "response stats code is [" + statusCode + "]");
		case 5:
			if (ignoreServerError)
				return;
			throw new SeaweedfsException(
					"fetch file from [" + fid + "] is request error, " + "response stats code is [" + statusCode + "]");
		default:
			throw new SeaweedfsException(
					"fetch file from [" + fid + "] is error, " + "response stats code is [" + statusCode + "]");
		}
	}

}
