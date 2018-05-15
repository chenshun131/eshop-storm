package com.chenshun.storm.http;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * User: mew <p />
 * Time: 18/5/15 09:19  <p />
 * Version: V1.0  <p />
 * Description: HttpClient 工具类 <p />
 */
public class HttpClientUtils {

    /**
     * 发送GET请求
     *
     * @param url
     *         请求URL
     * @return 响应结果
     */
    public static String sendGetRequest(String url) {
        String httpResponse = null;
        InputStream is = null;
        BufferedReader br = null;

        try {
            // 发送GET请求
            CloseableHttpClient httpClient = HttpClientUtils.createDefault();
            HttpGet httpget = new HttpGet(url);
            HttpResponse response = httpClient.execute(httpget);

            // 处理响应
            HttpEntity entity = response.getEntity();
            if (entity != null && response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                is = entity.getContent();
                br = new BufferedReader(new InputStreamReader(is));
                StringBuilder buffer = new StringBuilder();
                String line = null;
                while ((line = br.readLine()) != null) {
                    buffer.append(line + "\n");
                }
                httpResponse = buffer.toString();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
                if (is != null) {
                    is.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        return httpResponse;
    }

    /**
     * 发送post请求
     *
     * @param url
     *         URL
     * @param map
     *         参数Map
     * @return
     */
    public static String sendPostRequest(String url, Map<String, String> map) {
        String result = null;
        try {
            CloseableHttpClient httpClient = HttpClientUtils.createDefault();
            HttpPost httpPost = new HttpPost(url);

            // 设置参数
            List<NameValuePair> list = new ArrayList<NameValuePair>();
            for (Entry<String, String> entry : map.entrySet()) {
                list.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
            }
            if (list.size() > 0) {
                UrlEncodedFormEntity entity = new UrlEncodedFormEntity(list, "utf-8");
                httpPost.setEntity(entity);
            }

            HttpResponse response = httpClient.execute(httpPost);
            if (response != null && response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                HttpEntity resEntity = response.getEntity();
                if (resEntity != null) {
                    result = EntityUtils.toString(resEntity, "utf-8");
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return result;
    }

    private static CloseableHttpClient createDefault() {
        return HttpClientBuilder.create().build();
    }

}