package dev.meshcore.ota.android;

import android.app.Activity;
import android.content.Intent;
import android.graphics.Color;
import android.net.Uri;
import android.os.Bundle;
import android.util.Base64;
import android.util.Log;
import android.view.View;
import android.view.WindowManager;
import android.webkit.ValueCallback;
import android.webkit.WebChromeClient;
import android.webkit.WebSettings;
import android.webkit.WebView;
import android.webkit.WebViewClient;
import android.widget.TextView;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class MainActivity extends Activity {
    private static final String TAG = "MeshCoreOta";
    private static final int FILE_CHOOSER_REQUEST = 1001;

    private WebView webView;
    private BridgeServer bridgeServer;
    private ValueCallback<Uri[]> pendingFileCallback;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        getWindow().addFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON);
        getWindow().setStatusBarColor(Color.rgb(22, 27, 34));
        getWindow().setNavigationBarColor(Color.rgb(13, 17, 23));

        try {
            bridgeServer = new BridgeServer();
            bridgeServer.start();
            createWebView(bridgeServer.getLocalUrl());
        } catch (IOException error) {
            Log.e(TAG, "Unable to start local OTA bridge", error);
            TextView message = new TextView(this);
            message.setTextColor(Color.WHITE);
            message.setBackgroundColor(Color.rgb(13, 17, 23));
            message.setPadding(32, 32, 32, 32);
            message.setText("Impossible de démarrer le pont OTA local.\n\n" + error.getMessage());
            setContentView(message);
        }
    }

    private void createWebView(String localUrl) {
        webView = new WebView(this);
        webView.setBackgroundColor(Color.rgb(13, 17, 23));
        webView.setOverScrollMode(View.OVER_SCROLL_NEVER);

        WebSettings settings = webView.getSettings();
        settings.setJavaScriptEnabled(true);
        settings.setDomStorageEnabled(true);
        settings.setAllowFileAccess(false);
        settings.setAllowContentAccess(true);
        settings.setBuiltInZoomControls(false);
        settings.setDisplayZoomControls(false);
        settings.setMediaPlaybackRequiresUserGesture(true);

        webView.setWebViewClient(new WebViewClient() {
            @Override
            public void onPageFinished(WebView view, String url) {
                super.onPageFinished(view, url);
                // main.go only supplies the TCP bridge. Select the corresponding
                // transport by default while leaving the original UI untouched.
                view.evaluateJavascript(
                    "(function(){" +
                    "var s=document.getElementById('connectionMode');" +
                    "if(s){s.value='tcp';s.dispatchEvent(new Event('change'));}" +
                    "var h=document.querySelector('.header-sub');" +
                    "if(h){h.textContent='Android • TCP WiFi';}" +
                    "})();",
                    null
                );
            }
        });
        webView.setWebChromeClient(new WebChromeClient() {
            @Override
            public boolean onShowFileChooser(
                WebView view,
                ValueCallback<Uri[]> filePathCallback,
                FileChooserParams fileChooserParams
            ) {
                if (pendingFileCallback != null) {
                    pendingFileCallback.onReceiveValue(null);
                }
                pendingFileCallback = filePathCallback;

                Intent intent = new Intent(Intent.ACTION_OPEN_DOCUMENT);
                intent.addCategory(Intent.CATEGORY_OPENABLE);
                intent.setType("application/octet-stream");
                intent.putExtra(Intent.EXTRA_MIME_TYPES, new String[] {
                    "application/octet-stream",
                    "application/gzip",
                    "application/x-gzip",
                    "*/*"
                });
                startActivityForResult(intent, FILE_CHOOSER_REQUEST);
                return true;
            }
        });

        setContentView(webView);
        webView.loadUrl(localUrl);
    }

    @Override
    protected void onActivityResult(int requestCode, int resultCode, Intent data) {
        super.onActivityResult(requestCode, resultCode, data);
        if (requestCode != FILE_CHOOSER_REQUEST || pendingFileCallback == null) {
            return;
        }
        Uri[] result = null;
        if (resultCode == RESULT_OK && data != null && data.getData() != null) {
            result = new Uri[] {data.getData()};
        }
        pendingFileCallback.onReceiveValue(result);
        pendingFileCallback = null;
    }

    @Override
    public void onBackPressed() {
        if (webView != null && webView.canGoBack()) {
            webView.goBack();
        } else {
            super.onBackPressed();
        }
    }

    @Override
    protected void onDestroy() {
        if (pendingFileCallback != null) {
            pendingFileCallback.onReceiveValue(null);
            pendingFileCallback = null;
        }
        if (webView != null) {
            webView.stopLoading();
            webView.destroy();
            webView = null;
        }
        if (bridgeServer != null) {
            bridgeServer.close();
            bridgeServer = null;
        }
        super.onDestroy();
    }

    private final class BridgeServer implements Closeable {
        private static final int MAX_HTTP_HEADER = 32 * 1024;
        private static final int MAX_WS_FRAME = 1024 * 1024;
        private static final int RELAY_BUFFER_SIZE = 8 * 1024;
        private static final int DIAL_TIMEOUT_MS = 6000;

        private final ExecutorService workers = Executors.newCachedThreadPool();
        private volatile boolean running;
        private ServerSocket serverSocket;

        void start() throws IOException {
            serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0));
            running = true;
            workers.execute(this::acceptLoop);
            Log.i(TAG, "Local bridge listening at " + getLocalUrl());
        }

        String getLocalUrl() {
            return "http://127.0.0.1:" + serverSocket.getLocalPort() + "/";
        }

        private void acceptLoop() {
            while (running) {
                try {
                    Socket client = serverSocket.accept();
                    workers.execute(() -> handleClient(client));
                } catch (IOException error) {
                    if (running) {
                        Log.e(TAG, "Bridge accept failed", error);
                    }
                }
            }
        }

        private void handleClient(Socket client) {
            try (Socket connection = client) {
                connection.setSoTimeout(10_000);
                InputStream input = connection.getInputStream();
                OutputStream output = connection.getOutputStream();
                HttpRequest request = readHttpRequest(input);

                if ("/tcp".equals(request.path) && request.isWebSocketUpgrade()) {
                    handleTcpWebSocket(connection, input, output, request);
                    return;
                }
                if (!"GET".equals(request.method)) {
                    writeHttpResponse(output, 405, "text/plain; charset=utf-8", "Method Not Allowed".getBytes(StandardCharsets.UTF_8));
                    return;
                }
                serveAsset(output, request.path);
            } catch (IOException error) {
                Log.d(TAG, "Bridge client ended: " + error.getMessage());
            }
        }

        private HttpRequest readHttpRequest(InputStream input) throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            int matched = 0;
            while (bytes.size() < MAX_HTTP_HEADER) {
                int value = input.read();
                if (value < 0) {
                    throw new EOFException("HTTP request ended before headers");
                }
                bytes.write(value);
                if ((matched == 0 || matched == 2) && value == '\r') {
                    matched++;
                } else if ((matched == 1 || matched == 3) && value == '\n') {
                    matched++;
                    if (matched == 4) {
                        break;
                    }
                } else {
                    matched = value == '\r' ? 1 : 0;
                }
            }
            if (matched != 4) {
                throw new IOException("HTTP headers too large");
            }

            String headerText = bytes.toString(StandardCharsets.ISO_8859_1.name());
            String[] lines = headerText.split("\\r\\n");
            if (lines.length == 0) {
                throw new IOException("Empty HTTP request");
            }
            String[] requestLine = lines[0].split(" ", 3);
            if (requestLine.length < 2) {
                throw new IOException("Malformed HTTP request line");
            }

            String target = requestLine[1];
            int question = target.indexOf('?');
            String path = question >= 0 ? target.substring(0, question) : target;
            String query = question >= 0 ? target.substring(question + 1) : "";
            Map<String, String> headers = new HashMap<>();
            for (int i = 1; i < lines.length; i++) {
                int colon = lines[i].indexOf(':');
                if (colon > 0) {
                    headers.put(
                        lines[i].substring(0, colon).trim().toLowerCase(Locale.ROOT),
                        lines[i].substring(colon + 1).trim()
                    );
                }
            }
            return new HttpRequest(requestLine[0], path, parseQuery(query), headers);
        }

        private Map<String, String> parseQuery(String query) {
            Map<String, String> values = new HashMap<>();
            for (String pair : query.split("&")) {
                if (pair.isEmpty()) {
                    continue;
                }
                int equals = pair.indexOf('=');
                String rawKey = equals >= 0 ? pair.substring(0, equals) : pair;
                String rawValue = equals >= 0 ? pair.substring(equals + 1) : "";
                values.put(urlDecode(rawKey), urlDecode(rawValue));
            }
            return values;
        }

        private String urlDecode(String value) {
            try {
                return URLDecoder.decode(value, StandardCharsets.UTF_8.name());
            } catch (Exception ignored) {
                return value;
            }
        }

        private void serveAsset(OutputStream output, String requestedPath) throws IOException {
            String asset;
            String contentType;
            if ("/".equals(requestedPath) || "/index.html".equals(requestedPath)) {
                asset = "index.html";
                contentType = "text/html; charset=utf-8";
            } else if ("/app.js".equals(requestedPath)) {
                asset = "app.js";
                contentType = "application/javascript; charset=utf-8";
            } else if ("/favicon.ico".equals(requestedPath)) {
                writeHttpResponse(output, 204, "image/x-icon", new byte[0]);
                return;
            } else {
                writeHttpResponse(output, 404, "text/plain; charset=utf-8", "Not Found".getBytes(StandardCharsets.UTF_8));
                return;
            }

            try (InputStream assetInput = getAssets().open(asset)) {
                writeHttpResponse(output, 200, contentType, readAll(assetInput));
            }
        }

        private byte[] readAll(InputStream input) throws IOException {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            byte[] buffer = new byte[16 * 1024];
            int count;
            while ((count = input.read(buffer)) >= 0) {
                if (count > 0) {
                    output.write(buffer, 0, count);
                }
            }
            return output.toByteArray();
        }

        private void writeHttpResponse(OutputStream output, int status, String contentType, byte[] body) throws IOException {
            String reason = status == 200 ? "OK"
                : status == 204 ? "No Content"
                : status == 404 ? "Not Found"
                : status == 405 ? "Method Not Allowed"
                : "Error";
            String headers = "HTTP/1.1 " + status + " " + reason + "\r\n"
                + "Content-Type: " + contentType + "\r\n"
                + "Content-Length: " + body.length + "\r\n"
                + "Cache-Control: no-store\r\n"
                + "Connection: close\r\n\r\n";
            output.write(headers.getBytes(StandardCharsets.ISO_8859_1));
            output.write(body);
            output.flush();
        }

        private void handleTcpWebSocket(
            Socket browser,
            InputStream browserInput,
            OutputStream browserOutput,
            HttpRequest request
        ) throws IOException {
            String key = request.headers.get("sec-websocket-key");
            if (key == null || key.isEmpty()) {
                writeHttpResponse(browserOutput, 400, "text/plain; charset=utf-8", "Missing WebSocket key".getBytes(StandardCharsets.UTF_8));
                return;
            }

            String accept = websocketAccept(key);
            String response = "HTTP/1.1 101 Switching Protocols\r\n"
                + "Upgrade: websocket\r\n"
                + "Connection: Upgrade\r\n"
                + "Sec-WebSocket-Accept: " + accept + "\r\n\r\n";
            browserOutput.write(response.getBytes(StandardCharsets.ISO_8859_1));
            browserOutput.flush();
            browser.setSoTimeout(0);

            WebSocketWriter writer = new WebSocketWriter(browserOutput);
            String host = request.query.get("host");
            String rawPort = request.query.get("port");
            int port;
            try {
                port = Integer.parseInt(rawPort == null ? "" : rawPort);
                if (port < 1 || port > 65535) {
                    throw new NumberFormatException("out of range");
                }
            } catch (NumberFormatException error) {
                writer.writeText("error port TCP invalide");
                writer.writeClose(1000);
                return;
            }
            if (host == null || host.trim().isEmpty()) {
                writer.writeText("error hôte TCP manquant");
                writer.writeClose(1000);
                return;
            }

            Socket companion = new Socket();
            try {
                companion.connect(new InetSocketAddress(host.trim(), port), DIAL_TIMEOUT_MS);
                companion.setTcpNoDelay(true);
            } catch (IOException error) {
                closeQuietly(companion);
                writer.writeText("error " + error.getMessage());
                writer.writeClose(1000);
                return;
            }

            Log.i(TAG, "Connected companion " + host + ":" + port);
            writer.writeText("ready");

            workers.execute(() -> {
                try {
                    byte[] buffer = new byte[RELAY_BUFFER_SIZE];
                    InputStream tcpInput = companion.getInputStream();
                    int count;
                    while ((count = tcpInput.read(buffer)) >= 0) {
                        if (count > 0) {
                            writer.writeBinary(buffer, count);
                        }
                    }
                } catch (IOException ignored) {
                    // Closing either side is the normal way to end a relay.
                } finally {
                    try {
                        writer.writeClose(1000);
                    } catch (IOException ignored) {
                    }
                    closeQuietly(companion);
                    closeQuietly(browser);
                }
            });

            try {
                OutputStream tcpOutput = companion.getOutputStream();
                while (true) {
                    WebSocketFrame frame = readWebSocketFrame(browserInput);
                    if (frame.opcode == 0x2 || frame.opcode == 0x0) {
                        tcpOutput.write(frame.payload);
                        tcpOutput.flush();
                    } else if (frame.opcode == 0x9) {
                        writer.writeFrame(0xA, frame.payload, frame.payload.length);
                    } else if (frame.opcode == 0x8) {
                        break;
                    }
                }
            } finally {
                closeQuietly(companion);
            }
        }

        private String websocketAccept(String key) throws IOException {
            try {
                MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
                byte[] digest = sha1.digest((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")
                    .getBytes(StandardCharsets.ISO_8859_1));
                return Base64.encodeToString(digest, Base64.NO_WRAP);
            } catch (Exception error) {
                throw new IOException("SHA-1 unavailable", error);
            }
        }

        private WebSocketFrame readWebSocketFrame(InputStream input) throws IOException {
            int first = input.read();
            int second = input.read();
            if (first < 0 || second < 0) {
                throw new EOFException("WebSocket closed");
            }
            int opcode = first & 0x0f;
            boolean masked = (second & 0x80) != 0;
            long length = second & 0x7f;
            if (length == 126) {
                length = ((long) readByte(input) << 8) | readByte(input);
            } else if (length == 127) {
                length = 0;
                for (int i = 0; i < 8; i++) {
                    length = (length << 8) | readByte(input);
                }
            }
            if (length > MAX_WS_FRAME) {
                throw new IOException("WebSocket frame too large: " + length);
            }
            if (!masked) {
                throw new IOException("Client WebSocket frame is not masked");
            }

            byte[] mask = readExact(input, 4);
            byte[] payload = readExact(input, (int) length);
            for (int i = 0; i < payload.length; i++) {
                payload[i] = (byte) (payload[i] ^ mask[i & 3]);
            }
            return new WebSocketFrame(opcode, payload);
        }

        private int readByte(InputStream input) throws IOException {
            int value = input.read();
            if (value < 0) {
                throw new EOFException("Unexpected WebSocket EOF");
            }
            return value;
        }

        private byte[] readExact(InputStream input, int length) throws IOException {
            byte[] result = new byte[length];
            int offset = 0;
            while (offset < length) {
                int count = input.read(result, offset, length - offset);
                if (count < 0) {
                    throw new EOFException("Unexpected WebSocket EOF");
                }
                offset += count;
            }
            return result;
        }

        @Override
        public void close() {
            running = false;
            closeQuietly(serverSocket);
            workers.shutdownNow();
        }

        private void closeQuietly(Closeable closeable) {
            if (closeable == null) {
                return;
            }
            try {
                closeable.close();
            } catch (IOException ignored) {
            }
        }

        private final class WebSocketWriter {
            private final OutputStream output;

            WebSocketWriter(OutputStream output) {
                this.output = output;
            }

            synchronized void writeBinary(byte[] payload, int length) throws IOException {
                writeFrame(0x2, payload, length);
            }

            synchronized void writeText(String value) throws IOException {
                byte[] payload = value.getBytes(StandardCharsets.UTF_8);
                writeFrame(0x1, payload, payload.length);
            }

            synchronized void writeClose(int code) throws IOException {
                byte[] payload = new byte[] {(byte) (code >> 8), (byte) code};
                writeFrame(0x8, payload, payload.length);
            }

            synchronized void writeFrame(int opcode, byte[] payload, int length) throws IOException {
                output.write(0x80 | opcode);
                if (length < 126) {
                    output.write(length);
                } else if (length < 65_536) {
                    output.write(126);
                    output.write((length >> 8) & 0xff);
                    output.write(length & 0xff);
                } else {
                    output.write(127);
                    for (int shift = 56; shift >= 0; shift -= 8) {
                        output.write((length >> shift) & 0xff);
                    }
                }
                output.write(payload, 0, length);
                output.flush();
            }
        }

        private final class HttpRequest {
            final String method;
            final String path;
            final Map<String, String> query;
            final Map<String, String> headers;

            HttpRequest(String method, String path, Map<String, String> query, Map<String, String> headers) {
                this.method = method;
                this.path = path;
                this.query = query;
                this.headers = headers;
            }

            boolean isWebSocketUpgrade() {
                String connection = headers.get("connection");
                String upgrade = headers.get("upgrade");
                return connection != null
                    && connection.toLowerCase(Locale.ROOT).contains("upgrade")
                    && "websocket".equalsIgnoreCase(upgrade);
            }
        }

        private final class WebSocketFrame {
            final int opcode;
            final byte[] payload;

            WebSocketFrame(int opcode, byte[] payload) {
                this.opcode = opcode;
                this.payload = payload;
            }
        }
    }
}
