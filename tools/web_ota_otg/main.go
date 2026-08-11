// meshcore-ota-bridge — a tiny, dependency-free local bridge that lets the
// web OTA tool talk to a MeshCore companion over TCP.
//
// Browsers cannot open raw TCP sockets, but the companion's WiFi interface
// (SerialWifiInterface) speaks the exact same framing as the serial link
// ('>'/'<' + LE16 length). So this bridge is a *dumb byte pipe*: it serves the
// existing web page locally and exposes a WebSocket endpoint that relays raw
// bytes to/from the companion's TCP socket. The page's `web_tcp` transport
// reuses parseFrames() untouched.
//
// Build:   go build -o meshcore-ota-bridge.exe
// Run:     meshcore-ota-bridge.exe            (serves 127.0.0.1:8080, opens browser)
//
//	meshcore-ota-bridge.exe --lan      (also reachable from LAN, e.g. a phone)
package main

import (
	"bufio"
	"crypto/sha1"
	"embed"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"
)

// Optional Windows version metadata (a legitimacy signal for antivirus ML).
// One-time: go install github.com/josephspurrier/goversioninfo/cmd/goversioninfo@latest
// Then `go generate` emits resource.syso, which `go build` links in automatically.
// Entirely optional — normal builds work without it.
//go:generate goversioninfo -o resource.syso versioninfo.json

//go:embed index.html app.js
var embeddedWeb embed.FS

const (
	opContinuation = 0x0
	opText         = 0x1
	opBinary       = 0x2
	opClose        = 0x8
	opPing         = 0x9
	opPong         = 0xA

	maxFrame     = 1 << 20 // 1 MiB — OTA frames are tiny; this is just a sanity cap.
	dialTimeout  = 6 * time.Second
	relayBufSize = 8 * 1024
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "listen address host:port")
	lan := flag.Bool("lan", false, "listen on all interfaces so other devices (e.g. a phone) can use this bridge")
	noBrowser := flag.Bool("no-browser", false, "do not auto-open the default browser")
	webDir := flag.String("web", "", "serve the web UI from this directory instead of the embedded copy (dev)")
	flag.Parse()

	listenAddr := *addr
	if *lan {
		_, port, err := net.SplitHostPort(*addr)
		if err != nil {
			port = "8080"
		}
		listenAddr = net.JoinHostPort("0.0.0.0", port)
	}

	var static http.FileSystem
	if *webDir != "" {
		static = http.Dir(*webDir)
		log.Printf("serving web UI from %s", *webDir)
	} else {
		sub, err := fs.Sub(embeddedWeb, ".")
		if err != nil {
			log.Fatalf("embed: %v", err)
		}
		static = http.FS(sub)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/tcp", handleTCP)
	mux.Handle("/", http.FileServer(static))

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen %s: %v", listenAddr, err)
	}

	_, port, _ := net.SplitHostPort(ln.Addr().String())
	localURL := fmt.Sprintf("http://127.0.0.1:%s/", port)

	fmt.Println("MeshCore OTA bridge")
	fmt.Printf("  UI local  : %s\n", localURL)
	if *lan {
		for _, ip := range lanIPs() {
			fmt.Printf("  UI réseau : http://%s:%s/  (ouvrez cette URL depuis un téléphone du même WiFi)\n", ip, port)
		}
	}
	fmt.Println("  Ctrl+C pour quitter.")

	if !*noBrowser {
		go openBrowser(localURL)
	}

	srv := &http.Server{Handler: mux}
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("serve: %v", err)
	}
}

// handleTCP upgrades the request to a WebSocket, dials the companion's TCP
// endpoint given by the host/port query params, then pumps bytes both ways.
func handleTCP(w http.ResponseWriter, r *http.Request) {
	host := r.URL.Query().Get("host")
	port := r.URL.Query().Get("port")
	if host == "" || port == "" {
		http.Error(w, "missing host/port", http.StatusBadRequest)
		return
	}

	ws, err := upgrade(w, r)
	if err != nil {
		log.Printf("ws upgrade failed: %v", err)
		return
	}
	defer ws.Close()

	addr := net.JoinHostPort(host, port)
	log.Printf("dialing companion %s", addr)
	tcp, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		log.Printf("dial %s failed: %v", addr, err)
		_ = ws.writeText("error " + err.Error())
		return
	}
	defer tcp.Close()
	if tc, ok := tcp.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	log.Printf("companion %s connected", addr)
	if err := ws.writeText("ready"); err != nil {
		return
	}

	done := make(chan struct{}, 2)

	// companion TCP -> browser (binary frames)
	go func() {
		buf := make([]byte, relayBufSize)
		for {
			n, rerr := tcp.Read(buf)
			if n > 0 {
				if werr := ws.writeBinary(buf[:n]); werr != nil {
					break
				}
			}
			if rerr != nil {
				break
			}
		}
		done <- struct{}{}
	}()

	// browser -> companion TCP (unwrap ws frames)
	go func() {
		for {
			opcode, payload, rerr := ws.readFrame()
			if rerr != nil {
				break
			}
			switch opcode {
			case opBinary, opContinuation:
				if _, werr := tcp.Write(payload); werr != nil {
					done <- struct{}{}
					return
				}
			case opPing:
				_ = ws.writeFrame(opPong, payload)
			case opClose:
				done <- struct{}{}
				return
			}
		}
		done <- struct{}{}
	}()

	<-done
	log.Printf("companion %s session ended", addr)
	// Clean WebSocket close handshake so the browser sees wasClean rather than
	// an abnormal 1006. The deferred Close() on ws + tcp then unblocks the
	// surviving goroutine.
	_ = ws.writeClose(1000)
}

// ---------------------------------------------------------------------------
// Minimal RFC 6455 server (no external deps). The bridge only needs binary +
// text + ping/pong + close; client->server frames are always masked.
// ---------------------------------------------------------------------------

type wsConn struct {
	conn net.Conn
	br   *bufio.Reader
	wmu  sync.Mutex
}

func upgrade(w http.ResponseWriter, r *http.Request) (*wsConn, error) {
	if !headerContainsToken(r.Header.Get("Connection"), "upgrade") ||
		!strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		return nil, errors.New("not a websocket upgrade request")
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		return nil, errors.New("missing Sec-WebSocket-Key")
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		return nil, errors.New("response writer does not support hijacking")
	}
	conn, brw, err := hj.Hijack()
	if err != nil {
		return nil, err
	}
	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + acceptKey(key) + "\r\n\r\n"
	if _, err := brw.WriteString(resp); err != nil {
		conn.Close()
		return nil, err
	}
	if err := brw.Flush(); err != nil {
		conn.Close()
		return nil, err
	}
	return &wsConn{conn: conn, br: brw.Reader}, nil
}

func acceptKey(key string) string {
	const magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
	h := sha1.New()
	h.Write([]byte(key + magic))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func headerContainsToken(header, token string) bool {
	for _, part := range strings.Split(header, ",") {
		if strings.EqualFold(strings.TrimSpace(part), token) {
			return true
		}
	}
	return false
}

func (c *wsConn) Close() error { return c.conn.Close() }

func (c *wsConn) readFrame() (byte, []byte, error) {
	var hdr [2]byte
	if _, err := io.ReadFull(c.br, hdr[:]); err != nil {
		return 0, nil, err
	}
	opcode := hdr[0] & 0x0f
	masked := hdr[1]&0x80 != 0
	length := uint64(hdr[1] & 0x7f)
	switch length {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(c.br, ext[:]); err != nil {
			return 0, nil, err
		}
		length = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(c.br, ext[:]); err != nil {
			return 0, nil, err
		}
		length = binary.BigEndian.Uint64(ext[:])
	}
	if length > maxFrame {
		return 0, nil, fmt.Errorf("frame too large: %d bytes", length)
	}
	var mask [4]byte
	if masked {
		if _, err := io.ReadFull(c.br, mask[:]); err != nil {
			return 0, nil, err
		}
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(c.br, payload); err != nil {
		return 0, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i&3]
		}
	}
	return opcode, payload, nil
}

func (c *wsConn) writeFrame(opcode byte, payload []byte) error {
	n := len(payload)
	var hdr []byte
	switch {
	case n < 126:
		hdr = []byte{0x80 | opcode, byte(n)}
	case n < 1<<16:
		hdr = []byte{0x80 | opcode, 126, byte(n >> 8), byte(n)}
	default:
		hdr = make([]byte, 10)
		hdr[0] = 0x80 | opcode
		hdr[1] = 127
		binary.BigEndian.PutUint64(hdr[2:], uint64(n))
	}
	frame := append(hdr, payload...)
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_, err := c.conn.Write(frame)
	return err
}

func (c *wsConn) writeBinary(p []byte) error { return c.writeFrame(opBinary, p) }
func (c *wsConn) writeText(s string) error   { return c.writeFrame(opText, []byte(s)) }

func (c *wsConn) writeClose(code uint16) error {
	var body [2]byte
	binary.BigEndian.PutUint16(body[:], code)
	return c.writeFrame(opClose, body[:])
}

// ---------------------------------------------------------------------------

func lanIPs() []string {
	var out []string
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return out
	}
	for _, a := range addrs {
		ipnet, ok := a.(*net.IPNet)
		if !ok || ipnet.IP.IsLoopback() {
			continue
		}
		ip4 := ipnet.IP.To4()
		if ip4 != nil {
			out = append(out, ip4.String())
		}
	}
	return out
}

func openBrowser(url string) {
	// Deliberately avoid rundll32/cmd here: those are "LOLBins" abused by real
	// malware, so an unsigned binary that references them scores badly with
	// antivirus ML heuristics. "explorer" is an ordinary Windows program and
	// opens the default browser just as well.
	var cmd string
	var args []string
	switch runtime.GOOS {
	case "windows":
		cmd = "explorer"
		args = []string{url}
	case "darwin":
		cmd = "open"
		args = []string{url}
	default:
		cmd = "xdg-open"
		args = []string{url}
	}
	// explorer.exe returns a non-zero exit code even on success; Start() does
	// not wait for it, so that quirk is harmless here.
	if err := exec.Command(cmd, args...).Start(); err != nil {
		fmt.Fprintf(os.Stderr, "ouvrez manuellement %s\n", url)
	}
}
