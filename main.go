package main

import (
	"flag"
	"log"
	"math/rand"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

type Message struct {
    Username string `json:"username"`
    Text     string `json:"text"`
}

var addr = flag.String("addr", ":10000", "http service address")

var upgrader = websocket.Upgrader{
    ReadBufferSize:  1024,
    WriteBufferSize: 1024,
    CheckOrigin: func(r *http.Request) bool {
        return true // ← in production: verify origin!
    },
}

func serveWs(hub *Hub, c *gin.Context) {
    username := c.Query("username")
    if username == "" {
        username = "Guest_" + strconv.Itoa(rand.Intn(10000))
    }

    conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
    if err != nil {
        log.Println("upgrade:", err)
        return
    }

    client := &Client{
        hub:      hub,
        conn:     conn,
        send:     make(chan []byte, 256),
        username: username,
    }

    client.hub.register <- client

    go client.writePump()
    go client.readPump()
}

func main() {
    flag.Parse()

    hub := NewHub()
    go hub.Run()

    r := gin.Default()

    // Serve static HTML
    r.StaticFS("/static", http.Dir("./static"))
    r.GET("/", func(c *gin.Context) {
        c.File("./static/index.html")
    })

    // WebSocket endpoint
    r.GET("/ws", func(c *gin.Context) {
        serveWs(hub, c)
    })

    log.Printf("Server starting on %s", *addr)
    if err := r.Run(*addr); err != nil {
        log.Fatal(err)
    }
}
