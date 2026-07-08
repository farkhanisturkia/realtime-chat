package main

import (
	"database/sql"
	"flag"
	"log"
	"math/rand"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	_ "github.com/mattn/go-sqlite3"
)

type Message struct {
	Username string `json:"username"`
	Text     string `json:"text"`
	RoomId   string `json:"room_id"`
}

var addr = flag.String("addr", ":40003", "http service address")

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func serveWs(hub *Hub, c *gin.Context) {
	username := c.Query("username")
	roomId := c.Query("room")

	if username == "" {
		username = "Guest_" + strconv.Itoa(rand.Intn(10000))
	}
	if roomId == "" {
		roomId = "general"
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
		roomId:   roomId,
	}

	client.hub.register <- client

	go client.writePump()
	go client.readPump()
}

func main() {
	flag.Parse()

	db, err := sql.Open("sqlite3", "./chat.db")
	if err != nil {
		log.Fatal("Gagal membuka database:", err)
	}
	defer db.Close()

	query := `
	CREATE TABLE IF NOT EXISTS chat_history (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		room_id TEXT NOT NULL,
		username TEXT NOT NULL,
		message TEXT NOT NULL,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);`
	if _, err := db.Exec(query); err != nil {
		log.Fatal("Gagal membuat tabel:", err)
	}

	hub := NewHub(db)
	go hub.Run()

	r := gin.Default()

	r.StaticFS("/static", http.Dir("./static"))
	r.GET("/", func(c *gin.Context) {
		c.File("./static/index.html")
	})

	r.GET("/ws", func(c *gin.Context) {
		serveWs(hub, c)
	})

	log.Printf("Server starting on %s", *addr)
	if err := r.Run(*addr); err != nil {
		log.Fatal(err)
	}
}