package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"sync"
)

type Hub struct {
	rooms      map[string]map[*Client]bool
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
	db         *sql.DB
	mu         sync.RWMutex
}

func NewHub(db *sql.DB) *Hub {
	return &Hub{
		rooms:      make(map[string]map[*Client]bool),
		broadcast:  make(chan []byte, 256),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		db:         db,
	}
}

func (h *Hub) Run() {
    for {
        select {
        case client := <-h.register:
            h.mu.Lock()
            if h.rooms[client.roomId] == nil {
                h.rooms[client.roomId] = make(map[*Client]bool)
            }
            h.rooms[client.roomId][client] = true
            h.mu.Unlock()

        case client := <-h.unregister:
            h.mu.Lock()
            if clients, ok := h.rooms[client.roomId]; ok {
                if _, exists := clients[client]; exists {
                    delete(clients, client)
                    close(client.send)
                }
                if len(clients) == 0 {
                    delete(h.rooms, client.roomId)
                }
            }
            h.mu.Unlock()

        case messageBytes := <-h.broadcast:
            var msg Message
            if err := json.Unmarshal(messageBytes, &msg); err != nil {
                continue
            }

            _, err := h.db.Exec(
                "INSERT INTO chat_history (room_id, username, message) VALUES (?, ?, ?)",
                msg.RoomId, msg.Username, msg.Text,
            )
            if err != nil {
                log.Println("Gagal menyimpan chat ke SQLite:", err)
            }

            msg.IsHistory = false
            payloadBytes, _ := json.Marshal(msg)

            rows, err := h.db.Query("SELECT username FROM user_rooms WHERE room_id = ?", msg.RoomId)
            if err != nil {
                log.Println("Gagal mengambil data user_rooms:", err)
                continue
            }

            joinedUsers := make(map[string]bool)
            for rows.Next() {
                var u string
                if err := rows.Scan(&u); err == nil {
                    joinedUsers[u] = true
                }
            }
            rows.Close()

            h.mu.RLock()
            for _, clients := range h.rooms {
                for client := range clients {
                    if joinedUsers[client.username] {
                        select {
                        case client.send <- payloadBytes:
                        default:
                            close(client.send)
                            if activeClients, ok := h.rooms[client.roomId]; ok {
                                delete(activeClients, client)
                            }
                        }
                    }
                }
            }
            h.mu.RUnlock()
        }
    }
}