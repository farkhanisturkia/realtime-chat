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

			h.mu.RLock()
			if clients, ok := h.rooms[msg.RoomId]; ok {
				for client := range clients {
					select {
					case client.send <- messageBytes:
					default:
						close(client.send)
						delete(clients, client)
					}
				}
			}
			h.mu.RUnlock()
		}
	}
}