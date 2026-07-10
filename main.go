package main

import (
    "database/sql"
    "flag"
    "log"
    "net/http"
    "os"
    "time"

    "github.com/gin-gonic/gin"
    "github.com/golang-jwt/jwt/v5"
    "github.com/gorilla/websocket"
    _ "github.com/mattn/go-sqlite3"
    "golang.org/x/crypto/bcrypt"
)

var jwtSecret = []byte("MS_CHAT_SUPER_SECRET_KEY_2026")

type Message struct {
    Username  string `json:"username"`
    Text      string `json:"text"`
    RoomId    string `json:"room_id"`
    IsHistory bool   `json:"is_history"`
}

type AuthRequest struct {
    Username string `json:"username" binding:"required"`
    Password string `json:"password" binding:"required"`
}

type RoomRequest struct {
    RoomId string `json:"room_id" binding:"required"`
}

var addr = flag.String("addr", ":40003", "http service address")

var upgrader = websocket.Upgrader{
    ReadBufferSize:  4096,
    WriteBufferSize: 4096,
    CheckOrigin: func(r *http.Request) bool {
        return true
    },
}

func generateToken(username string) (string, error) {
    token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
        "username": username,
        "exp":      time.Now().Add(time.Hour * 24).Unix(),
    })
    return token.SignedString(jwtSecret)
}

func validateToken(tokenStr string) (string, bool) {
    token, err := jwt.Parse(tokenStr, func(token *jwt.Token) (interface{}, error) {
        return jwtSecret, nil
    })

    if err != nil || !token.Valid {
        return "", false
    }

    claims, ok := token.Claims.(jwt.MapClaims)
    if !ok {
        return "", false
    }

    username, ok := claims["username"].(string)
    return username, ok
}

func AuthMiddleware(db *sql.DB) gin.HandlerFunc {
    return func(c *gin.Context) {
        authHeader := c.GetHeader("Authorization")
        if authHeader == "" {
            c.JSON(http.StatusUnauthorized, gin.H{"error": "Butuh otentikasi token"})
            c.Abort()
            return
        }

        tokenStr := authHeader
        if len(authHeader) > 7 && authHeader[:7] == "Bearer " {
            tokenStr = authHeader[7:]
        }

        username, valid := validateToken(tokenStr)
        if !valid {
            c.JSON(http.StatusUnauthorized, gin.H{"error": "Token tidak valid atau kedaluwarsa"})
            c.Abort()
            return
        }

        var exists string
        err := db.QueryRow("SELECT username FROM users WHERE username = ?", username).Scan(&exists)
        if err != nil {
            c.JSON(http.StatusUnauthorized, gin.H{"error": "User tidak terdaftar, silakan login ulang"})
            c.Abort()
            return
        }

        c.Set("username", username)
        c.Next()
    }
}

func serveWs(hub *Hub, db *sql.DB, c *gin.Context) {
    token := c.Query("token")
    roomId := c.Query("room")

    username, valid := validateToken(token)
    if !valid || username == "" {
        c.JSON(http.StatusUnauthorized, gin.H{"error": "Token tidak valid!"})
        return
    }

    var exists string
    err := db.QueryRow("SELECT username FROM users WHERE username = ?", username).Scan(&exists)
    if err != nil {
        c.JSON(http.StatusUnauthorized, gin.H{"error": "User tidak valid!"})
        return
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

    _ = os.MkdirAll("./uploads", os.ModePerm)

    db, err := sql.Open("sqlite3", "./chat.db")
    if err != nil {
        log.Fatal("Gagal membuka database:", err)
    }
    defer db.Close()

    query := `
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL UNIQUE,
        password TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS chat_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        room_id TEXT NOT NULL,
        username TEXT NOT NULL,
        message TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS user_rooms (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL,
        room_id TEXT NOT NULL,
        UNIQUE(username, room_id)
    );`
    if _, err := db.Exec(query); err != nil {
        log.Fatal("Gagal membuat tabel database:", err)
    }

    hub := NewHub(db)
    go hub.Run()

    r := gin.Default()
    r.MaxMultipartMemory = 32 << 20

    r.GET("/favicon.ico", func(c *gin.Context) {
        c.Status(http.StatusNoContent)
    })

    r.POST("/api/register", func(c *gin.Context) {
        var req AuthRequest
        if err := c.ShouldBindJSON(&req); err != nil {
            c.JSON(http.StatusBadRequest, gin.H{"error": "Input tidak valid"})
            return
        }

        hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
        if err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal memproses password"})
            return
        }

        tx, err := db.Begin()
        if err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
            return
        }

        _, err = tx.Exec("INSERT INTO users (username, password) VALUES (?, ?)", req.Username, string(hashedPassword))
        if err != nil {
            tx.Rollback()
            c.JSON(http.StatusConflict, gin.H{"error": "Username sudah terdaftar!"})
            return
        }

        _, _ = tx.Exec("INSERT INTO user_rooms (username, room_id) VALUES (?, 'general')", req.Username)

        if err := tx.Commit(); err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal menyimpan data"})
            return
        }

        token, err := generateToken(req.Username)
        if err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": "Registrasi sukses, tetapi gagal membuat sesi login"})
            return
        }

        c.JSON(http.StatusOK, gin.H{
            "message":  "Registrasi dan login berhasil",
            "username": req.Username,
            "token":    token,
        })
    })

    r.POST("/api/login", func(c *gin.Context) {
        var req AuthRequest
        if err := c.ShouldBindJSON(&req); err != nil {
            c.JSON(http.StatusBadRequest, gin.H{"error": "Input tidak valid"})
            return
        }

        var dbPassword string
        err := db.QueryRow("SELECT password FROM users WHERE username = ?", req.Username).Scan(&dbPassword)
        if err != nil {
            c.JSON(http.StatusUnauthorized, gin.H{"error": "Username atau password salah"})
            return
        }

        err = bcrypt.CompareHashAndPassword([]byte(dbPassword), []byte(req.Password))
        if err != nil {
            c.JSON(http.StatusUnauthorized, gin.H{"error": "Username atau password salah"})
            return
        }

        token, err := generateToken(req.Username)
        if err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal membuat sesi login"})
            return
        }

        c.JSON(http.StatusOK, gin.H{
            "message":  "Login berhasil",
            "username": req.Username,
            "token":    token,
        })
    })

    authorized := r.Group("/api")
    authorized.Use(AuthMiddleware(db))
    {
        authorized.GET("/rooms", func(c *gin.Context) {
            username := c.MustGet("username").(string)

            rows, err := db.Query("SELECT room_id FROM user_rooms WHERE username = ?", username)
            if err != nil {
                c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal memuat room"})
                return
            }
            defer rows.Close()

            rooms := []string{}
            for rows.Next() {
                var rId string
                if err := rows.Scan(&rId); err == nil {
                    rooms = append(rooms, rId)
                }
            }
            c.JSON(http.StatusOK, gin.H{"rooms": rooms})
        })

        authorized.POST("/rooms/join", func(c *gin.Context) {
            username := c.MustGet("username").(string)
            var req RoomRequest
            if err := c.ShouldBindJSON(&req); err != nil {
                c.JSON(http.StatusBadRequest, gin.H{"error": "ID Room kosong"})
                return
            }

            _, _ = db.Exec("INSERT OR IGNORE INTO user_rooms (username, room_id) VALUES (?, ?)", username, req.RoomId)
            c.JSON(http.StatusOK, gin.H{"message": "Berhasil join room"})
        })

        authorized.DELETE("/rooms/leave", func(c *gin.Context) {
            username := c.MustGet("username").(string)
            var req RoomRequest
            if err := c.ShouldBindJSON(&req); err != nil {
                c.JSON(http.StatusBadRequest, gin.H{"error": "ID Room kosong"})
                return
            }

            _, _ = db.Exec("DELETE FROM user_rooms WHERE username = ? AND room_id = ?", username, req.RoomId)
            c.JSON(http.StatusOK, gin.H{"message": "Berhasil meninggalkan room"})
        })

        authorized.POST("/upload", func(c *gin.Context) {
            file, err := c.FormFile("file")
            if err != nil {
                c.JSON(http.StatusBadRequest, gin.H{"error": "Gagal menerima file"})
                return
            }

            uniqueFileName := time.Now().Format("20060102150405") + "_" + file.Filename
            filePath := "./uploads/" + uniqueFileName

            if err := c.SaveUploadedFile(file, filePath); err != nil {
                c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal menyimpan file di server"})
                return
            }

            c.JSON(http.StatusOK, gin.H{
                "url":       "/uploads/" + uniqueFileName,
                "filename":  file.Filename,
                "file_type": file.Header.Get("Content-Type"),
            })
        })
    }

    r.StaticFS("/static", http.Dir("./static"))
    r.Static("/uploads", "./uploads")
    
    r.GET("/", func(c *gin.Context) {
        c.File("./static/index.html")
    })

    r.GET("/ws", func(c *gin.Context) {
        serveWs(hub, db, c)
    })

    log.Printf("Server starting on %s", *addr)
    if err := r.Run(*addr); err != nil {
        log.Fatal(err)
    }
}