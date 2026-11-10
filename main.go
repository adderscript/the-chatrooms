package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// --------------------------------------------------
// MESSAGE
// --------------------------------------------------

// Message represents a single chat message.
//
// We use JSON to transfer messages between the Go
// server and JavaScript in the browser.
//
// The backticks after each field are struct tags.
// They tell Go how each field should be named in JSON.
type Message struct {
	Type     string `json:"type"`
	Username string `json:"username"`
	Text     string `json:"text"`
	Time     string `json:"time"`
}

// createMessage constructs a new message.
//
// Having a function for this means we don't have to
// manually set the time every time we create a message.
func createMessage(messageType, username, text string) []byte {
	msg := Message{
		Type:     messageType,
		Username: username,
		Text:     text,
		Time:     time.Now().Format("15:04"),
	}

	// Convert our Go struct into JSON bytes.
	//
	// For example:
	// Message{Type: "chat", Username: "Dave", Text: "Hello"}
	//
	// Becomes:
	// {"type":"chat","username":"Dave","text":"Hello"}
	data, err := json.Marshal(msg)
	if err != nil {
		log.Println("Could not encode message:", err)
		return nil
	}

	return data
}

// --------------------------------------------------
// CLIENT
// --------------------------------------------------

// Client represents one connected user.
//
// Every user gets their own Client instance.
//
// The send channel is a queue of messages waiting
// to be delivered to this particular user's browser.
type Client struct {
	hub      *Hub
	conn     *websocket.Conn
	send     chan []byte
	username string
}

// readPump continuously reads messages from a client.
//
// Each client has one readPump goroutine and one
// writePump goroutine.
//
// This separation is important because Gorilla WebSocket
// allows one concurrent reader and one concurrent writer.
func (c *Client) readPump() {
	defer func() {
		// When the connection closes, unregister the user.
		c.hub.unregister <- c

		// Close the WebSocket connection.
		c.conn.Close()
	}()

	// Limit incoming messages to 512 bytes.
	c.conn.SetReadLimit(512)

	for {
		// Read the next WebSocket message.
		messageType, message, err := c.conn.ReadMessage()
		if err != nil {
			// The connection was closed or an error occurred.
			break
		}

		// We only want normal text messages.
		if messageType != websocket.TextMessage {
			continue
		}

		// Remove whitespace from the beginning and end.
		text := strings.TrimSpace(string(message))

		// Ignore empty messages.
		if text == "" {
			continue
		}

		// Broadcast the message to everyone.
		data := createMessage("chat", c.username, text)

		if data != nil {
			c.hub.broadcast <- data
		}
	}
}

// writePump continuously sends messages to the client.
//
// Instead of every part of our application writing
// directly to the WebSocket, they put messages into
// the client's send channel.
//
// This goroutine is responsible for actually sending them.
func (c *Client) writePump() {
	defer c.conn.Close()

	for {
		// Wait until a message is available.
		message, ok := <-c.send

		// If the channel was closed, the client has
		// been disconnected.
		if !ok {
			c.conn.WriteMessage(
				websocket.CloseMessage,
				[]byte{},
			)
			return
		}

		// Send the message through the WebSocket.
		err := c.conn.WriteMessage(
			websocket.TextMessage,
			message,
		)

		if err != nil {
			return
		}
	}
}

// --------------------------------------------------
// HUB
// --------------------------------------------------

// Hub manages all connected clients.
//
// Think of this as the central manager of the chat.
//
// It keeps track of who is connected and handles
// distributing messages to everyone.
type Hub struct {
	// A map containing every connected client.
	//
	// The empty struct uses no meaningful storage.
	// We only care whether a client exists in the map.
	clients map[*Client]bool

	// Channels are used to communicate with the Hub.
	//
	// register:   A new client wants to connect.
	// unregister: A client wants to disconnect.
	// broadcast:  A message needs to reach everyone.
	register   chan *Client
	unregister chan *Client
	broadcast  chan []byte
}

// newHub creates and initializes a Hub.
func newHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan []byte),
	}
}

func (h *Hub) broadcastMessage(message []byte) {
	// Iterate through every connected client.
	for client := range h.clients {

		// Try to queue the message for this client.
		select {
		case client.send <- message:
			// Successfully queued.

		default:
			// The client cannot keep up.
			// Remove it and close its send channel.
			delete(h.clients, client)
			close(client.send)
		}
	}
}

// run continuously processes Hub events.
//
// This function runs in its own goroutine.
//
// The select statement waits for activity on any
// of the channels and executes the matching case.
func (h *Hub) run() {
	for {
		select {

		case client := <-h.register:
			h.clients[client] = true

			// Call the helper directly rather than sending
			// back into our own broadcast channel.
			h.broadcastMessage(createMessage(
				"system",
				"",
				client.username+" joined the chat",
			))

		case client := <-h.unregister:
			if h.clients[client] {
				delete(h.clients, client)
				close(client.send)

				h.broadcastMessage(createMessage(
					"system",
					"",
					client.username+" left the chat",
				))
			}

		case message := <-h.broadcast:
			h.broadcastMessage(message)
		}
	}
}

// --------------------------------------------------
// WEBSOCKET
// --------------------------------------------------

// Upgrader converts a normal HTTP connection into
// a WebSocket connection.
var upgrader = websocket.Upgrader{

	// Allow connections from any origin for this
	// local learning project.
	//
	// IMPORTANT: For production, validate the Origin
	// against your own website instead.
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// serveWs handles requests to our WebSocket endpoint.
func serveWs(hub *Hub, w http.ResponseWriter, r *http.Request) {

	// Get the username from the URL.
	//
	// Example:
	// ws://localhost:8080/ws?username=Dave
	username := strings.TrimSpace(r.URL.Query().Get("username"))

	// Make sure the username is valid.
	if username == "" || len(username) > 20 {
		http.Error(w, "Invalid username", http.StatusBadRequest)
		return
	}

	// Upgrade the HTTP connection to WebSocket.
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("WebSocket upgrade error:", err)
		return
	}

	// Create a new client.
	client := &Client{
		hub:      hub,
		conn:     conn,
		send:     make(chan []byte, 256),
		username: username,
	}

	// Register this client with the Hub.
	hub.register <- client

	// Start the two goroutines.
	//
	// readPump reads incoming messages.
	// writePump sends outgoing messages.
	go client.writePump()
	go client.readPump()
}

// --------------------------------------------------
// MAIN
// --------------------------------------------------

func main() {

	// Create our Hub.
	hub := newHub()

	// Start the Hub in a separate goroutine.
	go hub.run()

	// Register our WebSocket endpoint.
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	})

	// Serve the files inside the static directory.
	//
	// This means:
	// localhost:8080/ loads static/index.html
	// localhost:8080/style.css loads static/style.css
	// localhost:8080/chat.js loads static/chat.js
	http.Handle("/", http.FileServer(http.Dir("./static")))

	log.Println("Server running at http://localhost:8080")

	// Start the HTTP server.
	//
	// ListenAndServe blocks until the server stops
	// or encounters an error.
	err := http.ListenAndServe(":8080", nil)

	if err != nil {
		log.Fatal(err)
	}
}
