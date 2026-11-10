
// --------------------------------------------------
// ELEMENTS
// --------------------------------------------------

// Get references to the HTML elements we need.
//
// We store these in variables so we don't have to
// repeatedly search the document.
const messages = document.getElementById("messages");
const form = document.getElementById("message-form");
const input = document.getElementById("message-input");
const status = document.getElementById("status");


// --------------------------------------------------
// USERNAME
// --------------------------------------------------

// Ask the user for a username.
//
// In a real application, you'd have a proper login
// system instead of using prompt().
let username = prompt("Choose a username:");

// Give the user a default username if they leave it empty.
if (!username || !username.trim()) {
    username = "Anonymous";
}

// Keep the username within the server's 20-character limit.
username = username.trim().slice(0, 20);


// --------------------------------------------------
// WEBSOCKET CONNECTION
// --------------------------------------------------

// Choose the correct WebSocket protocol.
//
// HTTP websites use ws://
// HTTPS websites use wss://
//
// Using the current page's host means we don't have
// to hardcode localhost.
const protocol = location.protocol === "https:" ? "wss:" : "ws:";

// Construct the WebSocket URL.
//
// encodeURIComponent ensures special characters in
// usernames don't break the URL.
const socket = new WebSocket(
    `${protocol}//${location.host}/ws?username=${encodeURIComponent(username)}`
);


// --------------------------------------------------
// CONNECTION EVENTS
// --------------------------------------------------

// This runs when the connection is successfully opened.
socket.addEventListener("open", () => {
    status.textContent = "Connected";
});

// This runs if the connection closes.
socket.addEventListener("close", () => {
    status.textContent = "Disconnected";
});

// This runs if a WebSocket error occurs.
socket.addEventListener("error", (error) => {
    console.error("WebSocket error:", error);
    status.textContent = "Connection error";
});


// --------------------------------------------------
// RECEIVING MESSAGES
// --------------------------------------------------

// The server sends messages to us as JSON strings.
//
// Every time a message arrives, this event runs.
socket.addEventListener("message", (event) => {

    // Convert the JSON string into a JavaScript object.
    //
    // For example:
    // {"type":"chat","username":"Dave","text":"Hello"}
    //
    // Becomes:
    // { type: "chat", username: "Dave", text: "Hello" }
    const message = JSON.parse(event.data);

    // System messages are treated differently.
    if (message.type === "system") {
        addSystemMessage(message.text);
        return;
    }

    // Normal chat messages.
    if (message.type === "chat") {
        addChatMessage(message);
    }
});


// --------------------------------------------------
// DISPLAYING MESSAGES
// --------------------------------------------------

// Display a normal chat message.
function addChatMessage(message) {

    // Create a container for the message.
    const container = document.createElement("div");
    container.classList.add("message");

    // Create the username element.
    const usernameElement = document.createElement("span");
    usernameElement.classList.add("username");

    // Use textContent rather than innerHTML.
    //
    // This prevents users from injecting HTML or
    // JavaScript into other people's browsers.
    usernameElement.textContent = message.username;

    // Create the timestamp.
    const timeElement = document.createElement("span");
    timeElement.classList.add("time");
    timeElement.textContent = message.time;

    // Create the actual message text.
    const textElement = document.createElement("p");
    textElement.classList.add("text");
    textElement.textContent = message.text;

    // Put everything inside our container.
    container.appendChild(usernameElement);
    container.appendChild(timeElement);
    container.appendChild(textElement);

    // Add the message to the chat.
    messages.appendChild(container);

    // Scroll to the newest message.
    messages.scrollTop = messages.scrollHeight;
}


// Display system messages such as joining and leaving.
function addSystemMessage(text) {

    const element = document.createElement("p");

    element.classList.add("system-message");
    element.textContent = text;

    messages.appendChild(element);

    // Scroll to the bottom.
    messages.scrollTop = messages.scrollHeight;
}


// --------------------------------------------------
// SENDING MESSAGES
// --------------------------------------------------

// Listen for the form being submitted.
form.addEventListener("submit", (event) => {

    // Stop the browser from refreshing the page.
    event.preventDefault();

    // Get the message and remove surrounding whitespace.
    const text = input.value.trim();

    // Don't send empty messages.
    if (!text) {
        return;
    }

    // Only send messages if the WebSocket is connected.
    if (socket.readyState !== WebSocket.OPEN) {
        return;
    }

    // Send the message to the Go server.
    //
    // The server expects a plain text WebSocket message,
    // not JSON, because our Go readPump treats the
    // incoming message as the message text itself.
    socket.send(text);

    // Clear the input.
    input.value = "";

    // Put the cursor back in the input.
    input.focus();
});
