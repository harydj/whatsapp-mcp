package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/mdp/qrterminal"

	"bytes"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

// Message represents a chat message for our client
type Message struct {
	Time      time.Time
	Sender    string
	Content   string
	IsFromMe  bool
	MediaType string
	Filename  string
}

// Database handler for storing message history
type MessageStore struct {
	db *sql.DB
}

// HistorySyncTracker tracks on-demand history sync requests to prevent spam
type HistorySyncTracker struct {
	mu             sync.Mutex
	lastRequest    time.Time
	pendingRequest *HistorySyncRequest
	cooldown       time.Duration
}

type HistorySyncRequest struct {
	ChatJID         string
	AnchorMessageID string
	Count           int
	RequestTime     time.Time
}

var historySyncTracker = &HistorySyncTracker{
	cooldown: 30 * time.Second, // Minimum 30 seconds between requests
}

func databaseURL() string {
	if v := os.Getenv("WHATSAPP_DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://postgres:postgres@127.0.0.1:5432/whatsapp?sslmode=disable"
}

// Initialize message store
func NewMessageStore() (*MessageStore, error) {
	// Media files still live on disk under store/.
	if err := os.MkdirAll("store", 0755); err != nil {
		return nil, fmt.Errorf("failed to create store directory: %v", err)
	}

	db, err := sql.Open("postgres", databaseURL())
	if err != nil {
		return nil, fmt.Errorf("failed to open message database: %v", err)
	}
	db.SetMaxOpenConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping message database: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS chats (
			jid TEXT PRIMARY KEY,
			name TEXT,
			last_message_time TIMESTAMPTZ
		);

		CREATE TABLE IF NOT EXISTS messages (
			id TEXT,
			chat_jid TEXT,
			sender TEXT,
			content TEXT,
			timestamp TIMESTAMPTZ,
			is_from_me BOOLEAN,
			media_type TEXT,
			filename TEXT,
			url TEXT,
			media_key BYTEA,
			file_sha256 BYTEA,
			file_enc_sha256 BYTEA,
			file_length BIGINT,
			PRIMARY KEY (id, chat_jid),
			FOREIGN KEY (chat_jid) REFERENCES chats(jid)
		);
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create tables: %v", err)
	}

	return &MessageStore{db: db}, nil
}

// Close the database connection
func (store *MessageStore) Close() error {
	return store.db.Close()
}

// Store a chat in the database
func (store *MessageStore) StoreChat(jid, name string, lastMessageTime time.Time) error {
	_, err := store.db.Exec(
		`INSERT INTO chats (jid, name, last_message_time) VALUES ($1, $2, $3)
		ON CONFLICT (jid) DO UPDATE SET name = EXCLUDED.name, last_message_time = EXCLUDED.last_message_time`,
		jid, name, lastMessageTime,
	)
	return err
}

// Store a message in the database
func (store *MessageStore) StoreMessage(id, chatJID, sender, content string, timestamp time.Time, isFromMe bool,
	mediaType, filename, url string, mediaKey, fileSHA256, fileEncSHA256 []byte, fileLength uint64) error {
	// Only store if there's actual content or media
	if content == "" && mediaType == "" {
		return nil
	}

	_, err := store.db.Exec(
		`INSERT INTO messages
		(id, chat_jid, sender, content, timestamp, is_from_me, media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (id, chat_jid) DO UPDATE SET
			sender = EXCLUDED.sender,
			content = EXCLUDED.content,
			timestamp = EXCLUDED.timestamp,
			is_from_me = EXCLUDED.is_from_me,
			media_type = EXCLUDED.media_type,
			filename = EXCLUDED.filename,
			url = EXCLUDED.url,
			media_key = EXCLUDED.media_key,
			file_sha256 = EXCLUDED.file_sha256,
			file_enc_sha256 = EXCLUDED.file_enc_sha256,
			file_length = EXCLUDED.file_length`,
		id, chatJID, sender, content, timestamp, isFromMe, mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength,
	)
	return err
}

// Get messages from a chat
func (store *MessageStore) GetMessages(chatJID string, limit int) ([]Message, error) {
	rows, err := store.db.Query(
		"SELECT COALESCE(sender,''), COALESCE(content,''), timestamp, COALESCE(is_from_me,false), COALESCE(media_type,''), COALESCE(filename,'') FROM messages WHERE chat_jid = $1 ORDER BY timestamp DESC LIMIT $2",
		chatJID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var msg Message
		var timestamp time.Time
		err := rows.Scan(&msg.Sender, &msg.Content, &timestamp, &msg.IsFromMe, &msg.MediaType, &msg.Filename)
		if err != nil {
			return nil, err
		}
		msg.Time = timestamp
		messages = append(messages, msg)
	}

	return messages, nil
}

// Get all chats
func (store *MessageStore) GetChats() (map[string]time.Time, error) {
	rows, err := store.db.Query("SELECT jid, last_message_time FROM chats ORDER BY last_message_time DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	chats := make(map[string]time.Time)
	for rows.Next() {
		var jid string
		var lastMessageTime time.Time
		err := rows.Scan(&jid, &lastMessageTime)
		if err != nil {
			return nil, err
		}
		chats[jid] = lastMessageTime
	}

	return chats, nil
}

// AnchorInfo contains information needed to build a history sync anchor
type AnchorInfo struct {
	ID        string
	ChatJID   string
	Sender    string
	IsFromMe  bool
	Timestamp time.Time
}

// GetValidAnchorForChat finds a suitable anchor message for history sync.
// For group chats it prefers is_from_me messages, then messages with a valid participant sender.
// It rejects rows whose sender equals the group JID user part (corrupted history rows).
func (store *MessageStore) GetValidAnchorForChat(chatJID string, preferredAnchorID string) (*AnchorInfo, error) {
	isGroup := strings.HasSuffix(chatJID, "@g.us")
	groupUser := ""
	if isGroup {
		groupUser = strings.Split(chatJID, "@")[0]
	}

	if preferredAnchorID != "" {
		row := store.db.QueryRow(`
			SELECT id, chat_jid, COALESCE(sender,''), COALESCE(is_from_me,false), timestamp
			FROM messages
			WHERE chat_jid = $1 AND id = $2
		`, chatJID, preferredAnchorID)

		var anchor AnchorInfo
		err := row.Scan(&anchor.ID, &anchor.ChatJID, &anchor.Sender, &anchor.IsFromMe, &anchor.Timestamp)
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("anchor message %s not found in chat %s", preferredAnchorID, chatJID)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to query anchor: %v", err)
		}
		if isGroup && !anchor.IsFromMe && (anchor.Sender == groupUser || anchor.Sender == "") {
			return nil, fmt.Errorf("anchor message %s has corrupted sender (group ID); use a self-message or valid participant message", preferredAnchorID)
		}
		return &anchor, nil
	}

	var row *sql.Row
	if isGroup {
		row = store.db.QueryRow(`
			SELECT id, chat_jid, COALESCE(sender,''), COALESCE(is_from_me,false), timestamp
			FROM messages
			WHERE chat_jid = $1
			AND timestamp IS NOT NULL
			AND (is_from_me = true OR (COALESCE(sender,'') <> $2 AND COALESCE(sender,'') <> ''))
			ORDER BY is_from_me DESC, timestamp DESC
			LIMIT 1
		`, chatJID, groupUser)
	} else {
		row = store.db.QueryRow(`
			SELECT id, chat_jid, COALESCE(sender,''), COALESCE(is_from_me,false), timestamp
			FROM messages
			WHERE chat_jid = $1 AND timestamp IS NOT NULL
			ORDER BY timestamp DESC
			LIMIT 1
		`, chatJID)
	}

	var anchor AnchorInfo
	err := row.Scan(&anchor.ID, &anchor.ChatJID, &anchor.Sender, &anchor.IsFromMe, &anchor.Timestamp)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("no valid anchor message found in chat %s", chatJID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query anchor: %v", err)
	}
	return &anchor, nil
}

// Extract text content from a message
func extractTextContent(msg *waProto.Message) string {
	if msg == nil {
		return ""
	}

	// Plain / extended text
	if text := msg.GetConversation(); text != "" {
		return text
	}
	if extendedText := msg.GetExtendedTextMessage(); extendedText != nil {
		return extendedText.GetText()
	}

	// Captions on media (previously dropped → empty content for all images/docs)
	if img := msg.GetImageMessage(); img != nil {
		if cap := img.GetCaption(); cap != "" {
			return cap
		}
	}
	if vid := msg.GetVideoMessage(); vid != nil {
		if cap := vid.GetCaption(); cap != "" {
			return cap
		}
	}
	if doc := msg.GetDocumentMessage(); doc != nil {
		if cap := doc.GetCaption(); cap != "" {
			return cap
		}
	}
	if wrapped := msg.GetDocumentWithCaptionMessage(); wrapped != nil {
		if inner := wrapped.GetMessage(); inner != nil {
			if doc := inner.GetDocumentMessage(); doc != nil {
				if cap := doc.GetCaption(); cap != "" {
					return cap
				}
			}
			if text := extractTextContent(inner); text != "" {
				return text
			}
		}
	}
	if ephem := msg.GetEphemeralMessage(); ephem != nil {
		if inner := ephem.GetMessage(); inner != nil {
			return extractTextContent(inner)
		}
	}
	if viewOnce := msg.GetViewOnceMessage(); viewOnce != nil {
		if inner := viewOnce.GetMessage(); inner != nil {
			return extractTextContent(inner)
		}
	}
	if viewOnceV2 := msg.GetViewOnceMessageV2(); viewOnceV2 != nil {
		if inner := viewOnceV2.GetMessage(); inner != nil {
			return extractTextContent(inner)
		}
	}

	return ""
}

// SendMessageResponse represents the response for the send message API
type SendMessageResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// SendMessageRequest represents the request body for the send message API
type SendMessageRequest struct {
	Recipient string `json:"recipient"`
	Message   string `json:"message"`
	MediaPath string `json:"media_path,omitempty"`
}

// Function to send a WhatsApp message
func sendWhatsAppMessage(client *whatsmeow.Client, recipient string, message string, mediaPath string) (bool, string) {
	if !client.IsConnected() {
		return false, "Not connected to WhatsApp"
	}

	// Create JID for recipient
	var recipientJID types.JID
	var err error

	// Check if recipient is a JID
	isJID := strings.Contains(recipient, "@")

	if isJID {
		// Parse the JID string
		recipientJID, err = types.ParseJID(recipient)
		if err != nil {
			return false, fmt.Sprintf("Error parsing JID: %v", err)
		}
	} else {
		// Create JID from phone number
		recipientJID = types.JID{
			User:   recipient,
			Server: "s.whatsapp.net", // For personal chats
		}
	}

	msg := &waProto.Message{}

	// Check if we have media to send
	if mediaPath != "" {
		// Read media file
		mediaData, err := os.ReadFile(mediaPath)
		if err != nil {
			return false, fmt.Sprintf("Error reading media file: %v", err)
		}

		// Determine media type and mime type based on file extension
		fileExt := strings.ToLower(mediaPath[strings.LastIndex(mediaPath, ".")+1:])
		var mediaType whatsmeow.MediaType
		var mimeType string

		// Handle different media types
		switch fileExt {
		// Image types
		case "jpg", "jpeg":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/jpeg"
		case "png":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/png"
		case "gif":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/gif"
		case "webp":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/webp"

		// Audio types
		case "ogg":
			mediaType = whatsmeow.MediaAudio
			mimeType = "audio/ogg; codecs=opus"

		// Video types
		case "mp4":
			mediaType = whatsmeow.MediaVideo
			mimeType = "video/mp4"
		case "avi":
			mediaType = whatsmeow.MediaVideo
			mimeType = "video/avi"
		case "mov":
			mediaType = whatsmeow.MediaVideo
			mimeType = "video/quicktime"

		// Document types (for any other file type)
		default:
			mediaType = whatsmeow.MediaDocument
			mimeType = "application/octet-stream"
		}

		// Upload media to WhatsApp servers
		resp, err := client.Upload(context.Background(), mediaData, mediaType)
		if err != nil {
			return false, fmt.Sprintf("Error uploading media: %v", err)
		}

		fmt.Println("Media uploaded", resp)

		// Create the appropriate message type based on media type
		switch mediaType {
		case whatsmeow.MediaImage:
			msg.ImageMessage = &waProto.ImageMessage{
				Caption:       proto.String(message),
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
			}
		case whatsmeow.MediaAudio:
			// Handle ogg audio files
			var seconds uint32 = 30 // Default fallback
			var waveform []byte = nil

			// Try to analyze the ogg file
			if strings.Contains(mimeType, "ogg") {
				analyzedSeconds, analyzedWaveform, err := analyzeOggOpus(mediaData)
				if err == nil {
					seconds = analyzedSeconds
					waveform = analyzedWaveform
				} else {
					return false, fmt.Sprintf("Failed to analyze Ogg Opus file: %v", err)
				}
			} else {
				fmt.Printf("Not an Ogg Opus file: %s\n", mimeType)
			}

			msg.AudioMessage = &waProto.AudioMessage{
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
				Seconds:       proto.Uint32(seconds),
				PTT:           proto.Bool(true),
				Waveform:      waveform,
			}
		case whatsmeow.MediaVideo:
			msg.VideoMessage = &waProto.VideoMessage{
				Caption:       proto.String(message),
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
			}
		case whatsmeow.MediaDocument:
			msg.DocumentMessage = &waProto.DocumentMessage{
				Title:         proto.String(mediaPath[strings.LastIndex(mediaPath, "/")+1:]),
				Caption:       proto.String(message),
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
			}
		}
	} else {
		msg.Conversation = proto.String(message)
	}

	// Send message
	_, err = client.SendMessage(context.Background(), recipientJID, msg)

	if err != nil {
		return false, fmt.Sprintf("Error sending message: %v", err)
	}

	return true, fmt.Sprintf("Message sent to %s", recipient)
}

// Extract media info from a message
func extractMediaInfo(msg *waProto.Message) (mediaType string, filename string, url string, mediaKey []byte, fileSHA256 []byte, fileEncSHA256 []byte, fileLength uint64) {
	if msg == nil {
		return "", "", "", nil, nil, nil, 0
	}

	// Unwrap common containers first
	if ephem := msg.GetEphemeralMessage(); ephem != nil {
		if inner := ephem.GetMessage(); inner != nil {
			return extractMediaInfo(inner)
		}
	}
	if viewOnce := msg.GetViewOnceMessage(); viewOnce != nil {
		if inner := viewOnce.GetMessage(); inner != nil {
			return extractMediaInfo(inner)
		}
	}
	if viewOnceV2 := msg.GetViewOnceMessageV2(); viewOnceV2 != nil {
		if inner := viewOnceV2.GetMessage(); inner != nil {
			return extractMediaInfo(inner)
		}
	}
	if wrapped := msg.GetDocumentWithCaptionMessage(); wrapped != nil {
		if inner := wrapped.GetMessage(); inner != nil {
			return extractMediaInfo(inner)
		}
	}

	stamp := time.Now().Format("20060102_150405")

	// Prefer CDN URL (keeps signed oh/oe query). Bare DirectPath without
	// query makes whatsmeow build an unsigned CDN request → HTTP 403.
	pickLocator := func(directPath, cdnURL string) string {
		if cdnURL != "" {
			return cdnURL
		}
		return directPath
	}

	// Check for image message
	if img := msg.GetImageMessage(); img != nil {
		return "image", "image_" + stamp + ".jpg",
			pickLocator(img.GetDirectPath(), img.GetURL()), img.GetMediaKey(), img.GetFileSHA256(), img.GetFileEncSHA256(), img.GetFileLength()
	}

	// Check for video message
	if vid := msg.GetVideoMessage(); vid != nil {
		return "video", "video_" + stamp + ".mp4",
			pickLocator(vid.GetDirectPath(), vid.GetURL()), vid.GetMediaKey(), vid.GetFileSHA256(), vid.GetFileEncSHA256(), vid.GetFileLength()
	}

	// Check for audio message
	if aud := msg.GetAudioMessage(); aud != nil {
		return "audio", "audio_" + stamp + ".ogg",
			pickLocator(aud.GetDirectPath(), aud.GetURL()), aud.GetMediaKey(), aud.GetFileSHA256(), aud.GetFileEncSHA256(), aud.GetFileLength()
	}

	// Check for sticker message
	if st := msg.GetStickerMessage(); st != nil {
		return "sticker", "sticker_" + stamp + ".webp",
			pickLocator(st.GetDirectPath(), st.GetURL()), st.GetMediaKey(), st.GetFileSHA256(), st.GetFileEncSHA256(), st.GetFileLength()
	}

	// Check for document message
	if doc := msg.GetDocumentMessage(); doc != nil {
		filename := doc.GetFileName()
		if filename == "" {
			filename = "document_" + stamp
		}
		return "document", filename,
			pickLocator(doc.GetDirectPath(), doc.GetURL()), doc.GetMediaKey(), doc.GetFileSHA256(), doc.GetFileEncSHA256(), doc.GetFileLength()
	}

	return "", "", "", nil, nil, nil, 0
}

// Handle regular incoming messages with media support
func handleMessage(client *whatsmeow.Client, messageStore *MessageStore, msg *events.Message, logger waLog.Logger) {
	// Save message to database
	chatJID := msg.Info.Chat.String()
	sender := msg.Info.Sender.User

	// Get appropriate chat name (pass nil for conversation since we don't have one for regular messages)
	name := GetChatName(client, messageStore, msg.Info.Chat, chatJID, nil, sender, logger)

	// Update chat in database with the message timestamp (keeps last message time updated)
	err := messageStore.StoreChat(chatJID, name, msg.Info.Timestamp)
	if err != nil {
		logger.Warnf("Failed to store chat: %v", err)
	}

	// Extract text content
	content := extractTextContent(msg.Message)

	// Extract media info
	mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength := extractMediaInfo(msg.Message)

	// Skip if there's no content and no media
	if content == "" && mediaType == "" {
		return
	}

	// Keep filenames unique per message so downloads do not overwrite
	if mediaType != "" && filename != "" && !strings.HasPrefix(filename, msg.Info.ID+"_") {
		filename = msg.Info.ID + "_" + filename
	}

	// Store message in database
	err = messageStore.StoreMessage(
		msg.Info.ID,
		chatJID,
		sender,
		content,
		msg.Info.Timestamp,
		msg.Info.IsFromMe,
		mediaType,
		filename,
		url,
		mediaKey,
		fileSHA256,
		fileEncSHA256,
		fileLength,
	)

	if err != nil {
		logger.Warnf("Failed to store message: %v", err)
	} else {
		// Log message reception
		timestamp := msg.Info.Timestamp.Format("2006-01-02 15:04:05")
		direction := "â†"
		if msg.Info.IsFromMe {
			direction = "â†’"
		}

		// Log based on message type
		if mediaType != "" {
			fmt.Printf("[%s] %s %s: [%s: %s] %s\n", timestamp, direction, sender, mediaType, filename, content)
		} else if content != "" {
			fmt.Printf("[%s] %s %s: %s\n", timestamp, direction, sender, content)
		}
	}
}

// DownloadMediaRequest represents the request body for the download media API
type DownloadMediaRequest struct {
	MessageID string `json:"message_id"`
	ChatJID   string `json:"chat_jid"`
}

// DownloadMediaResponse represents the response for the download media API
type DownloadMediaResponse struct {
	Success   bool   `json:"success"`
	Message   string `json:"message"`
	Filename  string `json:"filename,omitempty"`
	Path      string `json:"path,omitempty"`
	MediaType string `json:"media_type,omitempty"`
}

// Store additional media info in the database
func (store *MessageStore) StoreMediaInfo(id, chatJID, url string, mediaKey, fileSHA256, fileEncSHA256 []byte, fileLength uint64) error {
	_, err := store.db.Exec(
		"UPDATE messages SET url = $1, media_key = $2, file_sha256 = $3, file_enc_sha256 = $4, file_length = $5 WHERE id = $6 AND chat_jid = $7",
		url, mediaKey, fileSHA256, fileEncSHA256, fileLength, id, chatJID,
	)
	return err
}

// Get media info from the database
func (store *MessageStore) GetMediaInfo(id, chatJID string) (string, string, string, []byte, []byte, []byte, uint64, error) {
	var mediaType, filename, url string
	var mediaKey, fileSHA256, fileEncSHA256 []byte
	var fileLength uint64

	err := store.db.QueryRow(
		"SELECT COALESCE(media_type,''), COALESCE(filename,''), COALESCE(url,''), media_key, file_sha256, file_enc_sha256, COALESCE(file_length,0) FROM messages WHERE id = $1 AND chat_jid = $2",
		id, chatJID,
	).Scan(&mediaType, &filename, &url, &mediaKey, &fileSHA256, &fileEncSHA256, &fileLength)

	return mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength, err
}

// MediaDownloader implements the whatsmeow.DownloadableMessage interface
type MediaDownloader struct {
	URL           string
	DirectPath    string
	MediaKey      []byte
	FileLength    uint64
	FileSHA256    []byte
	FileEncSHA256 []byte
	MediaType     whatsmeow.MediaType
}

// GetDirectPath implements the DownloadableMessage interface
func (d *MediaDownloader) GetDirectPath() string {
	return d.DirectPath
}

// GetURL implements the DownloadableMessage interface
func (d *MediaDownloader) GetURL() string {
	return d.URL
}

// GetMediaKey implements the DownloadableMessage interface
func (d *MediaDownloader) GetMediaKey() []byte {
	return d.MediaKey
}

// GetFileLength implements the DownloadableMessage interface
func (d *MediaDownloader) GetFileLength() uint64 {
	return d.FileLength
}

// GetFileSHA256 implements the DownloadableMessage interface
func (d *MediaDownloader) GetFileSHA256() []byte {
	return d.FileSHA256
}

// GetFileEncSHA256 implements the DownloadableMessage interface
func (d *MediaDownloader) GetFileEncSHA256() []byte {
	return d.FileEncSHA256
}

// GetMediaType implements the DownloadableMessage interface
func (d *MediaDownloader) GetMediaType() whatsmeow.MediaType {
	return d.MediaType
}

// Function to download media from a message
func downloadMedia(client *whatsmeow.Client, messageStore *MessageStore, messageID, chatJID string) (bool, string, string, string, error) {
	// Query the database for the message
	var mediaType, filename, url string
	var mediaKey, fileSHA256, fileEncSHA256 []byte
	var fileLength uint64
	var err error

	// First, check if we already have this file
	chatDir := fmt.Sprintf("store/%s", strings.ReplaceAll(chatJID, ":", "_"))
	localPath := ""

	// Get media info from the database
	mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength, err = messageStore.GetMediaInfo(messageID, chatJID)

	if err != nil {
		// Try to get basic info if extended info isn't available
		err = messageStore.db.QueryRow(
			"SELECT COALESCE(media_type,''), COALESCE(filename,'') FROM messages WHERE id = $1 AND chat_jid = $2",
			messageID, chatJID,
		).Scan(&mediaType, &filename)

		if err != nil {
			return false, "", "", "", fmt.Errorf("failed to find message: %v", err)
		}
	}

	// Check if this is a media message
	if mediaType == "" {
		return false, "", "", "", fmt.Errorf("not a media message")
	}

	// Create directory for the chat if it doesn't exist
	if err := os.MkdirAll(chatDir, 0755); err != nil {
		return false, "", "", "", fmt.Errorf("failed to create chat directory: %v", err)
	}

	// Generate a local path for the file
	localPath = fmt.Sprintf("%s/%s", chatDir, filename)

	// Get absolute path
	absPath, err := filepath.Abs(localPath)
	if err != nil {
		return false, "", "", "", fmt.Errorf("failed to get absolute path: %v", err)
	}

	// Check if file already exists
	if _, err := os.Stat(localPath); err == nil {
		// File exists, return it
		return true, mediaType, filename, absPath, nil
	}

	// If we don't have all the media info we need, we can't download
	if url == "" || len(mediaKey) == 0 || len(fileSHA256) == 0 || len(fileEncSHA256) == 0 || fileLength == 0 {
		return false, "", "", "", fmt.Errorf("incomplete media information for download (need locator+keys+length)")
	}

	fmt.Printf("Attempting to download media for message %s in chat %s...\n", messageID, chatJID)

	// whatsmeow rebuilds https://<media-host><directPath>&hash=...&mms-type=...
	// so directPath MUST keep the signed query (?ccb=&oh=&oe=...).
	directPath := extractDirectPathFromURL(url)
	if directPath == "" {
		return false, "", "", "", fmt.Errorf("could not derive media direct path from stored locator")
	}
	if !strings.Contains(directPath, "?") {
		fmt.Printf("Warning: media locator for %s has no signed query; CDN may return 403\n", messageID)
	}

	// Create a downloader that implements DownloadableMessage
	var waMediaType whatsmeow.MediaType
	switch mediaType {
	case "image":
		waMediaType = whatsmeow.MediaImage
	case "video":
		waMediaType = whatsmeow.MediaVideo
	case "audio":
		waMediaType = whatsmeow.MediaAudio
	case "document":
		waMediaType = whatsmeow.MediaDocument
	case "sticker":
		waMediaType = whatsmeow.MediaImage
	default:
		return false, "", "", "", fmt.Errorf("unsupported media type: %s", mediaType)
	}

	dl := &MediaDownloader{
		URL:           url,
		DirectPath:    directPath,
		MediaKey:      mediaKey,
		FileLength:    fileLength,
		FileSHA256:    fileSHA256,
		FileEncSHA256: fileEncSHA256,
		MediaType:     waMediaType,
	}
	mediaData, err := client.Download(context.Background(), dl)
	if err != nil || mediaData == nil {
		return false, "", "", "", fmt.Errorf("failed to download media: %v", err)
	}

	// Save the downloaded media to file
	if err := os.WriteFile(localPath, mediaData, 0644); err != nil {
		return false, "", "", "", fmt.Errorf("failed to save media file: %v", err)
	}

	fmt.Printf("Successfully downloaded %s media to %s (%d bytes)\n", mediaType, absPath, len(mediaData))
	return true, mediaType, filename, absPath, nil
}

// Extract direct path from a WhatsApp media URL.
// Keep the signed query string (ccb/oh/oe/_nc_sid/mms3). whatsmeow appends
// &hash=... with an ampersand, so stripping "?" yields unsigned 403s.
func extractDirectPathFromURL(url string) string {
	if url == "" {
		return ""
	}
	u := strings.TrimSpace(url)
	// Already a path (optionally signed)
	if strings.HasPrefix(u, "/") {
		u = strings.SplitN(u, "#", 2)[0]
		return u
	}
	if idx := strings.Index(u, "://"); idx >= 0 {
		u = u[idx+3:]
	}
	// Drop host, keep /path?query
	if idx := strings.Index(u, "/"); idx >= 0 {
		u = u[idx:]
	} else {
		return ""
	}
	u = strings.SplitN(u, "#", 2)[0]
	if !strings.HasPrefix(u, "/") {
		u = "/" + u
	}
	return u
}

// HistorySyncRequest API types
type HistorySyncAPIRequest struct {
	ChatJID         string `json:"chat_jid"`
	AnchorMessageID string `json:"anchor_message_id,omitempty"` // Optional - will auto-select if not provided
	Count           int    `json:"count"`
}

type HistorySyncAPIResponse struct {
	Success         bool   `json:"success"`
	Message         string `json:"message"`
	RequestAccepted bool   `json:"request_accepted,omitempty"`
	ChatJID         string `json:"chat_jid,omitempty"`
	AnchorMessageID string `json:"anchor_message_id,omitempty"`
	AnchorTimestamp string `json:"anchor_timestamp,omitempty"`
	Count           int    `json:"count,omitempty"`
	CooldownSeconds int    `json:"cooldown_seconds,omitempty"`
}

// RequestOnDemandHistorySync sends an on-demand history sync request via SendPeerMessage.
// This sends to the user's own primary device, NOT to any contact/group/status.
func RequestOnDemandHistorySync(client *whatsmeow.Client, messageStore *MessageStore, chatJID string, anchorMessageID string, count int, logger waLog.Logger) (*HistorySyncAPIResponse, error) {
	// Validate client state
	if !client.IsConnected() {
		return nil, fmt.Errorf("not connected to WhatsApp")
	}

	if client.Store.ID == nil {
		return nil, fmt.Errorf("not logged in")
	}

	// Validate count bounds (1-50)
	if count < 1 {
		count = 1
	}
	if count > 50 {
		count = 50
	}

	// Parse and validate chat JID
	parsedChatJID, err := types.ParseJID(chatJID)
	if err != nil {
		return nil, fmt.Errorf("invalid chat_jid: %v", err)
	}

	// Verify chat exists in our database
	var chatExists int
	err = messageStore.db.QueryRow("SELECT 1 FROM chats WHERE jid = $1", chatJID).Scan(&chatExists)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("chat %s not found in local database", chatJID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to verify chat: %v", err)
	}

	// Check cooldown
	historySyncTracker.mu.Lock()
	timeSinceLastRequest := time.Since(historySyncTracker.lastRequest)
	if timeSinceLastRequest < historySyncTracker.cooldown {
		remainingCooldown := historySyncTracker.cooldown - timeSinceLastRequest
		historySyncTracker.mu.Unlock()
		return &HistorySyncAPIResponse{
			Success:         false,
			Message:         fmt.Sprintf("Please wait %d seconds before making another history sync request", int(remainingCooldown.Seconds())+1),
			CooldownSeconds: int(remainingCooldown.Seconds()) + 1,
		}, nil
	}
	historySyncTracker.mu.Unlock()

	// Get valid anchor from database
	anchor, err := messageStore.GetValidAnchorForChat(chatJID, anchorMessageID)
	if err != nil {
		return nil, fmt.Errorf("failed to find valid anchor: %v", err)
	}

	// Build the MessageInfo for the anchor
	var senderJID types.JID
	if anchor.IsFromMe {
		senderJID = client.Store.ID.ToNonAD()
	} else {
		// Parse sender - could be phone@s.whatsapp.net or lid@lid
		if strings.Contains(anchor.Sender, "@") {
			senderJID, err = types.ParseJID(anchor.Sender)
		} else {
			// Assume phone number
			senderJID = types.JID{User: anchor.Sender, Server: types.DefaultUserServer}
		}
		if err != nil {
			return nil, fmt.Errorf("failed to parse sender JID: %v", err)
		}
	}

	anchorInfo := &types.MessageInfo{
		MessageSource: types.MessageSource{
			Chat:     parsedChatJID,
			Sender:   senderJID,
			IsFromMe: anchor.IsFromMe,
			IsGroup:  strings.HasSuffix(chatJID, "@g.us"),
		},
		ID:        anchor.ID,
		Timestamp: anchor.Timestamp,
	}

	// Build the history sync request message
	historyMsg := client.BuildHistorySyncRequest(anchorInfo, count)
	if historyMsg == nil {
		return nil, fmt.Errorf("failed to build history sync request")
	}

	// Send via SendPeerMessage - this goes to user's primary device, not any contact
	logger.Infof("Sending on-demand history sync request for chat %s, anchor %s, count %d", chatJID, anchor.ID, count)
	_, err = client.SendPeerMessage(context.Background(), historyMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to send history sync request: %v", err)
	}

	// Update cooldown tracker
	historySyncTracker.mu.Lock()
	historySyncTracker.lastRequest = time.Now()
	historySyncTracker.pendingRequest = &HistorySyncRequest{
		ChatJID:         chatJID,
		AnchorMessageID: anchor.ID,
		Count:           count,
		RequestTime:     time.Now(),
	}
	historySyncTracker.mu.Unlock()

	logger.Infof("On-demand history sync request sent successfully")

	return &HistorySyncAPIResponse{
		Success:         true,
		Message:         "History sync request sent to primary device. Response will arrive as ON_DEMAND history sync event.",
		RequestAccepted: true,
		ChatJID:         chatJID,
		AnchorMessageID: anchor.ID,
		AnchorTimestamp: anchor.Timestamp.Format(time.RFC3339),
		Count:           count,
	}, nil
}

// Start a REST API server to expose the WhatsApp client functionality
func startRESTServer(client *whatsmeow.Client, messageStore *MessageStore, port int, logger waLog.Logger) {
	// Handler for sending messages
	http.HandleFunc("/api/send", func(w http.ResponseWriter, r *http.Request) {
		// Only allow POST requests
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Parse the request body
		var req SendMessageRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}

		// Validate request
		if req.Recipient == "" {
			http.Error(w, "Recipient is required", http.StatusBadRequest)
			return
		}

		if req.Message == "" && req.MediaPath == "" {
			http.Error(w, "Message or media path is required", http.StatusBadRequest)
			return
		}

		fmt.Println("Received request to send message", req.Message, req.MediaPath)

		// Send the message
		success, message := sendWhatsAppMessage(client, req.Recipient, req.Message, req.MediaPath)
		fmt.Println("Message sent", success, message)
		// Set response headers
		w.Header().Set("Content-Type", "application/json")

		// Set appropriate status code
		if !success {
			w.WriteHeader(http.StatusInternalServerError)
		}

		// Send response
		json.NewEncoder(w).Encode(SendMessageResponse{
			Success: success,
			Message: message,
		})
	})

	// Handler for downloading media
	http.HandleFunc("/api/download", func(w http.ResponseWriter, r *http.Request) {
		// Only allow POST requests
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Parse the request body
		var req DownloadMediaRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}

		// Validate request
		if req.MessageID == "" || req.ChatJID == "" {
			http.Error(w, "Message ID and Chat JID are required", http.StatusBadRequest)
			return
		}

		// Download the media
		success, mediaType, filename, path, err := downloadMedia(client, messageStore, req.MessageID, req.ChatJID)

		// Set response headers
		w.Header().Set("Content-Type", "application/json")

		// Handle download result
		if !success || err != nil {
			errMsg := "Unknown error"
			if err != nil {
				errMsg = err.Error()
			}

			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(DownloadMediaResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to download media: %s", errMsg),
			})
			return
		}

		// Prefer container mount path so MCP can map to the Windows host store.
		respPath := path
		if idx := strings.Index(strings.ReplaceAll(path, "\\", "/"), "/store/"); idx >= 0 {
			respPath = "/app" + strings.ReplaceAll(path, "\\", "/")[idx:]
		}

		// Send successful response
		json.NewEncoder(w).Encode(DownloadMediaResponse{
			Success:   true,
			Message:   fmt.Sprintf("Successfully downloaded %s media", mediaType),
			Filename:  filename,
			Path:      respPath,
			MediaType: mediaType,
		})
	})

	http.HandleFunc("/api/history-sync", func(w http.ResponseWriter, r *http.Request) {
		// Only allow POST requests
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Parse the request body
		var req HistorySyncAPIRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(HistorySyncAPIResponse{
				Success: false,
				Message: "Invalid request format",
			})
			return
		}

		// Validate required fields
		if req.ChatJID == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(HistorySyncAPIResponse{
				Success: false,
				Message: "chat_jid is required",
			})
			return
		}

		// Default count to 50 if not specified
		if req.Count == 0 {
			req.Count = 50
		}

		// Execute the history sync request
		response, err := RequestOnDemandHistorySync(client, messageStore, req.ChatJID, req.AnchorMessageID, req.Count, logger)

		w.Header().Set("Content-Type", "application/json")

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(HistorySyncAPIResponse{
				Success: false,
				Message: err.Error(),
			})
			return
		}

		if !response.Success {
			w.WriteHeader(http.StatusTooManyRequests)
		}

		json.NewEncoder(w).Encode(response)
	})

	// Start the server
	serverAddr := fmt.Sprintf(":%d", port)
	fmt.Printf("Starting REST API server on %s...\n", serverAddr)

	// Run server in a goroutine so it doesn't block
	go func() {
		if err := http.ListenAndServe(serverAddr, nil); err != nil {
			fmt.Printf("REST API server error: %v\n", err)
		}
	}()
}

func main() {
	// Set up logger
	logger := waLog.Stdout("Client", "INFO", true)
	logger.Infof("Starting WhatsApp client...")

	// Session store lives in local Postgres (same database as messages).
	dbLog := waLog.Stdout("Database", "INFO", true)

	if err := os.MkdirAll("store", 0755); err != nil {
		logger.Errorf("Failed to create store directory: %v", err)
		return
	}

	logger.Infof("Using Postgres for WhatsApp session and messages")
	container, err := sqlstore.New(context.Background(), "postgres", databaseURL(), dbLog)
	if err != nil {
		logger.Errorf("Failed to connect to database: %v", err)
		return
	}

	// Get device store - This contains session information
	deviceStore, err := container.GetFirstDevice(context.Background())
	if err != nil {
		if err == sql.ErrNoRows {
			// No device exists, create one
			deviceStore = container.NewDevice()
			logger.Infof("Created new device")
		} else {
			logger.Errorf("Failed to get device: %v", err)
			return
		}
	}

	// Create client instance
	client := whatsmeow.NewClient(deviceStore, logger)
	if client == nil {
		logger.Errorf("Failed to create WhatsApp client")
		return
	}

	// Initialize message store
	messageStore, err := NewMessageStore()
	if err != nil {
		logger.Errorf("Failed to initialize message store: %v", err)
		return
	}
	defer messageStore.Close()

	// Setup event handling for messages and history sync
	client.AddEventHandler(func(evt interface{}) {
		switch v := evt.(type) {
		case *events.Message:
			// Process regular messages
			handleMessage(client, messageStore, v, logger)

		case *events.HistorySync:
			// Process history sync events
			handleHistorySync(client, messageStore, v, logger)

		case *events.Connected:
			logger.Infof("Connected to WhatsApp")

		case *events.LoggedOut:
			logger.Warnf("Device logged out, please scan QR code to log in again")
		}
	})

	// Create channel to track connection success
	connected := make(chan bool, 1)

	// Connect to WhatsApp
	if client.Store.ID == nil {
		// No ID stored, this is a new client, need to pair with phone
		qrChan, _ := client.GetQRChannel(context.Background())
		err = client.Connect()
		if err != nil {
			logger.Errorf("Failed to connect: %v", err)
			return
		}

		// Print QR code for pairing with phone
		for evt := range qrChan {
			if evt.Event == "code" {
				fmt.Println("\nScan this QR code with your WhatsApp app:")
				qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)
			} else if evt.Event == "success" {
				connected <- true
				break
			}
		}

		// Wait for connection
		select {
		case <-connected:
			fmt.Println("\nSuccessfully connected and authenticated!")
		case <-time.After(3 * time.Minute):
			logger.Errorf("Timeout waiting for QR code scan")
			return
		}
	} else {
		// Already logged in, just connect
		err = client.Connect()
		if err != nil {
			logger.Errorf("Failed to connect: %v", err)
			return
		}
		connected <- true
	}

	// Wait a moment for connection to stabilize
	time.Sleep(2 * time.Second)

	if !client.IsConnected() {
		logger.Errorf("Failed to establish stable connection")
		return
	}

	fmt.Println("\nâœ“ Connected to WhatsApp! Type 'help' for commands.")

	// Start REST API server
	startRESTServer(client, messageStore, 8080, logger)

	// Create a channel to keep the main goroutine alive
	exitChan := make(chan os.Signal, 1)
	signal.Notify(exitChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("REST server is running. Press Ctrl+C to disconnect and exit.")

	// Wait for termination signal
	<-exitChan

	fmt.Println("Disconnecting...")
	// Disconnect client
	client.Disconnect()
}

// GetChatName determines the appropriate name for a chat based on JID and other info
func GetChatName(client *whatsmeow.Client, messageStore *MessageStore, jid types.JID, chatJID string, conversation interface{}, sender string, logger waLog.Logger) string {
	// First, check if chat already exists in database with a name
	var existingName string
	err := messageStore.db.QueryRow("SELECT COALESCE(name,'') FROM chats WHERE jid = $1", chatJID).Scan(&existingName)
	if err == nil && existingName != "" {
		// Chat exists with a name, use that
		logger.Infof("Using existing chat name for %s: %s", chatJID, existingName)
		return existingName
	}

	// Need to determine chat name
	var name string

	if jid.Server == "g.us" {
		// This is a group chat
		logger.Infof("Getting name for group: %s", chatJID)

		// Use conversation data if provided (from history sync)
		if conversation != nil {
			// Extract name from conversation if available
			// This uses type assertions to handle different possible types
			var displayName, convName *string
			// Try to extract the fields we care about regardless of the exact type
			v := reflect.ValueOf(conversation)
			if v.Kind() == reflect.Ptr && !v.IsNil() {
				v = v.Elem()

				// Try to find DisplayName field
				if displayNameField := v.FieldByName("DisplayName"); displayNameField.IsValid() && displayNameField.Kind() == reflect.Ptr && !displayNameField.IsNil() {
					dn := displayNameField.Elem().String()
					displayName = &dn
				}

				// Try to find Name field
				if nameField := v.FieldByName("Name"); nameField.IsValid() && nameField.Kind() == reflect.Ptr && !nameField.IsNil() {
					n := nameField.Elem().String()
					convName = &n
				}
			}

			// Use the name we found
			if displayName != nil && *displayName != "" {
				name = *displayName
			} else if convName != nil && *convName != "" {
				name = *convName
			}
		}

		// If we didn't get a name, try group info
		if name == "" {
			groupInfo, err := client.GetGroupInfo(context.Background(), jid)
			if err == nil && groupInfo.Name != "" {
				name = groupInfo.Name
			} else {
				// Fallback name for groups
				name = fmt.Sprintf("Group %s", jid.User)
			}
		}

		logger.Infof("Using group name: %s", name)
	} else {
		// This is an individual contact
		logger.Infof("Getting name for contact: %s", chatJID)

		// Just use contact info (full name)
		contact, err := client.Store.Contacts.GetContact(context.Background(), jid)
		if err == nil && contact.FullName != "" {
			name = contact.FullName
		} else if sender != "" {
			// Fallback to sender
			name = sender
		} else {
			// Last fallback to JID
			name = jid.User
		}

		logger.Infof("Using contact name: %s", name)
	}

	return name
}

// Handle history sync events using ParseWebMessage for correct sender extraction.
// This handles both initial sync and ON_DEMAND responses.
func handleHistorySync(client *whatsmeow.Client, messageStore *MessageStore, historySync *events.HistorySync, logger waLog.Logger) {
	syncType := "UNKNOWN"
	if historySync.Data.SyncType != nil {
		syncType = historySync.Data.SyncType.String()
	}

	fmt.Printf("Received history sync event (type: %s) with %d conversations\n", syncType, len(historySync.Data.Conversations))

	syncedCount := 0
	skippedNoSender := 0
	repairedCount := 0

	for _, conversation := range historySync.Data.Conversations {
		// Parse JID from the conversation
		if conversation.ID == nil {
			continue
		}

		chatJID := *conversation.ID

		// Try to parse the JID
		jid, err := types.ParseJID(chatJID)
		if err != nil {
			logger.Warnf("Failed to parse JID %s: %v", chatJID, err)
			continue
		}

		// Get appropriate chat name by passing the history sync conversation directly
		name := GetChatName(client, messageStore, jid, chatJID, conversation, "", logger)

		// Process messages
		messages := conversation.Messages
		if len(messages) > 0 {
			// Update chat with latest message timestamp
			latestMsg := messages[0]
			if latestMsg == nil || latestMsg.Message == nil {
				continue
			}

			// Get timestamp from message info
			timestamp := time.Time{}
			if ts := latestMsg.Message.GetMessageTimestamp(); ts != 0 {
				timestamp = time.Unix(int64(ts), 0)
			} else {
				continue
			}

			messageStore.StoreChat(chatJID, name, timestamp)

			// Store messages using ParseWebMessage for correct sender extraction
			for _, msg := range messages {
				if msg == nil || msg.Message == nil {
					continue
				}

				// Use ParseWebMessage - the library-supported way to extract sender
				evt, err := client.ParseWebMessage(jid, msg.Message)
				if err != nil {
					// ParseWebMessage returns an error if sender cannot be determined
					if jid.Server == types.GroupServer {
						skippedNoSender++
						logger.Warnf("Skipping group message %s: %v (participant info unavailable)",
							msg.Message.GetKey().GetID(), err)
					} else {
						logger.Warnf("Failed to parse message %s: %v", msg.Message.GetKey().GetID(), err)
					}
					continue
				}

				// Extract text content
				content := extractTextContent(evt.RawMessage)

				// Extract media info
				mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength := extractMediaInfo(evt.RawMessage)

				// Skip messages with no content and no media
				if content == "" && mediaType == "" {
					continue
				}

				// Get sender from the parsed event - this is the CORRECT sender
				sender := evt.Info.Sender.User

				// Check if this message already exists with corrupted sender
				var existingSender string
				err = messageStore.db.QueryRow(
					"SELECT COALESCE(sender,'') FROM messages WHERE id = $1 AND chat_jid = $2",
					evt.Info.ID, chatJID,
				).Scan(&existingSender)

				isRepair := false
				if err == nil {
					// Message exists - check if this is a repair
					isGroup := jid.Server == types.GroupServer
					groupUser := ""
					if isGroup {
						groupUser = jid.User
					}
					if isGroup && existingSender == groupUser && sender != groupUser {
						// This is a repair - old sender was corrupted, new sender is valid
						isRepair = true
						logger.Infof("Repairing message %s sender: %s -> %s", evt.Info.ID, existingSender, sender)
					}
				}

				err = messageStore.StoreMessage(
					evt.Info.ID,
					chatJID,
					sender,
					content,
					evt.Info.Timestamp,
					evt.Info.IsFromMe,
					mediaType,
					filename,
					url,
					mediaKey,
					fileSHA256,
					fileEncSHA256,
					fileLength,
				)
				if err != nil {
					logger.Warnf("Failed to store history message: %v", err)
				} else {
					syncedCount++
					if isRepair {
						repairedCount++
					}
					// Log successful message storage
					if mediaType != "" {
						logger.Infof("Stored message: [%s] %s -> %s: [%s: %s] %s",
							evt.Info.Timestamp.Format("2006-01-02 15:04:05"), sender, chatJID, mediaType, filename, content)
					} else {
						logger.Infof("Stored message: [%s] %s -> %s: %s",
							evt.Info.Timestamp.Format("2006-01-02 15:04:05"), sender, chatJID, content)
					}
				}
			}
		}
	}

	fmt.Printf("History sync complete (type: %s). Stored %d messages", syncType, syncedCount)
	if repairedCount > 0 {
		fmt.Printf(", repaired %d corrupted sender(s)", repairedCount)
	}
	if skippedNoSender > 0 {
		fmt.Printf(", skipped %d group messages without participant info", skippedNoSender)
	}
	fmt.Println(".")

	// Clear pending request if this was an ON_DEMAND response
	if syncType == "ON_DEMAND" {
		historySyncTracker.mu.Lock()
		historySyncTracker.pendingRequest = nil
		historySyncTracker.mu.Unlock()
	}
}

// Request history sync from the server
func requestHistorySync(client *whatsmeow.Client) {
	if client == nil {
		fmt.Println("Client is not initialized. Cannot request history sync.")
		return
	}

	if !client.IsConnected() {
		fmt.Println("Client is not connected. Please ensure you are connected to WhatsApp first.")
		return
	}

	if client.Store.ID == nil {
		fmt.Println("Client is not logged in. Please scan the QR code first.")
		return
	}

	// Build and send a history sync request
	historyMsg := client.BuildHistorySyncRequest(nil, 100)
	if historyMsg == nil {
		fmt.Println("Failed to build history sync request.")
		return
	}

	_, err := client.SendMessage(context.Background(), types.JID{
		Server: "s.whatsapp.net",
		User:   "status",
	}, historyMsg)

	if err != nil {
		fmt.Printf("Failed to request history sync: %v\n", err)
	} else {
		fmt.Println("History sync requested. Waiting for server response...")
	}
}

// analyzeOggOpus tries to extract duration and generate a simple waveform from an Ogg Opus file
func analyzeOggOpus(data []byte) (duration uint32, waveform []byte, err error) {
	// Try to detect if this is a valid Ogg file by checking for the "OggS" signature
	// at the beginning of the file
	if len(data) < 4 || string(data[0:4]) != "OggS" {
		return 0, nil, fmt.Errorf("not a valid Ogg file (missing OggS signature)")
	}

	// Parse Ogg pages to find the last page with a valid granule position
	var lastGranule uint64
	var sampleRate uint32 = 48000 // Default Opus sample rate
	var preSkip uint16 = 0
	var foundOpusHead bool

	// Scan through the file looking for Ogg pages
	for i := 0; i < len(data); {
		// Check if we have enough data to read Ogg page header
		if i+27 >= len(data) {
			break
		}

		// Verify Ogg page signature
		if string(data[i:i+4]) != "OggS" {
			// Skip until next potential page
			i++
			continue
		}

		// Extract header fields
		granulePos := binary.LittleEndian.Uint64(data[i+6 : i+14])
		pageSeqNum := binary.LittleEndian.Uint32(data[i+18 : i+22])
		numSegments := int(data[i+26])

		// Extract segment table
		if i+27+numSegments >= len(data) {
			break
		}
		segmentTable := data[i+27 : i+27+numSegments]

		// Calculate page size
		pageSize := 27 + numSegments
		for _, segLen := range segmentTable {
			pageSize += int(segLen)
		}

		// Check if we're looking at an OpusHead packet (should be in first few pages)
		if !foundOpusHead && pageSeqNum <= 1 {
			// Look for "OpusHead" marker in this page
			pageData := data[i : i+pageSize]
			headPos := bytes.Index(pageData, []byte("OpusHead"))
			if headPos >= 0 && headPos+12 < len(pageData) {
				// Found OpusHead, extract sample rate and pre-skip
				// OpusHead format: Magic(8) + Version(1) + Channels(1) + PreSkip(2) + SampleRate(4) + ...
				headPos += 8 // Skip "OpusHead" marker
				// PreSkip is 2 bytes at offset 10
				if headPos+12 <= len(pageData) {
					preSkip = binary.LittleEndian.Uint16(pageData[headPos+10 : headPos+12])
					sampleRate = binary.LittleEndian.Uint32(pageData[headPos+12 : headPos+16])
					foundOpusHead = true
					fmt.Printf("Found OpusHead: sampleRate=%d, preSkip=%d\n", sampleRate, preSkip)
				}
			}
		}

		// Keep track of last valid granule position
		if granulePos != 0 {
			lastGranule = granulePos
		}

		// Move to next page
		i += pageSize
	}

	if !foundOpusHead {
		fmt.Println("Warning: OpusHead not found, using default values")
	}

	// Calculate duration based on granule position
	if lastGranule > 0 {
		// Formula for duration: (lastGranule - preSkip) / sampleRate
		durationSeconds := float64(lastGranule-uint64(preSkip)) / float64(sampleRate)
		duration = uint32(math.Ceil(durationSeconds))
		fmt.Printf("Calculated Opus duration from granule: %f seconds (lastGranule=%d)\n",
			durationSeconds, lastGranule)
	} else {
		// Fallback to rough estimation if granule position not found
		fmt.Println("Warning: No valid granule position found, using estimation")
		durationEstimate := float64(len(data)) / 2000.0 // Very rough approximation
		duration = uint32(durationEstimate)
	}

	// Make sure we have a reasonable duration (at least 1 second, at most 300 seconds)
	if duration < 1 {
		duration = 1
	} else if duration > 300 {
		duration = 300
	}

	// Generate waveform
	waveform = placeholderWaveform(duration)

	fmt.Printf("Ogg Opus analysis: size=%d bytes, calculated duration=%d sec, waveform=%d bytes\n",
		len(data), duration, len(waveform))

	return duration, waveform, nil
}

// min returns the smaller of x or y
func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// placeholderWaveform generates a synthetic waveform for WhatsApp voice messages
// that appears natural with some variability based on the duration
func placeholderWaveform(duration uint32) []byte {
	// WhatsApp expects a 64-byte waveform for voice messages
	const waveformLength = 64
	waveform := make([]byte, waveformLength)

	// Seed the random number generator for consistent results with the same duration
	rand.Seed(int64(duration))

	// Create a more natural looking waveform with some patterns and variability
	// rather than completely random values

	// Base amplitude and frequency - longer messages get faster frequency
	baseAmplitude := 35.0
	frequencyFactor := float64(min(int(duration), 120)) / 30.0

	for i := range waveform {
		// Position in the waveform (normalized 0-1)
		pos := float64(i) / float64(waveformLength)

		// Create a wave pattern with some randomness
		// Use multiple sine waves of different frequencies for more natural look
		val := baseAmplitude * math.Sin(pos*math.Pi*frequencyFactor*8)
		val += (baseAmplitude / 2) * math.Sin(pos*math.Pi*frequencyFactor*16)

		// Add some randomness to make it look more natural
		val += (rand.Float64() - 0.5) * 15

		// Add some fade-in and fade-out effects
		fadeInOut := math.Sin(pos * math.Pi)
		val = val * (0.7 + 0.3*fadeInOut)

		// Center around 50 (typical voice baseline)
		val = val + 50

		// Ensure values stay within WhatsApp's expected range (0-100)
		if val < 0 {
			val = 0
		} else if val > 100 {
			val = 100
		}

		waveform[i] = byte(val)
	}

	return waveform
}
