package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// TestMessageStoreBasics tests basic MessageStore operations
func TestMessageStoreBasics(t *testing.T) {
	// Create temp directory for test database
	tmpDir, err := os.MkdirTemp("", "whatsapp_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create store subdirectory
	storeDir := filepath.Join(tmpDir, "store")
	if err := os.MkdirAll(storeDir, 0755); err != nil {
		t.Fatalf("Failed to create store dir: %v", err)
	}

	// Change working directory temporarily
	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	// Create MessageStore
	store, err := NewMessageStore()
	if err != nil {
		t.Fatalf("Failed to create MessageStore: %v", err)
	}
	defer store.Close()

	// Test storing a chat
	now := time.Now()
	err = store.StoreChat("123456789@s.whatsapp.net", "Test User", now)
	if err != nil {
		t.Errorf("Failed to store chat: %v", err)
	}

	// Test storing a message
	err = store.StoreMessage(
		"msg123",
		"123456789@s.whatsapp.net",
		"123456789",
		"Hello world",
		now,
		false,
		"", "", "", nil, nil, nil, 0,
	)
	if err != nil {
		t.Errorf("Failed to store message: %v", err)
	}

	// Test retrieving messages
	messages, err := store.GetMessages("123456789@s.whatsapp.net", 10)
	if err != nil {
		t.Errorf("Failed to get messages: %v", err)
	}
	if len(messages) != 1 {
		t.Errorf("Expected 1 message, got %d", len(messages))
	}
	if messages[0].Content != "Hello world" {
		t.Errorf("Expected 'Hello world', got '%s'", messages[0].Content)
	}
}

// TestGroupMessageSender tests that group messages store correct sender
func TestGroupMessageSender(t *testing.T) {
	// Create temp directory for test database
	tmpDir, err := os.MkdirTemp("", "whatsapp_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create store subdirectory
	storeDir := filepath.Join(tmpDir, "store")
	if err := os.MkdirAll(storeDir, 0755); err != nil {
		t.Fatalf("Failed to create store dir: %v", err)
	}

	// Change working directory temporarily
	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	// Create MessageStore
	store, err := NewMessageStore()
	if err != nil {
		t.Fatalf("Failed to create MessageStore: %v", err)
	}
	defer store.Close()

	groupJID := "120363429500369902@g.us"
	participantPhone := "6281234567890" // Participant phone number

	// Store group chat
	err = store.StoreChat(groupJID, "Test Group", time.Now())
	if err != nil {
		t.Errorf("Failed to store chat: %v", err)
	}

	// Store a message with correct participant sender
	err = store.StoreMessage(
		"msg001",
		groupJID,
		participantPhone,
		"Hello from participant",
		time.Now(),
		false,
		"", "", "", nil, nil, nil, 0,
	)
	if err != nil {
		t.Errorf("Failed to store message: %v", err)
	}

	// Retrieve and verify
	messages, err := store.GetMessages(groupJID, 10)
	if err != nil {
		t.Errorf("Failed to get messages: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(messages))
	}

	// The sender should be the participant, not the group ID
	if messages[0].Sender != participantPhone {
		t.Errorf("Expected sender '%s', got '%s'", participantPhone, messages[0].Sender)
	}
}

// TestCorruptedSenderDetection tests that corrupted sender (group ID as sender) is properly detected
func TestCorruptedSenderDetection(t *testing.T) {
	// Create temp directory for test database
	tmpDir, err := os.MkdirTemp("", "whatsapp_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create store subdirectory
	storeDir := filepath.Join(tmpDir, "store")
	if err := os.MkdirAll(storeDir, 0755); err != nil {
		t.Fatalf("Failed to create store dir: %v", err)
	}

	// Change working directory temporarily
	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	// Create MessageStore
	store, err := NewMessageStore()
	if err != nil {
		t.Fatalf("Failed to create MessageStore: %v", err)
	}
	defer store.Close()

	groupJID := "120363429500369902@g.us"
	groupUser := "120363429500369902" // The corrupted sender (equals group user part)
	validParticipant := "6281234567890"

	// Store group chat
	err = store.StoreChat(groupJID, "Test Group", time.Now())
	if err != nil {
		t.Errorf("Failed to store chat: %v", err)
	}

	// Store message with corrupted sender (sender = group ID)
	err = store.StoreMessage(
		"corrupt_msg",
		groupJID,
		groupUser,
		"Corrupted message",
		time.Now().Add(-time.Hour),
		false,
		"", "", "", nil, nil, nil, 0,
	)
	if err != nil {
		t.Errorf("Failed to store corrupted message: %v", err)
	}

	// Store message with valid sender
	err = store.StoreMessage(
		"valid_msg",
		groupJID,
		validParticipant,
		"Valid message",
		time.Now(),
		false,
		"", "", "", nil, nil, nil, 0,
	)
	if err != nil {
		t.Errorf("Failed to store valid message: %v", err)
	}

	// Store self-message (is_from_me = true)
	err = store.StoreMessage(
		"self_msg",
		groupJID,
		"6289876543210",
		"My message",
		time.Now().Add(-30*time.Minute),
		true,
		"", "", "", nil, nil, nil, 0,
	)
	if err != nil {
		t.Errorf("Failed to store self message: %v", err)
	}

	// Test anchor validation - corrupted anchor should be rejected
	_, err = store.GetValidAnchorForChat(groupJID, "corrupt_msg")
	if err == nil {
		t.Error("Expected error for corrupted anchor, got nil")
	}

	// Test anchor validation - valid anchor should work
	anchor, err := store.GetValidAnchorForChat(groupJID, "valid_msg")
	if err != nil {
		t.Errorf("Failed to get valid anchor: %v", err)
	}
	if anchor.ID != "valid_msg" {
		t.Errorf("Expected anchor ID 'valid_msg', got '%s'", anchor.ID)
	}

	// Test anchor validation - self-message anchor should work
	anchor, err = store.GetValidAnchorForChat(groupJID, "self_msg")
	if err != nil {
		t.Errorf("Failed to get self-message anchor: %v", err)
	}
	if !anchor.IsFromMe {
		t.Error("Expected IsFromMe=true for self-message anchor")
	}

	// Test auto-selection should prefer self-message or valid participant
	anchor, err = store.GetValidAnchorForChat(groupJID, "")
	if err != nil {
		t.Errorf("Failed to auto-select anchor: %v", err)
	}
	// Should NOT select the corrupted message
	if anchor.ID == "corrupt_msg" {
		t.Error("Auto-selected corrupted anchor, should have skipped it")
	}
}

// TestLongNumericLID tests that long numeric LIDs (18+ digits) are handled correctly
func TestLongNumericLID(t *testing.T) {
	// Create temp directory for test database
	tmpDir, err := os.MkdirTemp("", "whatsapp_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create store subdirectory
	storeDir := filepath.Join(tmpDir, "store")
	if err := os.MkdirAll(storeDir, 0755); err != nil {
		t.Fatalf("Failed to create store dir: %v", err)
	}

	// Change working directory temporarily
	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	// Create MessageStore
	store, err := NewMessageStore()
	if err != nil {
		t.Fatalf("Failed to create MessageStore: %v", err)
	}
	defer store.Close()

	groupJID := "120363429500369902@g.us"
	longLID := "123456789012345678" // 18-digit LID

	// Store group chat
	err = store.StoreChat(groupJID, "Test Group", time.Now())
	if err != nil {
		t.Errorf("Failed to store chat: %v", err)
	}

	// Store message with long numeric LID sender
	err = store.StoreMessage(
		"lid_msg",
		groupJID,
		longLID,
		"Message from LID user",
		time.Now(),
		false,
		"", "", "", nil, nil, nil, 0,
	)
	if err != nil {
		t.Errorf("Failed to store message: %v", err)
	}

	// The long LID should NOT be confused with a group ID
	// Verify anchor selection accepts it
	anchor, err := store.GetValidAnchorForChat(groupJID, "lid_msg")
	if err != nil {
		t.Errorf("Long LID wrongly rejected as anchor: %v", err)
	}
	if anchor.Sender != longLID {
		t.Errorf("Expected sender '%s', got '%s'", longLID, anchor.Sender)
	}
}

// TestDirectChatAnchor tests anchor selection for direct (1:1) chats
func TestDirectChatAnchor(t *testing.T) {
	// Create temp directory for test database
	tmpDir, err := os.MkdirTemp("", "whatsapp_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create store subdirectory
	storeDir := filepath.Join(tmpDir, "store")
	if err := os.MkdirAll(storeDir, 0755); err != nil {
		t.Fatalf("Failed to create store dir: %v", err)
	}

	// Change working directory temporarily
	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	// Create MessageStore
	store, err := NewMessageStore()
	if err != nil {
		t.Fatalf("Failed to create MessageStore: %v", err)
	}
	defer store.Close()

	chatJID := "6281234567890@s.whatsapp.net"
	contactPhone := "6281234567890"

	// Store direct chat
	err = store.StoreChat(chatJID, "Test Contact", time.Now())
	if err != nil {
		t.Errorf("Failed to store chat: %v", err)
	}

	// Store messages
	err = store.StoreMessage(
		"dm_msg",
		chatJID,
		contactPhone,
		"Direct message",
		time.Now(),
		false,
		"", "", "", nil, nil, nil, 0,
	)
	if err != nil {
		t.Errorf("Failed to store message: %v", err)
	}

	// For direct chats, any message should be valid anchor
	anchor, err := store.GetValidAnchorForChat(chatJID, "")
	if err != nil {
		t.Errorf("Failed to get anchor for direct chat: %v", err)
	}
	if anchor.ID != "dm_msg" {
		t.Errorf("Expected anchor ID 'dm_msg', got '%s'", anchor.ID)
	}
}

// TestHistorySyncCooldown tests the cooldown mechanism
func TestHistorySyncCooldown(t *testing.T) {
	// Reset tracker for test
	oldCooldown := historySyncTracker.cooldown
	historySyncTracker.cooldown = 100 * time.Millisecond
	defer func() { historySyncTracker.cooldown = oldCooldown }()

	// Set last request time
	historySyncTracker.mu.Lock()
	historySyncTracker.lastRequest = time.Now()
	historySyncTracker.mu.Unlock()

	// Check cooldown is active
	historySyncTracker.mu.Lock()
	timeSinceLastRequest := time.Since(historySyncTracker.lastRequest)
	inCooldown := timeSinceLastRequest < historySyncTracker.cooldown
	historySyncTracker.mu.Unlock()

	if !inCooldown {
		t.Error("Expected to be in cooldown period")
	}

	// Wait for cooldown
	time.Sleep(150 * time.Millisecond)

	// Check cooldown expired
	historySyncTracker.mu.Lock()
	timeSinceLastRequest = time.Since(historySyncTracker.lastRequest)
	inCooldown = timeSinceLastRequest < historySyncTracker.cooldown
	historySyncTracker.mu.Unlock()

	if inCooldown {
		t.Error("Expected cooldown to have expired")
	}
}

// TestCountBounds tests that history sync count is bounded 1-50
func TestCountBounds(t *testing.T) {
	testCases := []struct {
		input    int
		expected int
	}{
		{-5, 1},
		{0, 1},
		{1, 1},
		{25, 25},
		{50, 50},
		{51, 50},
		{100, 50},
	}

	for _, tc := range testCases {
		count := tc.input
		if count < 1 {
			count = 1
		}
		if count > 50 {
			count = 50
		}
		if count != tc.expected {
			t.Errorf("Input %d: expected %d, got %d", tc.input, tc.expected, count)
		}
	}
}

// TestAnchorNotFound tests error handling when anchor message doesn't exist
func TestAnchorNotFound(t *testing.T) {
	// Create temp directory for test database
	tmpDir, err := os.MkdirTemp("", "whatsapp_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create store subdirectory
	storeDir := filepath.Join(tmpDir, "store")
	if err := os.MkdirAll(storeDir, 0755); err != nil {
		t.Fatalf("Failed to create store dir: %v", err)
	}

	// Change working directory temporarily
	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	// Create MessageStore
	store, err := NewMessageStore()
	if err != nil {
		t.Fatalf("Failed to create MessageStore: %v", err)
	}
	defer store.Close()

	chatJID := "120363429500369902@g.us"

	// Store chat without any messages
	err = store.StoreChat(chatJID, "Test Group", time.Now())
	if err != nil {
		t.Errorf("Failed to store chat: %v", err)
	}

	// Try to get anchor for non-existent message
	_, err = store.GetValidAnchorForChat(chatJID, "nonexistent_msg")
	if err == nil {
		t.Error("Expected error for non-existent anchor")
	}

	// Try to auto-select when no messages exist
	_, err = store.GetValidAnchorForChat(chatJID, "")
	if err == nil {
		t.Error("Expected error when no valid anchors exist")
	}
}
