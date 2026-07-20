package main

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// db temporário com o MESMO schema/pragma do NewMessageStore (FK ligada — é ela que
// obriga o TouchChat a vir antes do StoreMessage no caminho de envio).
func testStore(t *testing.T) *MessageStore {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+filepath.Join(t.TempDir(), "m.db")+"?_foreign_keys=on")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		CREATE TABLE chats (jid TEXT PRIMARY KEY, name TEXT, last_message_time TIMESTAMP);
		CREATE TABLE messages (
			id TEXT, chat_jid TEXT, sender TEXT, content TEXT, timestamp TIMESTAMP,
			is_from_me BOOLEAN, media_type TEXT, filename TEXT, url TEXT, media_key BLOB,
			file_sha256 BLOB, file_enc_sha256 BLOB, file_length INTEGER,
			PRIMARY KEY (id, chat_jid), FOREIGN KEY (chat_jid) REFERENCES chats(jid));`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return &MessageStore{db: db}
}

func chatRow(t *testing.T, s *MessageStore, jid string) (string, time.Time) {
	t.Helper()
	var name string
	var ts time.Time
	if err := s.db.QueryRow("SELECT name, last_message_time FROM chats WHERE jid = ?", jid).
		Scan(&name, &ts); err != nil {
		t.Fatal(err)
	}
	return name, ts
}

// O que faria a thread regredir: TouchChat apagar o nome bom do contato.
func TestTouchChatPreservesName(t *testing.T) {
	s := testStore(t)
	jid := "5527998002260@s.whatsapp.net"
	t0 := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	if err := s.StoreChat(jid, "Romulo Gatti", t0); err != nil {
		t.Fatal(err)
	}

	t1 := t0.Add(time.Hour)
	if err := s.TouchChat(jid, "5527998002260", t1); err != nil { // fallback = número
		t.Fatal(err)
	}

	name, ts := chatRow(t, s, jid)
	if name != "Romulo Gatti" {
		t.Errorf("nome sobrescrito pelo fallback: %q", name)
	}
	if !ts.Equal(t1) {
		t.Errorf("last_message_time não avançou: %v != %v", ts, t1)
	}
}

// Conversa iniciada por nós: a linha precisa nascer, senão o inner JOIN dos
// leitores (Clauditor) esconde a conversa inteira.
func TestTouchChatCreatesNewAndAllowsMessage(t *testing.T) {
	s := testStore(t)
	jid := "5511999999999@s.whatsapp.net"
	now := time.Now().UTC().Truncate(time.Second)

	if err := s.TouchChat(jid, "Contato Novo", now); err != nil {
		t.Fatal(err)
	}
	if name, _ := chatRow(t, s, jid); name != "Contato Novo" {
		t.Errorf("chat novo devia nascer com o fallback, veio %q", name)
	}

	// só passa se a linha de chats existe — é a FK que o caminho de envio precisa honrar
	if err := s.StoreMessage("MSGID1", jid, "5527998372363", "oi", now, true,
		"", "", "", nil, nil, nil, 0); err != nil {
		t.Fatalf("FK barrou a mensagem mesmo após TouchChat: %v", err)
	}
}

// Sem o TouchChat antes, a FK rejeita — a regressão que o comentário no envio previne.
func TestStoreMessageNeedsChatRow(t *testing.T) {
	s := testStore(t)
	if err := s.StoreMessage("MSGID2", "5511888888888@s.whatsapp.net", "eu", "oi",
		time.Now(), true, "", "", "", nil, nil, nil, 0); err == nil {
		t.Error("esperava erro de FK para chat inexistente")
	}
}
