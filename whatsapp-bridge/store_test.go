package main

import (
	"database/sql"
	"os"
	"path/filepath"
	"syscall"
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
	return &MessageStore{db: db, storeDir: t.TempDir()}
}

func TestLoadRuntimeConfigDefaults(t *testing.T) {
	t.Setenv("WHATSAPP_ACCOUNT_NAME", "")
	t.Setenv("WHATSAPP_STORE_DIR", "")
	t.Setenv("WHATSAPP_QR_PATH", "")
	t.Setenv("WHATSAPP_BRIDGE_BIND", "")
	t.Setenv("WHATSAPP_BRIDGE_PORT", "")
	t.Setenv("WHATSAPP_AUTO_TRANSCRIBE", "")

	cfg, err := loadRuntimeConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AccountName != "whatsapp" || cfg.StoreDir != "store" || cfg.QRPath != "qr.txt" {
		t.Fatalf("unexpected default paths: %+v", cfg)
	}
	if cfg.Port != 8080 || cfg.BindAddress != "" || !cfg.AutoTranscribe {
		t.Fatalf("unexpected default runtime settings: %+v", cfg)
	}
}

func TestLoadRuntimeConfigIsolatedAccount(t *testing.T) {
	root := t.TempDir()
	storeDir := filepath.Join(root, "store")
	qrPath := filepath.Join(root, "account.qr")
	t.Setenv("WHATSAPP_ACCOUNT_NAME", "Trainer Connect")
	t.Setenv("WHATSAPP_STORE_DIR", storeDir)
	t.Setenv("WHATSAPP_QR_PATH", qrPath)
	t.Setenv("WHATSAPP_BRIDGE_BIND", "127.0.0.1")
	t.Setenv("WHATSAPP_BRIDGE_PORT", "8081")
	t.Setenv("WHATSAPP_AUTO_TRANSCRIBE", "false")

	cfg, err := loadRuntimeConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AccountName != "Trainer Connect" || cfg.StoreDir != storeDir || cfg.QRPath != qrPath {
		t.Fatalf("unexpected isolated paths: %+v", cfg)
	}
	if cfg.Port != 8081 || cfg.BindAddress != "127.0.0.1" || cfg.AutoTranscribe {
		t.Fatalf("unexpected isolated runtime settings: %+v", cfg)
	}
}

func TestLoadRuntimeConfigRejectsInvalidValues(t *testing.T) {
	t.Setenv("WHATSAPP_BRIDGE_PORT", "8080x")
	if _, err := loadRuntimeConfig(); err == nil {
		t.Fatal("expected invalid bridge port to fail")
	}
	t.Setenv("WHATSAPP_BRIDGE_PORT", "8081")
	t.Setenv("WHATSAPP_AUTO_TRANSCRIBE", "sometimes")
	if _, err := loadRuntimeConfig(); err == nil {
		t.Fatal("expected invalid auto-transcribe value to fail")
	}
}

func TestNewMessageStoreUsesConfiguredDirectory(t *testing.T) {
	storeDir := filepath.Join(t.TempDir(), "account-store")
	store, err := NewMessageStore(storeDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	if store.storeDir != storeDir {
		t.Fatalf("store directory mismatch: %q != %q", store.storeDir, storeDir)
	}
	if _, err := os.Stat(filepath.Join(storeDir, "messages.db")); err != nil {
		t.Fatalf("configured messages database was not created: %v", err)
	}
}

func TestAcquireInstanceLockRejectsConcurrentStore(t *testing.T) {
	storeDir := filepath.Join(t.TempDir(), "locked-store")
	first, err := acquireInstanceLock(storeDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		syscall.Flock(int(first.Fd()), syscall.LOCK_UN)
		first.Close()
	})

	if second, err := acquireInstanceLock(storeDir); err == nil {
		second.Close()
		t.Fatal("expected a concurrent bridge lock to fail")
	}
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

// Reentrega da mesma mensagem não pode mover o timestamp: era o que deixava a conversa
// eternamente não lida no Clauditor (auto-reply de escritório reentregue de 8 em 8 min).
func TestStoreMessageKeepsFirstTimestamp(t *testing.T) {
	s := testStore(t)
	jid := "5527998760170@s.whatsapp.net"
	primeira := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	if err := s.TouchChat(jid, "Contato", primeira); err != nil {
		t.Fatal(err)
	}
	if err := s.StoreMessage("MSGID3", jid, jid, "agradece seu contato", primeira, false,
		"", "", "", nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}

	// mesma mensagem chegando de novo, agora com a hora da reentrega e a legenda completa
	if err := s.StoreMessage("MSGID3", jid, jid, "agradece seu contato, como podemos ajudar?",
		primeira.Add(time.Hour), false, "", "", "", nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}

	var ts time.Time
	var content string
	var n int
	if err := s.db.QueryRow(
		"SELECT timestamp, content, (SELECT COUNT(*) FROM messages) FROM messages WHERE id = 'MSGID3'").
		Scan(&ts, &content, &n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("reentrega devia atualizar a linha, não criar outra: %d linhas", n)
	}
	if !ts.Equal(primeira) {
		t.Errorf("timestamp andou com a reentrega: %v != %v", ts, primeira)
	}
	if content != "agradece seu contato, como podemos ajudar?" {
		t.Errorf("conteúdo não foi atualizado: %q", content)
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

func TestWhatsAppSendSafeHours(t *testing.T) {
	local := time.FixedZone("America/Sao_Paulo", -3*60*60)
	cases := []struct {
		hour, minute int
		allowed      bool
	}{
		{8, 59, false},
		{9, 0, true},
		{19, 59, true},
		{20, 0, false},
	}
	for _, tc := range cases {
		when := time.Date(2026, 8, 5, tc.hour, tc.minute, 0, 0, local)
		if got := whatsappSendAllowedAt(when); got != tc.allowed {
			t.Errorf("%02d:%02d allowed=%v, want %v", tc.hour, tc.minute, got, tc.allowed)
		}
	}
	if got := whatsappQuietHoursMessage(); got !=
		"envio de WhatsApp bloqueado fora do horário seguro (09:00–19:59, horário local)" {
		t.Fatalf("mensagem inesperada: %q", got)
	}
}
