package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
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
			quoted_id TEXT, quoted_sender TEXT, quoted_text TEXT, transcript TEXT,
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
		"", "", "", nil, nil, nil, 0, "", "", ""); err != nil {
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
		"", "", "", nil, nil, nil, 0, "", "", ""); err != nil {
		t.Fatal(err)
	}

	// mesma mensagem chegando de novo, agora com a hora da reentrega e a legenda completa
	if err := s.StoreMessage("MSGID3", jid, jid, "agradece seu contato, como podemos ajudar?",
		primeira.Add(time.Hour), false, "", "", "", nil, nil, nil, 0, "", "", ""); err != nil {
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

// Citação: o que a pessoa respondeu tem que sobreviver ao store, senão o leitor recebe
// "concordo" sem saber com o quê. A reentrega não pode apagar a citação já gravada.
func TestStoreMessageKeepsQuoted(t *testing.T) {
	s := testStore(t)
	jid := "120363000000000000@g.us"
	now := time.Now().UTC().Truncate(time.Second)
	if err := s.TouchChat(jid, "Grupo", now); err != nil {
		t.Fatal(err)
	}
	if err := s.StoreMessage("MSGQ1", jid, "5527999999999", "concordo", now, false,
		"", "", "", nil, nil, nil, 0, "MSGORIG", "5527998888888", "vamos subir sexta?"); err != nil {
		t.Fatal(err)
	}
	var qid, qsender, qtext string
	if err := s.db.QueryRow(
		"SELECT quoted_id, quoted_sender, quoted_text FROM messages WHERE id = 'MSGQ1'").
		Scan(&qid, &qsender, &qtext); err != nil {
		t.Fatal(err)
	}
	if qid != "MSGORIG" || qsender != "5527998888888" || qtext != "vamos subir sexta?" {
		t.Errorf("citação não sobreviveu: %q / %q / %q", qid, qsender, qtext)
	}
}

// A transcrição fica na linha do áudio e sobrevive à reentrega da mesma mensagem
// (o UPSERT do StoreMessage não toca a coluna).
func TestSetTranscriptSurvivesRedelivery(t *testing.T) {
	s := testStore(t)
	jid := "5527999999999@s.whatsapp.net"
	now := time.Now().UTC().Truncate(time.Second)
	if err := s.TouchChat(jid, "Marco", now); err != nil {
		t.Fatal(err)
	}
	store := func() {
		if err := s.StoreMessage("AUD1", jid, "5527999999999", "", now, false,
			"audio", "a.ogg", "u", nil, nil, nil, 10, "", "", ""); err != nil {
			t.Fatal(err)
		}
	}
	store()
	if err := s.SetTranscript("AUD1", jid, "oi, tudo bem?"); err != nil {
		t.Fatal(err)
	}
	store() // reentrega
	var tr sql.NullString
	if err := s.db.QueryRow("SELECT transcript FROM messages WHERE id = 'AUD1'").Scan(&tr); err != nil {
		t.Fatal(err)
	}
	if tr.String != "oi, tudo bem?" {
		t.Errorf("transcrição sumiu na reentrega: %q", tr.String)
	}
}

// Sem o TouchChat antes, a FK rejeita — a regressão que o comentário no envio previne.
func TestStoreMessageNeedsChatRow(t *testing.T) {
	s := testStore(t)
	if err := s.StoreMessage("MSGID2", "5511888888888@s.whatsapp.net", "eu", "oi",
		time.Now(), true, "", "", "", nil, nil, nil, 0, "", "", ""); err == nil {
		t.Error("esperava erro de FK para chat inexistente")
	}
}

func TestStoreReactionUpsertAndClear(t *testing.T) {
	dir := t.TempDir()
	s, err := NewMessageStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	jid, mid, who := "5511999999999@s.whatsapp.net", "MSG1", "5511888888888"
	now := time.Now().UTC().Truncate(time.Second)
	if err := s.StoreReaction(jid, mid, who, "👍", now, false); err != nil {
		t.Fatal(err)
	}
	var emoji string
	var n int
	if err := s.db.QueryRow(
		`SELECT emoji, (SELECT COUNT(*) FROM reactions) FROM reactions WHERE message_id = ?`, mid).
		Scan(&emoji, &n); err != nil {
		t.Fatal(err)
	}
	if emoji != "👍" || n != 1 {
		t.Fatalf("primeira reação: emoji=%q n=%d", emoji, n)
	}
	if err := s.StoreReaction(jid, mid, who, "❤️", now.Add(time.Second), false); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(
		`SELECT emoji, (SELECT COUNT(*) FROM reactions) FROM reactions WHERE message_id = ?`, mid).
		Scan(&emoji, &n); err != nil {
		t.Fatal(err)
	}
	if emoji != "❤️" || n != 1 {
		t.Fatalf("troca de emoji tinha de atualizar a linha: emoji=%q n=%d", emoji, n)
	}
	if err := s.StoreReaction(jid, mid, who, "", now.Add(2*time.Second), false); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM reactions`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("emoji vazio tinha de apagar a reação: %d linhas", n)
	}
}

func TestIncomingReactionPlain(t *testing.T) {
	ev := &struct {
		Message *waProto.Message
	}{Message: &waProto.Message{
		ReactionMessage: &waProto.ReactionMessage{
			Key:  &waProto.MessageKey{ID: proto.String("ALVO")},
			Text: proto.String("😂"),
		},
	}}
	// incomingReaction precisa de *events.Message; o proto é o que importa aqui.
	target, emoji, ok := protoReaction(ev.Message)
	if !ok || target != "ALVO" || emoji != "😂" {
		t.Fatalf("protoReaction: ok=%v target=%q emoji=%q", ok, target, emoji)
	}
	if _, _, ok := protoReaction(&waProto.Message{}); ok {
		t.Fatal("mensagem sem reação não pode parecer reação")
	}
	if !validReactionEmoji("") || !validReactionEmoji("👍") || validReactionEmoji("x\ny") || !validReactionEmoji("❤️") {
		t.Fatal("validReactionEmoji recusou o que era pra aceitar (ou aceitou lixo)")
	}
	if validReactionEmoji("123456789") {
		t.Fatal("reação longa demais passou")
	}
}

func TestTargetSenderJID(t *testing.T) {
	dm := types.NewJID("5511999", types.DefaultUserServer)
	if got := targetSenderJID(dm, true, "x"); !got.IsEmpty() {
		t.Fatalf("própria mensagem 1:1: queria EmptyJID, veio %s", got)
	}
	if got := targetSenderJID(dm, false, "x"); got != dm {
		t.Fatalf("mensagem deles 1:1: queria o chat, veio %s", got)
	}
	grp := types.NewJID("120363", types.GroupServer)
	got := targetSenderJID(grp, false, "5511888")
	if got.User != "5511888" || got.Server != types.DefaultUserServer {
		t.Fatalf("participante de grupo: %s", got)
	}
}

func TestWhatsAppSendSafeHours(t *testing.T) {
	local := time.FixedZone("America/Sao_Paulo", -3*60*60)
	// bordas DERIVADAS das constantes: o commit que trocou a janela 09-20 para 06-22
	// (12/08) esqueceu deste teste e ele ficou vermelho em silêncio. Assim não repete.
	cases := []struct {
		hour, minute int
		allowed      bool
	}{
		{(whatsappSendStartHour + 23) % 24, 59, false},
		{whatsappSendStartHour, 0, true},
		{(whatsappSendEndHour - 1) % 24, 59, true},
		{whatsappSendEndHour % 24, 0, false}, // fim é exclusivo (24 => 00:00 é madrugada)
	}
	for _, tc := range cases {
		when := time.Date(2026, 8, 5, tc.hour, tc.minute, 0, 0, local)
		if got := whatsappSendAllowedAt(when); got != tc.allowed {
			t.Errorf("%02d:%02d allowed=%v, want %v", tc.hour, tc.minute, got, tc.allowed)
		}
	}
	want := fmt.Sprintf("envio de WhatsApp bloqueado fora do horário seguro (%02d:00–%02d:59, horário local)",
		whatsappSendStartHour, whatsappSendEndHour-1)
	if got := whatsappQuietHoursMessage(); got != want {
		t.Fatalf("mensagem inesperada: %q (queria %q)", got, want)
	}
}

// O ContextInfo não mora num campo só: cada tipo de mensagem carrega o seu. Este teste
// é a garantia de que texto E mídia com citação são reconhecidos, e de que ContextInfo
// sem stanza id (menção, encaminhamento) NÃO vira citação inventada.
func TestQuotedContextInfo(t *testing.T) {
	quoted := &waProto.Message{Conversation: proto.String("vamos subir sexta?")}
	ctx := func() *waProto.ContextInfo {
		return &waProto.ContextInfo{
			StanzaID:      proto.String("ORIG1"),
			Participant:   proto.String("5527998888888@s.whatsapp.net"),
			QuotedMessage: quoted,
		}
	}
	cases := map[string]*waProto.Message{
		"texto": {ExtendedTextMessage: &waProto.ExtendedTextMessage{
			Text: proto.String("concordo"), ContextInfo: ctx()}},
		"imagem": {ImageMessage: &waProto.ImageMessage{
			Caption: proto.String("essa aqui"), ContextInfo: ctx()}},
		"audio": {AudioMessage: &waProto.AudioMessage{ContextInfo: ctx()}},
		"documento": {DocumentMessage: &waProto.DocumentMessage{ContextInfo: ctx()}},
	}
	for kind, msg := range cases {
		ci := quotedContextInfo(msg)
		if ci == nil {
			t.Fatalf("%s: citação não reconhecida", kind)
		}
		if ci.GetStanzaID() != "ORIG1" || ci.GetParticipant() != "5527998888888@s.whatsapp.net" {
			t.Errorf("%s: id/autor errados: %q / %q", kind, ci.GetStanzaID(), ci.GetParticipant())
		}
		if got := extractTextContent(ci.GetQuotedMessage()); got != "vamos subir sexta?" {
			t.Errorf("%s: texto da citada veio %q", kind, got)
		}
	}

	semCitacao := []*waProto.Message{
		nil,
		{Conversation: proto.String("oi")},
		{ExtendedTextMessage: &waProto.ExtendedTextMessage{ // menção: ContextInfo sem stanza id
			Text: proto.String("@5527 bom dia"), ContextInfo: &waProto.ContextInfo{
				MentionedJID: []string{"5527998888888@s.whatsapp.net"}}}},
	}
	for i, msg := range semCitacao {
		if ci := quotedContextInfo(msg); ci != nil {
			t.Errorf("caso %d: inventou citação (stanza %q)", i, ci.GetStanzaID())
		}
	}
}
