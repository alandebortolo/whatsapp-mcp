package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"mime"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mdp/qrterminal"

	"bytes"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/proto/waMmsRetry"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

const (
	defaultStoreDir   = "store"
	defaultBridgePort = 8080
	// Janela de envio automático (decisão do Alan, 14/08/2026: era 06-22 e travava demais).
	// Os mesmos números vivem no Clauditor (core.SEND_START_HOUR/SEND_END_HOUR) e na regra
	// dura do ~/.claude/CLAUDE.md — mudar aqui pede mudar lá. Fim é EXCLUSIVO: 24 = 23:59.
	whatsappSendStartHour = 5
	whatsappSendEndHour   = 24
)

func whatsappSendAllowedAt(now time.Time) bool {
	hour := now.Hour()
	return hour >= whatsappSendStartHour && hour < whatsappSendEndHour
}

func whatsappQuietHoursMessage() string {
	return fmt.Sprintf("envio de WhatsApp bloqueado fora do horário seguro (%02d:00–%02d:59, horário local)",
		whatsappSendStartHour, whatsappSendEndHour-1)
}

type runtimeConfig struct {
	AccountName    string
	StoreDir       string
	QRPath         string
	BindAddress    string
	Port           int
	AutoTranscribe bool
}

func loadRuntimeConfig() (runtimeConfig, error) {
	cfg := runtimeConfig{
		AccountName:    strings.TrimSpace(os.Getenv("WHATSAPP_ACCOUNT_NAME")),
		StoreDir:       strings.TrimSpace(os.Getenv("WHATSAPP_STORE_DIR")),
		QRPath:         strings.TrimSpace(os.Getenv("WHATSAPP_QR_PATH")),
		BindAddress:    strings.TrimSpace(os.Getenv("WHATSAPP_BRIDGE_BIND")),
		Port:           defaultBridgePort,
		AutoTranscribe: true,
	}
	if cfg.AccountName == "" {
		cfg.AccountName = "whatsapp"
	}
	if cfg.StoreDir == "" {
		cfg.StoreDir = defaultStoreDir
	}
	if cfg.QRPath == "" {
		if cfg.StoreDir == defaultStoreDir {
			cfg.QRPath = "qr.txt"
		} else {
			cfg.QRPath = filepath.Join(filepath.Dir(cfg.StoreDir), "qr.txt")
		}
	}
	if raw := strings.TrimSpace(os.Getenv("WHATSAPP_BRIDGE_PORT")); raw != "" {
		port, err := strconv.Atoi(raw)
		if err != nil || port < 1 || port > 65535 {
			return runtimeConfig{}, fmt.Errorf("invalid WHATSAPP_BRIDGE_PORT %q", raw)
		}
		cfg.Port = port
	}
	if raw := strings.TrimSpace(os.Getenv("WHATSAPP_AUTO_TRANSCRIBE")); raw != "" {
		enabled, err := strconv.ParseBool(raw)
		if err != nil {
			return runtimeConfig{}, fmt.Errorf("invalid WHATSAPP_AUTO_TRANSCRIBE %q", raw)
		}
		cfg.AutoTranscribe = enabled
	}
	return cfg, nil
}

func sqliteDSN(path string) string {
	return "file:" + filepath.ToSlash(path) + "?_foreign_keys=on"
}

func acquireInstanceLock(storeDir string) (*os.File, error) {
	if err := os.MkdirAll(storeDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create store directory: %v", err)
	}
	lockPath := filepath.Join(storeDir, "bridge.lock")
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open instance lock: %v", err)
	}
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lockFile.Close()
		return nil, fmt.Errorf("another bridge is already using store %s", storeDir)
	}
	return lockFile, nil
}

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
	db       *sql.DB
	storeDir string
}

// Initialize message store
func NewMessageStore(storeDir string) (*MessageStore, error) {
	// Create directory for database if it doesn't exist
	if err := os.MkdirAll(storeDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create store directory: %v", err)
	}

	// Open SQLite database for messages
	db, err := sql.Open("sqlite3", sqliteDSN(filepath.Join(storeDir, "messages.db")))
	if err != nil {
		return nil, fmt.Errorf("failed to open message database: %v", err)
	}

	// Create tables if they don't exist
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS chats (
			jid TEXT PRIMARY KEY,
			name TEXT,
			last_message_time TIMESTAMP
		);
		
		CREATE TABLE IF NOT EXISTS messages (
			id TEXT,
			chat_jid TEXT,
			sender TEXT,
			content TEXT,
			timestamp TIMESTAMP,
			is_from_me BOOLEAN,
			media_type TEXT,
			filename TEXT,
			url TEXT,
			media_key BLOB,
			file_sha256 BLOB,
			file_enc_sha256 BLOB,
			file_length INTEGER,
			quoted_id TEXT,
			quoted_sender TEXT,
			quoted_text TEXT,
			ack INTEGER,
			PRIMARY KEY (id, chat_jid),
			FOREIGN KEY (chat_jid) REFERENCES chats(jid)
		);
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create tables: %v", err)
	}

	// Banco que já existe não ganha coluna pelo CREATE IF NOT EXISTS acima: as três
	// colunas de citação nasceram depois dos stores em produção. ponytail: ALTER que
	// já rodou devolve "duplicate column name" — erro esperado, não é falha de migração.
	// transcript: texto da transcrição automática do áudio (Whisper local), na PRÓPRIA linha
	// do áudio — é daqui que o Clauditor lê em vez de rodar o whisper de novo.
	// ack: recibo de entrega/leitura das NOSSAS mensagens (1=servidor, 2=entregue, 3=lida).
	// revoked_at: quando a mensagem foi apagada "para todos". A linha FICA — o leitor marca
	// "apagada" em cima do conteúdo, que continua visível (decisão do Alan, 03/09/2026).
	for _, col := range []string{"quoted_id TEXT", "quoted_sender TEXT", "quoted_text TEXT", "transcript TEXT", "ack INTEGER", "revoked_at TIMESTAMP"} {
		db.Exec("ALTER TABLE messages ADD COLUMN " + col)
	}

	// Reação não é mensagem: uma por pessoa por alvo (o WhatsApp só deixa uma). Emoji
	// vazio = a pessoa tirou a reação. Sem FK pra messages: a alvo pode não estar no
	// store (anterior à ponte, ou history sync ainda não trouxe).
	if _, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS reactions (
			chat_jid TEXT NOT NULL,
			message_id TEXT NOT NULL,
			sender TEXT NOT NULL,
			emoji TEXT NOT NULL,
			timestamp TIMESTAMP,
			is_from_me BOOLEAN,
			PRIMARY KEY (chat_jid, message_id, sender)
		);
	`); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create reactions table: %v", err)
	}

	return &MessageStore{db: db, storeDir: storeDir}, nil
}

// Close the database connection
func (store *MessageStore) Close() error {
	return store.db.Close()
}

// Store a chat in the database
func (store *MessageStore) StoreChat(jid, name string, lastMessageTime time.Time) error {
	_, err := store.db.Exec(
		"INSERT OR REPLACE INTO chats (jid, name, last_message_time) VALUES (?, ?, ?)",
		jid, name, lastMessageTime,
	)
	return err
}

// Marca atividade no chat sem destruir o nome já conhecido: o nome só é gravado
// quando a linha nasce (chat novo, iniciado por nós). StoreChat é INSERT OR REPLACE
// e sobrescreveria um nome bom pelo fallback, então não serve pro caminho de envio.
func (store *MessageStore) TouchChat(jid, fallbackName string, lastMessageTime time.Time) error {
	_, err := store.db.Exec(
		`INSERT INTO chats (jid, name, last_message_time) VALUES (?, ?, ?)
		 ON CONFLICT(jid) DO UPDATE SET last_message_time = excluded.last_message_time`,
		jid, fallbackName, lastMessageTime,
	)
	return err
}

// Store a message in the database
func (store *MessageStore) StoreMessage(id, chatJID, sender, content string, timestamp time.Time, isFromMe bool,
	mediaType, filename, url string, mediaKey, fileSHA256, fileEncSHA256 []byte, fileLength uint64,
	quotedID, quotedSender, quotedText string) error {
	// Only store if there's actual content or media
	if content == "" && mediaType == "" {
		return nil
	}

	// Reentrega da MESMA mensagem (mesmo id no mesmo chat) não é mensagem nova: atualiza
	// conteúdo e mídia, mas o timestamp continua o da primeira entrega. Com INSERT OR REPLACE
	// ele pulava para a hora da reentrega, e isso quebrava dois leitores: a mensagem antiga
	// subia para o topo da conversa e o Clauditor desmarcava "lida" (a marca dele é o maior
	// timestamp já visto no chat, então um timestamp que anda deixa a conversa não lida para
	// sempre). Quem reentrega é o remetente insistindo em mensagem sem recibo de leitura.
	// ack não entra no UPSERT: um recibo já gravado (entregue/lida) não volta pra "enviada".
	ack := 0
	if isFromMe {
		ack = 1 // chegou no servidor (SendMessage ok, ou veio do celular via events.Message)
	}
	_, err := store.db.Exec(
		`INSERT INTO messages
		(id, chat_jid, sender, content, timestamp, is_from_me, media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length, quoted_id, quoted_sender, quoted_text, ack)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id, chat_jid) DO UPDATE SET
			sender = excluded.sender, content = excluded.content, is_from_me = excluded.is_from_me,
			media_type = excluded.media_type, filename = excluded.filename, url = excluded.url,
			media_key = excluded.media_key, file_sha256 = excluded.file_sha256,
			file_enc_sha256 = excluded.file_enc_sha256, file_length = excluded.file_length,
			quoted_id = excluded.quoted_id, quoted_sender = excluded.quoted_sender,
			quoted_text = excluded.quoted_text`,
		id, chatJID, sender, content, timestamp, isFromMe, mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength,
		quotedID, quotedSender, quotedText, ack,
	)
	return err
}

// SetAck sobe o recibo da mensagem (1=enviada, 2=entregue, 3=lida). Nunca desce:
// entregue depois de lida é ruído, e a reentrega do StoreMessage não pode apagar.
func (store *MessageStore) SetAck(id, chatJID string, ack int) error {
	if id == "" || chatJID == "" || ack < 1 || ack > 3 {
		return nil
	}
	_, err := store.db.Exec(
		`UPDATE messages SET ack = ? WHERE id = ? AND chat_jid = ? AND is_from_me = 1 AND IFNULL(ack, 0) < ?`,
		ack, id, chatJID, ack)
	return err
}

// receiptAck traduz o tipo do whatsmeow pro inteiro que a UI desenha.
// Delivered chega com type vazio; played de áudio conta como lida (é o que o
// celular mostra). O resto (retry, sender, inactive…) não muda o tick.
func receiptAck(t types.ReceiptType) int {
	switch t {
	case types.ReceiptTypeDelivered:
		return 2
	case types.ReceiptTypeRead, types.ReceiptTypePlayed:
		return 3
	default:
		return 0
	}
}

// SetTranscript grava a transcrição do áudio na linha dele. Fica fora do UPSERT do
// StoreMessage de propósito: reentrega da mesma mensagem não pode apagar o texto.
func (store *MessageStore) SetTranscript(id, chatJID, text string) error {
	_, err := store.db.Exec(
		`UPDATE messages SET transcript = ? WHERE id = ? AND chat_jid = ?`, text, id, chatJID)
	return err
}

// SetRevoked marca a mensagem como apagada "para todos" sem tirar a linha: o WhatsApp
// oficial some com ela, aqui ela fica legível com a marca. Só a primeira revogação conta
// (reentrega do mesmo ProtocolMessage não anda o horário). Alvo fora do store = 0 linhas,
// sem erro: apagou algo que a ponte nunca viu.
func (store *MessageStore) SetRevoked(id, chatJID string, ts time.Time) (int64, error) {
	if id == "" || chatJID == "" {
		return 0, nil
	}
	res, err := store.db.Exec(
		`UPDATE messages SET revoked_at = ? WHERE id = ? AND chat_jid = ? AND revoked_at IS NULL`,
		ts, id, chatJID)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// applyRevoke trata o "apagar para todos", que chega como ProtocolMessage REVOKE com a
// chave da mensagem alvo (do outro lado, ou do celular do dono). ok=false: não era
// revogação. Vem ANTES do StoreChat no handleMessage: apagar não é mensagem nova, não
// bumpa a conversa nem vira nudge.
func applyRevoke(store *MessageStore, chatJID string, msg *events.Message, logger waLog.Logger) bool {
	if msg == nil || msg.Message == nil {
		return false
	}
	p := msg.Message.GetProtocolMessage()
	if p == nil || p.GetType() != waProto.ProtocolMessage_REVOKE {
		return false
	}
	id := p.GetKey().GetID()
	if id == "" {
		return true
	}
	n, err := store.SetRevoked(id, chatJID, msg.Info.Timestamp)
	if err != nil {
		logger.Warnf("revogação de %s em %s não gravada: %v", id, chatJID, err)
	} else if n == 0 {
		logger.Infof("revogação de %s em %s: mensagem alvo não está no store", id, chatJID)
	}
	return true
}

// StoreReaction grava ou apaga a reação de UMA pessoa numa mensagem. Emoji vazio
// (o que o WhatsApp manda quando a pessoa tira) apaga a linha. Uma pessoa só tem
// uma reação por alvo: o UPSERT substitui o emoji anterior.
func (store *MessageStore) StoreReaction(chatJID, messageID, sender, emoji string, ts time.Time, fromMe bool) error {
	if messageID == "" || sender == "" {
		return nil
	}
	if strings.TrimSpace(emoji) == "" {
		_, err := store.db.Exec(
			`DELETE FROM reactions WHERE chat_jid = ? AND message_id = ? AND sender = ?`,
			chatJID, messageID, sender)
		return err
	}
	_, err := store.db.Exec(
		`INSERT INTO reactions (chat_jid, message_id, sender, emoji, timestamp, is_from_me)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(chat_jid, message_id, sender) DO UPDATE SET
			emoji = excluded.emoji, timestamp = excluded.timestamp, is_from_me = excluded.is_from_me`,
		chatJID, messageID, sender, emoji, ts, fromMe)
	return err
}

// GetMessageMeta devolve de quem é a mensagem alvo (pra montar o MessageKey da reação).
func (store *MessageStore) GetMessageMeta(id, chatJID string) (fromMe bool, sender string, ok bool) {
	err := store.db.QueryRow(
		`SELECT is_from_me, IFNULL(sender, '') FROM messages WHERE id = ? AND chat_jid = ?`,
		id, chatJID,
	).Scan(&fromMe, &sender)
	return fromMe, sender, err == nil
}

// GetMessageQuote é o GetMessageMeta + o texto: o retrato mínimo que o ContextInfo de uma
// resposta-citação precisa (StanzaID vem do caller, Participant do sender, QuotedMessage do content).
func (store *MessageStore) GetMessageQuote(id, chatJID string) (fromMe bool, sender, content string, ok bool) {
	err := store.db.QueryRow(
		`SELECT is_from_me, IFNULL(sender, ''), IFNULL(content, '') FROM messages WHERE id = ? AND chat_jid = ?`,
		id, chatJID,
	).Scan(&fromMe, &sender, &content)
	return fromMe, sender, content, err == nil
}

// Get messages from a chat
func (store *MessageStore) GetMessages(chatJID string, limit int) ([]Message, error) {
	rows, err := store.db.Query(
		"SELECT sender, content, timestamp, is_from_me, media_type, filename FROM messages WHERE chat_jid = ? ORDER BY timestamp DESC LIMIT ?",
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

// Extract text content from a message
func extractTextContent(msg *waProto.Message) string {
	if msg == nil {
		return ""
	}
	msg = unwrapMessage(msg)

	// Try to get text content
	if text := msg.GetConversation(); text != "" {
		return text
	} else if extendedText := msg.GetExtendedTextMessage(); extendedText != nil {
		return extendedText.GetText()
	}

	// Media messages carry their text in the Caption field
	if img := msg.GetImageMessage(); img != nil {
		return img.GetCaption()
	} else if vid := msg.GetVideoMessage(); vid != nil {
		return vid.GetCaption()
	} else if doc := msg.GetDocumentMessage(); doc != nil {
		return doc.GetCaption()
	}

	// Cartão de contato (vCard) não tem Conversation nem mídia: descartado calado,
	// o número que a pessoa mandou some da conversa (medido 26/08: 8 cartões
	// perdidos num dia). Vira texto legível — o que interessa é nome/tel/e-mail.
	if c := msg.GetContactMessage(); c != nil {
		return contactText(c.GetDisplayName(), c.GetVcard())
	}
	if arr := msg.GetContactsArrayMessage(); arr != nil {
		var parts []string
		for _, c := range arr.GetContacts() {
			if t := contactText(c.GetDisplayName(), c.GetVcard()); t != "" {
				parts = append(parts, t)
			}
		}
		if len(parts) > 0 {
			return strings.Join(parts, "\n")
		}
	}

	return interactiveText(msg)
}

// contactText resume um vCard numa linha: "[contato] Nome: tel | e-mail".
// Só TEL/EMAIL interessam pra conversa; o valor vem depois do primeiro ":".
func contactText(name, vcard string) string {
	var fields []string
	for _, line := range strings.Split(vcard, "\n") {
		u := strings.ToUpper(strings.TrimSpace(line))
		if strings.HasPrefix(u, "TEL") || strings.HasPrefix(u, "EMAIL") {
			if kv := strings.SplitN(line, ":", 2); len(kv) == 2 && strings.TrimSpace(kv[1]) != "" {
				fields = append(fields, strings.TrimSpace(kv[1]))
			}
		}
	}
	out := strings.TrimSpace(name)
	if len(fields) > 0 {
		if out != "" {
			out += ": "
		}
		out += strings.Join(fields, " | ")
	}
	if out == "" {
		return ""
	}
	return "[contato] " + out
}

// Efêmera, ver-uma-vez e editada não são um tipo de mensagem: são um envelope em volta
// da mensagem de verdade. O whatsmeow desembrulha o que chega ao vivo, mas o que vem do
// history sync e a mensagem CITADA chegam embrulhadas — e olhar só a casca dá texto vazio.
func unwrapMessage(msg *waProto.Message) *waProto.Message {
	for i := 0; i < 4 && msg != nil; i++ { // teto: envelope dentro de envelope não passa disso
		switch {
		case msg.GetEphemeralMessage().GetMessage() != nil:
			msg = msg.GetEphemeralMessage().GetMessage()
		case msg.GetViewOnceMessage().GetMessage() != nil:
			msg = msg.GetViewOnceMessage().GetMessage()
		case msg.GetViewOnceMessageV2().GetMessage() != nil:
			msg = msg.GetViewOnceMessageV2().GetMessage()
		case msg.GetDocumentWithCaptionMessage().GetMessage() != nil:
			msg = msg.GetDocumentWithCaptionMessage().GetMessage()
		case msg.GetEditedMessage().GetMessage() != nil:
			msg = msg.GetEditedMessage().GetMessage()
		default:
			return msg
		}
	}
	return msg
}

// Menu de bot de atendimento (Asaas, Cloud API da Meta e afins) não tem texto em
// Conversation: o corpo mora num campo próprio e as opções em outro. Sem isto a
// mensagem inteira chegava vazia e o handler a DESCARTAVA — a conversa virava um
// monólogo com buracos, e o agente, que nunca viu o menu, respondia texto livre e
// levava "Opção inválida" de volta. Vale para os dois lados: a escolha de quem
// clica no botão também é uma mensagem sem Conversation.
//
// Formato: corpo em cima, opções numa linha "[menu] a | b | c" — legível pra pessoa
// no dashboard e pro agente que precisa escolher uma delas.
func interactiveText(msg *waProto.Message) string {
	var body, opts []string
	addBody := func(parts ...string) {
		for _, s := range parts {
			if s = strings.TrimSpace(s); s != "" {
				body = append(body, s)
			}
		}
	}
	addOpt := func(title, desc string) {
		title, desc = strings.TrimSpace(title), strings.TrimSpace(desc)
		if title == "" {
			title = desc
			desc = ""
		}
		if title == "" {
			return
		}
		if desc != "" {
			title += " (" + desc + ")"
		}
		opts = append(opts, title)
	}

	switch {
	// --- menus ---
	case msg.GetButtonsMessage() != nil:
		b := msg.GetButtonsMessage()
		addBody(b.GetText(), b.GetContentText(), b.GetFooterText())
		for _, btn := range b.GetButtons() {
			addOpt(btn.GetButtonText().GetDisplayText(), "")
		}
	case msg.GetListMessage() != nil:
		l := msg.GetListMessage()
		addBody(l.GetTitle(), l.GetDescription(), l.GetFooterText())
		for _, sec := range l.GetSections() {
			for _, row := range sec.GetRows() {
				addOpt(row.GetTitle(), row.GetDescription())
			}
		}
	case msg.GetInteractiveMessage() != nil:
		b, o := interactiveParts(msg.GetInteractiveMessage())
		addBody(b...)
		opts = append(opts, o...)
	case msg.GetTemplateMessage() != nil:
		t := msg.GetTemplateMessage()
		if inner := t.GetInteractiveMessageTemplate(); inner != nil {
			b, o := interactiveParts(inner)
			addBody(b...)
			opts = append(opts, o...)
		}
		if h := t.GetHydratedTemplate(); h != nil {
			addBody(h.GetHydratedTitleText(), h.GetHydratedContentText(), h.GetHydratedFooterText())
			for _, btn := range h.GetHydratedButtons() {
				switch {
				case btn.GetQuickReplyButton() != nil:
					addOpt(btn.GetQuickReplyButton().GetDisplayText(), "")
				case btn.GetUrlButton() != nil:
					addOpt(btn.GetUrlButton().GetDisplayText(), btn.GetUrlButton().GetURL())
				case btn.GetCallButton() != nil:
					addOpt(btn.GetCallButton().GetDisplayText(), btn.GetCallButton().GetPhoneNumber())
				}
			}
		}
	// Enquete some pelo mesmo motivo (texto fora de Conversation); as versões V2/V3
	// são o mesmo payload em campos diferentes, por migração do protocolo.
	case msg.GetPollCreationMessage() != nil || msg.GetPollCreationMessageV2() != nil || msg.GetPollCreationMessageV3() != nil:
		p := msg.GetPollCreationMessage()
		if p == nil {
			p = msg.GetPollCreationMessageV2()
		}
		if p == nil {
			p = msg.GetPollCreationMessageV3()
		}
		addBody(p.GetName())
		for _, opt := range p.GetOptions() {
			addOpt(opt.GetOptionName(), "")
		}

	// --- respostas (o que a pessoa escolheu; o texto É a resposta, sem "[menu]") ---
	case msg.GetButtonsResponseMessage() != nil:
		r := msg.GetButtonsResponseMessage()
		return firstNonEmpty(r.GetSelectedDisplayText(), r.GetSelectedButtonID())
	case msg.GetListResponseMessage() != nil:
		r := msg.GetListResponseMessage()
		return firstNonEmpty(r.GetTitle(), r.GetDescription(), r.GetSingleSelectReply().GetSelectedRowID())
	case msg.GetTemplateButtonReplyMessage() != nil:
		r := msg.GetTemplateButtonReplyMessage()
		return firstNonEmpty(r.GetSelectedDisplayText(), r.GetSelectedID())
	case msg.GetInteractiveResponseMessage() != nil:
		r := msg.GetInteractiveResponseMessage()
		if t := strings.TrimSpace(r.GetBody().GetText()); t != "" {
			return t
		}
		p := parseFlowParams(r.GetNativeFlowResponseMessage().GetParamsJSON())
		return firstNonEmpty(p.DisplayText, p.Title, p.ID)
	}

	if len(opts) > 0 {
		body = append(body, "[menu] "+strings.Join(opts, " | "))
	}
	return strings.Join(body, "\n")
}

// InteractiveMessage é o menu da Cloud API: corpo em campos próprios e as opções
// escondidas num JSON por botão (native flow), cuja forma muda com o tipo do botão.
func interactiveParts(m *waProto.InteractiveMessage) (body, opts []string) {
	body = append(body, m.GetHeader().GetTitle(), m.GetHeader().GetSubtitle(), m.GetBody().GetText(), m.GetFooter().GetText())
	for _, btn := range m.GetNativeFlowMessage().GetButtons() {
		p := parseFlowParams(btn.GetButtonParamsJSON())
		if len(p.Sections) > 0 { // single_select: as opções vivem nas linhas das seções
			for _, sec := range p.Sections {
				for _, row := range sec.Rows {
					opts = append(opts, strings.TrimSpace(firstNonEmpty(row.Title, row.Description, row.ID)))
				}
			}
			continue
		}
		if t := strings.TrimSpace(firstNonEmpty(p.DisplayText, p.Title, btn.GetName())); t != "" {
			if p.URL != "" {
				t += " (" + p.URL + ")"
			}
			opts = append(opts, t)
		}
	}
	// Carrossel: cada card é um InteractiveMessage inteiro. Sem isto o card fica vazio.
	for _, card := range m.GetCarouselMessage().GetCards() {
		b, o := interactiveParts(card)
		body = append(body, b...)
		opts = append(opts, o...)
	}
	return body, opts
}

// Parâmetros do botão native flow. Um só struct para todos os tipos (quick_reply,
// cta_url, single_select…): campo que não existe naquele tipo fica vazio.
type flowParams struct {
	DisplayText string `json:"display_text"`
	Title       string `json:"title"`
	URL         string `json:"url"`
	ID          string `json:"id"`
	Sections    []struct {
		Title string `json:"title"`
		Rows  []struct {
			Title       string `json:"title"`
			Description string `json:"description"`
			ID          string `json:"id"`
		} `json:"rows"`
	} `json:"sections"`
}

func parseFlowParams(raw string) flowParams {
	var p flowParams
	if raw != "" {
		_ = json.Unmarshal([]byte(raw), &p) // JSON quebrado vira menu sem opção, não erro
	}
	return p
}

// Tipos que legitimamente não têm texto: não valem uma linha de log (o bridge.log já
// passa de 80 MB e o launchd não rotaciona nada). Qualquer outro tipo descartado é
// candidato a buraco na conversa, como foram os menus, e por isso vira warn.
var silentDrop = map[string]bool{
	"ProtocolMessage": true, "PollUpdateMessage": true,
	"SenderKeyDistributionMessage": true, "StickerSyncRmrMessage": true,
	"KeepInChatMessage": true, "PinInChatMessage": true,
	"EventResponseMessage": true, "Call": true,
}

// incomingReaction lê ReactionMessage (1:1 e grupo comum) ou EncReactionMessage
// (comunidade). ok=false: não era reação, ou a criptografada não abriu.
func incomingReaction(client *whatsmeow.Client, ev *events.Message) (targetID, emoji string, ok bool) {
	if ev == nil || ev.Message == nil {
		return "", "", false
	}
	if r := ev.Message.GetReactionMessage(); r != nil {
		if key := r.GetKey(); key != nil && key.GetID() != "" {
			return key.GetID(), r.GetText(), true
		}
		return "", "", false
	}
	if ev.Message.GetEncReactionMessage() == nil || client == nil {
		return "", "", false
	}
	dec, err := client.DecryptReaction(context.Background(), ev)
	if err != nil || dec == nil {
		return "", "", false
	}
	if key := dec.GetKey(); key != nil && key.GetID() != "" {
		return key.GetID(), dec.GetText(), true
	}
	return "", "", false
}

// protoReaction é o caminho do history sync: só o proto, sem events.Message —
// EncReaction fica de fora (DecryptReaction precisa do envelope do evento).
func protoReaction(msg *waProto.Message) (targetID, emoji string, ok bool) {
	if msg == nil {
		return "", "", false
	}
	msg = unwrapMessage(msg)
	r := msg.GetReactionMessage()
	if r == nil {
		return "", "", false
	}
	key := r.GetKey()
	if key == nil || key.GetID() == "" {
		return "", "", false
	}
	return key.GetID(), r.GetText(), true
}

func validReactionEmoji(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return true // vazio = tirar a reação
	}
	if strings.ContainsAny(s, "\n\r\t") {
		return false
	}
	n := 0
	for range s {
		n++
		if n > 8 {
			return false
		}
	}
	return n > 0
}

func parseRecipientJID(recipient string) (types.JID, error) {
	if strings.Contains(recipient, "@") {
		return types.ParseJID(recipient)
	}
	return types.JID{User: recipient, Server: "s.whatsapp.net"}, nil
}

func targetSenderJID(chat types.JID, fromMe bool, sender string) types.JID {
	if fromMe {
		return types.EmptyJID
	}
	if chat.Server != types.GroupServer {
		return chat
	}
	if strings.Contains(sender, "@") {
		if j, err := types.ParseJID(sender); err == nil {
			return j
		}
	}
	if sender == "" {
		return types.EmptyJID
	}
	return types.NewJID(sender, types.DefaultUserServer)
}

// Nome do campo preenchido dentro de Message — só para o log dizer o que foi descartado.
func messageTypeName(msg *waProto.Message) string {
	if msg == nil {
		return "nil"
	}
	v := reflect.ValueOf(msg).Elem()
	for i := 0; i < v.NumField(); i++ {
		f := v.Type().Field(i)
		if !f.IsExported() || f.Name == "MessageContextInfo" {
			continue
		}
		if fv := v.Field(i); fv.Kind() == reflect.Ptr && !fv.IsNil() {
			return f.Name
		}
	}
	return "desconhecido"
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

// Responder citando ("reply") não é mensagem comum: quem é citado vive no ContextInfo,
// que o whatsmeow pendura em CADA tipo de mensagem em vez de num campo só. Sem isto o
// leitor recebe "concordo" solto e não sabe com o quê — em grupo, com três conversas
// entrelaçadas, isso é o texto inteiro perdendo o sentido. Guardamos o id da citada
// (casa com messages.id), quem a escreveu e um retrato do texto dela: o retrato cobre
// citação de mensagem que nunca entrou no nosso store (anterior à ponte, ou apagada).
func quotedContextInfo(msg *waProto.Message) *waProto.ContextInfo {
	if msg == nil {
		return nil
	}
	msg = unwrapMessage(msg)
	var ci *waProto.ContextInfo
	switch {
	case msg.GetExtendedTextMessage() != nil:
		ci = msg.GetExtendedTextMessage().GetContextInfo()
	case msg.GetImageMessage() != nil:
		ci = msg.GetImageMessage().GetContextInfo()
	case msg.GetVideoMessage() != nil:
		ci = msg.GetVideoMessage().GetContextInfo()
	case msg.GetAudioMessage() != nil:
		ci = msg.GetAudioMessage().GetContextInfo()
	case msg.GetDocumentMessage() != nil:
		ci = msg.GetDocumentMessage().GetContextInfo()
	case msg.GetStickerMessage() != nil:
		ci = msg.GetStickerMessage().GetContextInfo()
	// Resposta de menu é sempre uma citação da mensagem do menu — é isso que o
	// WhatsApp desenha em cima do "Já sou cliente" que a pessoa clicou.
	case msg.GetButtonsResponseMessage() != nil:
		ci = msg.GetButtonsResponseMessage().GetContextInfo()
	case msg.GetListResponseMessage() != nil:
		ci = msg.GetListResponseMessage().GetContextInfo()
	case msg.GetTemplateButtonReplyMessage() != nil:
		ci = msg.GetTemplateButtonReplyMessage().GetContextInfo()
	case msg.GetInteractiveResponseMessage() != nil:
		ci = msg.GetInteractiveResponseMessage().GetContextInfo()
	case msg.GetButtonsMessage() != nil:
		ci = msg.GetButtonsMessage().GetContextInfo()
	case msg.GetListMessage() != nil:
		ci = msg.GetListMessage().GetContextInfo()
	case msg.GetInteractiveMessage() != nil:
		ci = msg.GetInteractiveMessage().GetContextInfo()
	case msg.GetTemplateMessage() != nil:
		ci = msg.GetTemplateMessage().GetContextInfo()
	}
	if ci.GetStanzaID() == "" {
		return nil // ContextInfo sem stanza id é menção/encaminhamento, não citação
	}
	return ci
}

func extractQuoted(client *whatsmeow.Client, msg *waProto.Message) (id, sender, text string) {
	ci := quotedContextInfo(msg)
	if ci == nil {
		return "", "", ""
	}
	sender = ci.GetParticipant()
	if jid, err := types.ParseJID(sender); err == nil {
		sender = resolveJID(client, jid).User // mesmo formato de messages.sender
	}
	return ci.GetStanzaID(), sender, extractTextContent(ci.GetQuotedMessage())
}

// SendMessageResponse represents the response for the send message API
type SendMessageResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// SendMessageRequest represents the request body for the send message API
type SendMessageRequest struct {
	Recipient string   `json:"recipient"`
	Message   string   `json:"message"`
	MediaPath string   `json:"media_path,omitempty"`
	ReplyTo   string   `json:"reply_to,omitempty"` // id de mensagem do histórico local: envia CITANDO ela
	Mentions  []string `json:"mentions,omitempty"` // jids mencionados; o texto leva o token @<numero>
}

// Function to send a WhatsApp message
func sendWhatsAppMessage(client *whatsmeow.Client, messageStore *MessageStore, recipient string, message string, mediaPath string, replyTo string, mentions []string) (bool, string) {
	if !whatsappSendAllowedAt(time.Now()) {
		return false, whatsappQuietHoursMessage()
	}
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
			// ponytail: stdlib mime table instead of a hand-kept switch; octet-stream is last resort.
			// Wrong mime + missing FileName is what made PDFs arrive unopenable ("corrupted").
			mimeType = mime.TypeByExtension("." + fileExt)
			if mimeType == "" {
				mimeType = "application/octet-stream"
			}
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
				FileName:      proto.String(mediaPath[strings.LastIndex(mediaPath, "/")+1:]),
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
	} else if replyTo != "" || len(mentions) > 0 {
		// Resposta-citação e/ou menção: ambas moram no ContextInfo, que só existe no
		// ExtendedTextMessage — Conversation puro não carrega nenhum dos dois.
		ctxInfo := &waProto.ContextInfo{}
		if replyTo != "" {
			qFromMe, qSender, qText, ok := messageStore.GetMessageQuote(replyTo, recipientJID.String())
			if !ok {
				return false, fmt.Sprintf("reply_to %s não está no histórico local desta conversa", replyTo)
			}
			participant := qSender
			if qFromMe {
				participant = client.Store.ID.ToNonAD().String()
			} else if participant == "" {
				participant = recipientJID.String() // 1:1: o autor da citada é o próprio contato
			} else if !strings.Contains(participant, "@") {
				participant += "@s.whatsapp.net" // o store guarda só o user em grupo
			}
			ctxInfo.StanzaID = proto.String(replyTo)
			ctxInfo.Participant = proto.String(participant)
			ctxInfo.QuotedMessage = &waProto.Message{Conversation: proto.String(qText)}
		}
		for _, m := range mentions {
			m = strings.TrimSpace(m)
			if m == "" {
				continue
			}
			if !strings.Contains(m, "@") {
				m += "@s.whatsapp.net"
			}
			ctxInfo.MentionedJID = append(ctxInfo.MentionedJID, m)
		}
		msg.ExtendedTextMessage = &waProto.ExtendedTextMessage{
			Text:        proto.String(message),
			ContextInfo: ctxInfo,
		}
	} else {
		msg.Conversation = proto.String(message)
	}

	// Send message
	resp, err := client.SendMessage(context.Background(), recipientJID, msg)

	// "no LID found ... from server" on a personal chat means we addressed a JID the server
	// doesn't know — almost always a Brazilian number whose WhatsApp is registered WITHOUT
	// the 9th digit (an app signup form gladly stores +55 42 99810-2118 while the account
	// lives at 554298102118). Ask the server for the canonical JID and retry once, instead of
	// making every caller guess the variant and risk messaging a stranger.
	if err != nil && recipientJID.Server == types.DefaultUserServer && strings.Contains(err.Error(), "no LID found") {
		if canonical, ok := canonicalUserJID(client, recipientJID); ok && canonical.User != recipientJID.User {
			fmt.Printf("send: %s has no LID, retrying at canonical %s\n", recipientJID, canonical)
			recipientJID = canonical
			resp, err = client.SendMessage(context.Background(), recipientJID, msg)
		}
	}

	if err != nil {
		return false, fmt.Sprintf("Error sending message: %v", err)
	}

	// whatsmeow NÃO emite events.Message pro que nós mesmos mandamos — só handleMessage
	// grava, e ele só roda no recebimento. Sem isto a mensagem sai no WhatsApp e nunca
	// entra no store: a thread fica só com o lado deles. Mesmos extratores do
	// handleMessage, pra gravar idêntico ao caminho de recebimento.
	chatJID := recipientJID.String()

	// TouchChat ANTES do StoreMessage: o db abre com _foreign_keys=on e messages.chat_jid
	// referencia chats.jid, então numa conversa nova (iniciada por nós) a mensagem seria
	// rejeitada por FK. O fallback só vale pra linha nova; conversa existente mantém o
	// nome dela. Nome bom sem ida à rede: o contato do store local, senão o número.
	fallbackName := recipientJID.User
	if contact, err := client.Store.Contacts.GetContact(context.Background(), recipientJID); err == nil && contact.FullName != "" {
		fallbackName = contact.FullName
	}
	if err := messageStore.TouchChat(chatJID, fallbackName, resp.Timestamp); err != nil {
		fmt.Printf("Warning: message sent but chat not touched: %v\n", err)
	}

	mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength := extractMediaInfo(msg)
	quotedID, quotedSender, quotedText := extractQuoted(client, msg)
	if err := messageStore.StoreMessage(resp.ID, chatJID, client.Store.ID.User,
		extractTextContent(msg), resp.Timestamp, true,
		mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength,
		quotedID, quotedSender, quotedText); err != nil {
		// enviada de verdade — não falha a request, mas avisa que a cópia local faltou
		fmt.Printf("Warning: message sent but not stored locally: %v\n", err)
	}

	// recipientJID, not the requested string: a canonical-JID retry may have re-addressed
	// the message, and the caller needs the JID the conversation actually lives at.
	return true, fmt.Sprintf("Message sent to %s", recipientJID)
}

// sendWhatsAppReaction manda uma reação (emoji vazio = tira). Fora da janela de
// horário: reação não é recado, é um toque — o celular também deixa de madrugada.
func sendWhatsAppReaction(client *whatsmeow.Client, messageStore *MessageStore, recipient, messageID, emoji string) (bool, string) {
	if !client.IsConnected() {
		return false, "Not connected to WhatsApp"
	}
	if strings.TrimSpace(messageID) == "" {
		return false, "message_id is required"
	}
	if !validReactionEmoji(emoji) {
		return false, "reação inválida"
	}
	chat, err := parseRecipientJID(recipient)
	if err != nil {
		return false, fmt.Sprintf("Error parsing JID: %v", err)
	}
	fromMe, sender, ok := messageStore.GetMessageMeta(messageID, chat.String())
	if !ok {
		return false, "mensagem alvo não está no histórico local"
	}
	senderJID := targetSenderJID(chat, fromMe, sender)
	msg := client.BuildReaction(chat, senderJID, types.MessageID(messageID), strings.TrimSpace(emoji))
	resp, err := client.SendMessage(context.Background(), chat, msg)
	if err != nil {
		return false, fmt.Sprintf("Error sending reaction: %v", err)
	}
	ourUser := ""
	if client.Store.ID != nil {
		ourUser = client.Store.ID.User
	}
	if ourUser == "" {
		ourUser = "me"
	}
	if err := messageStore.StoreReaction(chat.String(), messageID, ourUser, strings.TrimSpace(emoji), resp.Timestamp, true); err != nil {
		fmt.Printf("Warning: reaction sent but not stored locally: %v\n", err)
	}
	return true, "ok"
}

// sendWhatsAppRevoke apaga "para todos" uma mensagem NOSSA (o WhatsApp só deixa o autor
// apagar; admin de grupo apagando alheio fica de fora de propósito). O revoke que nós
// mandamos não volta como events.Message, então a marca local é gravada aqui.
func sendWhatsAppRevoke(client *whatsmeow.Client, messageStore *MessageStore, recipient, messageID string) (bool, string) {
	if !client.IsConnected() {
		return false, "Not connected to WhatsApp"
	}
	if strings.TrimSpace(messageID) == "" {
		return false, "message_id is required"
	}
	chat, err := parseRecipientJID(recipient)
	if err != nil {
		return false, fmt.Sprintf("Error parsing JID: %v", err)
	}
	fromMe, _, ok := messageStore.GetMessageMeta(messageID, chat.String())
	if !ok {
		return false, "mensagem alvo não está no histórico local"
	}
	if !fromMe {
		return false, "só apaga mensagem nossa"
	}
	resp, err := client.SendMessage(context.Background(), chat, client.BuildRevoke(chat, types.EmptyJID, types.MessageID(messageID)))
	if err != nil {
		return false, fmt.Sprintf("Error sending revoke: %v", err)
	}
	if _, err := messageStore.SetRevoked(messageID, chat.String(), resp.Timestamp); err != nil {
		fmt.Printf("Warning: revoke sent but not stored locally: %v\n", err)
	}
	return true, "ok"
}

// sendWhatsAppRead marca as mensagens como lidas no protocolo (o outro lado vê o tick
// azul). Fora da janela de horário: ler não é recado — o celular também marca de madrugada.
// Em grupo o sender precisa ser quem escreveu (MarkRead só aceita um remetente por chamada).
func sendWhatsAppRead(client *whatsmeow.Client, recipient string, messageIDs []string, sender string) (bool, string) {
	if !client.IsConnected() {
		return false, "Not connected to WhatsApp"
	}
	chat, err := parseRecipientJID(recipient)
	if err != nil {
		return false, fmt.Sprintf("Error parsing JID: %v", err)
	}
	ids := make([]types.MessageID, 0, len(messageIDs))
	for _, id := range messageIDs {
		id = strings.TrimSpace(id)
		if id != "" {
			ids = append(ids, types.MessageID(id))
		}
	}
	if len(ids) == 0 {
		return false, "message_ids is required"
	}
	if len(ids) > 50 {
		ids = ids[:50]
	}
	senderJID := types.EmptyJID
	if strings.TrimSpace(sender) != "" {
		senderJID, err = parseRecipientJID(sender)
		if err != nil {
			return false, fmt.Sprintf("Error parsing sender: %v", err)
		}
	}
	if err := client.MarkRead(context.Background(), ids, time.Now(), chat, senderJID); err != nil {
		return false, fmt.Sprintf("Error sending read receipt: %v", err)
	}
	return true, fmt.Sprintf("Read receipt sent to %s", recipient)
}

// sendWhatsAppChatPresence mostra "digitando…" (composing) ou some com ele (paused) na
// conversa — é o que o autopilot usa entre o tique azul e o envio, pra resposta não se
// materializar do nada. composing exige presença global available antes (sem isso o outro
// lado não vê nada); paused devolve unavailable pro celular do dono voltar a receber as
// notificações (companion "online" silencia o aparelho).
func sendWhatsAppChatPresence(client *whatsmeow.Client, recipient, state string) (bool, string) {
	if !client.IsConnected() {
		return false, "Not connected to WhatsApp"
	}
	chat, err := parseRecipientJID(recipient)
	if err != nil {
		return false, fmt.Sprintf("Error parsing JID: %v", err)
	}
	ctx := context.Background()
	switch state {
	case "composing", "":
		if err := client.SendPresence(ctx, types.PresenceAvailable); err != nil {
			return false, fmt.Sprintf("Error sending presence: %v", err)
		}
		if err := client.SendChatPresence(ctx, chat, types.ChatPresenceComposing, types.ChatPresenceMediaText); err != nil {
			return false, fmt.Sprintf("Error sending chat presence: %v", err)
		}
	case "paused":
		if err := client.SendChatPresence(ctx, chat, types.ChatPresencePaused, types.ChatPresenceMediaText); err != nil {
			return false, fmt.Sprintf("Error sending chat presence: %v", err)
		}
		if err := client.SendPresence(ctx, types.PresenceUnavailable); err != nil {
			return false, fmt.Sprintf("Error sending presence: %v", err)
		}
	default:
		return false, "state must be composing or paused"
	}
	return true, fmt.Sprintf("Presence %s sent to %s", state, recipient)
}

// Extract media info from a message
func extractMediaInfo(msg *waProto.Message) (mediaType string, filename string, url string, mediaKey []byte, fileSHA256 []byte, fileEncSHA256 []byte, fileLength uint64) {
	if msg == nil {
		return "", "", "", nil, nil, nil, 0
	}

	// Check for image message
	if img := msg.GetImageMessage(); img != nil {
		return "image", "image_" + time.Now().Format("20060102_150405") + ".jpg",
			img.GetURL(), img.GetMediaKey(), img.GetFileSHA256(), img.GetFileEncSHA256(), img.GetFileLength()
	}

	// Check for video message
	if vid := msg.GetVideoMessage(); vid != nil {
		return "video", "video_" + time.Now().Format("20060102_150405") + ".mp4",
			vid.GetURL(), vid.GetMediaKey(), vid.GetFileSHA256(), vid.GetFileEncSHA256(), vid.GetFileLength()
	}

	// Check for audio message
	if aud := msg.GetAudioMessage(); aud != nil {
		return "audio", "audio_" + time.Now().Format("20060102_150405") + ".ogg",
			aud.GetURL(), aud.GetMediaKey(), aud.GetFileSHA256(), aud.GetFileEncSHA256(), aud.GetFileLength()
	}

	// Figurinha: mesmo download de imagem (whatsmeow.MediaImage), arquivo .webp — o
	// animado é webp animado, que o <img> do dashboard toca sozinho.
	if st := msg.GetStickerMessage(); st != nil {
		return "sticker", "sticker_" + time.Now().Format("20060102_150405") + ".webp",
			st.GetURL(), st.GetMediaKey(), st.GetFileSHA256(), st.GetFileEncSHA256(), st.GetFileLength()
	}

	// Check for document message
	if doc := msg.GetDocumentMessage(); doc != nil {
		filename := doc.GetFileName()
		if filename == "" {
			filename = "document_" + time.Now().Format("20060102_150405")
		}
		return "document", filename,
			doc.GetURL(), doc.GetMediaKey(), doc.GetFileSHA256(), doc.GetFileEncSHA256(), doc.GetFileLength()
	}

	return "", "", "", nil, nil, nil, 0
}

// --- Transcrição automática de áudios recebidos (Whisper local) ---

var whisperModelPath = func() string {
	if p := os.Getenv("WHISPER_MODEL"); p != "" {
		return p
	}
	return "/Users/alandebortolo/Dev/whatsapp-mcp/ggml-large-v3-turbo.bin"
}()

// ponytail: semáforo global de 1 slot — whisper carrega ~500MB por processo;
// áudios em rajada transcrevem em série. Subir o cap se virar gargalo.
var transcribeSem = make(chan struct{}, 1)

// transcribeAndReply baixa o áudio, transcreve via whisper.cpp local e
// responde na própria conversa citando a mensagem de áudio.
func transcribeAndReply(client *whatsmeow.Client, messageStore *MessageStore, msg *events.Message, chat types.JID, logger waLog.Logger) {
	// Evita rajada de replies atrasados se o bridge ficou fora do ar
	if time.Since(msg.Info.Timestamp) > 15*time.Minute {
		return
	}
	aud := msg.Message.GetAudioMessage()
	if aud == nil {
		return
	}

	transcribeSem <- struct{}{}
	defer func() { <-transcribeSem }()

	data, err := client.Download(context.Background(), aud)
	if err != nil {
		logger.Warnf("transcricao: falha no download do audio: %v", err)
		return
	}

	tmpDir, err := os.MkdirTemp("", "wamcp-audio-*")
	if err != nil {
		logger.Warnf("transcricao: falha ao criar tmpdir: %v", err)
		return
	}
	defer os.RemoveAll(tmpDir)

	ogg := filepath.Join(tmpDir, "audio.ogg")
	wav := filepath.Join(tmpDir, "audio.wav")
	if err := os.WriteFile(ogg, data, 0600); err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Caminhos absolutos: launchd não tem /opt/homebrew/bin no PATH
	if out, err := exec.CommandContext(ctx, "/opt/homebrew/bin/ffmpeg",
		"-y", "-i", ogg, "-ar", "16000", "-ac", "1", wav).CombinedOutput(); err != nil {
		logger.Warnf("transcricao: ffmpeg falhou: %v (%s)", err, out)
		return
	}

	out, err := exec.CommandContext(ctx, "/opt/homebrew/bin/whisper-cli",
		"-m", whisperModelPath, "-f", wav, "-l", "pt", "-nt", "-np").Output()
	if err != nil {
		logger.Warnf("transcricao: whisper falhou: %v", err)
		return
	}
	text := strings.TrimSpace(string(out))
	if text == "" {
		return
	}
	// Grava ANTES de tentar enviar: mesmo com o reply bloqueado pelo horário seguro o
	// texto fica no store, e o Clauditor lê daqui em vez de re-transcrever.
	if err := messageStore.SetTranscript(msg.Info.ID, chat.String(), text); err != nil {
		logger.Warnf("transcricao: falha ao gravar no store: %v", err)
	}

	reply := &waProto.Message{
		ExtendedTextMessage: &waProto.ExtendedTextMessage{
			Text: proto.String("📝 *Transcrição automática:*\n" + text),
			ContextInfo: &waProto.ContextInfo{
				StanzaID:      proto.String(msg.Info.ID),
				Participant:   proto.String(msg.Info.Sender.ToNonAD().String()),
				QuotedMessage: msg.Message,
			},
		},
	}
	if !whatsappSendAllowedAt(time.Now()) {
		logger.Infof("transcricao pronta, mas envio bloqueado pelo horario seguro para %s", chat.String())
		return
	}
	if _, err := client.SendMessage(context.Background(), chat, reply); err != nil {
		logger.Warnf("transcricao: falha ao enviar reply: %v", err)
	} else {
		logger.Infof("transcricao enviada para %s (%d chars)", chat.String(), len(text))
	}
}

// canonicalUserJID asks the server which JID actually owns a phone number. Brazilian
// numbers are the reason this exists: a 2010s line is registered on WhatsApp with 8
// digits (554298102118) while every form, CRM and signup screen stores the 9-digit
// form (5542998102118), and addressing the 9-digit JID fails with "no LID found".
// Returns ok=false when the number is not on WhatsApp at all, so the caller can tell
// "wrong variant" from "no WhatsApp" instead of guessing.
func canonicalUserJID(client *whatsmeow.Client, jid types.JID) (types.JID, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	resp, err := client.IsOnWhatsApp(ctx, []string{"+" + jid.User})
	if err != nil {
		return jid, false
	}
	for _, item := range resp {
		if item.IsIn {
			return item.JID.ToNonAD(), true
		}
	}
	return jid, false
}

// resolveJID maps a LID JID (WhatsApp's post-2026 privacy identifier) back to
// the phone-number JID via whatsmeow's lid_map, so each conversation keeps a
// single identity in the local DB. Returns the input unchanged when there is
// no mapping (or the JID is not a LID).
func resolveJID(client *whatsmeow.Client, jid types.JID) types.JID {
	if jid.Server != types.HiddenUserServer {
		return jid
	}
	pn, err := client.Store.LIDs.GetPNForLID(context.Background(), jid.ToNonAD())
	if err != nil || pn.IsEmpty() {
		return jid
	}
	return pn
}

// Recibo de entrega/leitura das mensagens que NÓS mandamos. Chat pode chegar
// como LID: resolve pra o mesmo JID que o StoreMessage gravou, e tenta o
// original se o resolve falhar (mensagem antiga no store ainda em LID).
func handleReceipt(client *whatsmeow.Client, messageStore *MessageStore, evt *events.Receipt, logger waLog.Logger) {
	ack := receiptAck(evt.Type)
	if ack == 0 || len(evt.MessageIDs) == 0 {
		return
	}
	chat := resolveJID(client, evt.Chat).ToNonAD()
	chatJID := chat.String()
	alt := evt.Chat.ToNonAD().String()
	for _, id := range evt.MessageIDs {
		if err := messageStore.SetAck(string(id), chatJID, ack); err != nil {
			logger.Warnf("Failed to store receipt %s %s: %v", id, chatJID, err)
		}
		if alt != chatJID {
			if err := messageStore.SetAck(string(id), alt, ack); err != nil {
				logger.Warnf("Failed to store receipt %s %s: %v", id, alt, err)
			}
		}
	}
}

// Handle regular incoming messages with media support
func handleMessage(client *whatsmeow.Client, messageStore *MessageStore, msg *events.Message, logger waLog.Logger, autoTranscribe bool) {
	// Save message to database (normalizing LID -> phone-number JID)
	chat := resolveJID(client, msg.Info.Chat)
	chatJID := chat.String()
	sender := resolveJID(client, msg.Info.Sender).User

	// Reação não é mensagem: grava na tabela própria e sai sem tocar last_message_time
	// (a prévia da caixa continua sendo a última mensagem de verdade). EncReaction
	// que não abriu também sai aqui — senão o StoreChat abaixo bumpava o chat à toa.
	if targetID, emoji, ok := incomingReaction(client, msg); ok {
		if err := messageStore.StoreReaction(chatJID, targetID, sender, emoji, msg.Info.Timestamp, msg.Info.IsFromMe); err != nil {
			logger.Warnf("Failed to store reaction: %v", err)
		}
		return
	}
	if msg.Message != nil && (msg.Message.GetReactionMessage() != nil || msg.Message.GetEncReactionMessage() != nil) {
		return
	}
	if applyRevoke(messageStore, chatJID, msg, logger) {
		return
	}

	// Get appropriate chat name (pass nil for conversation since we don't have one for regular messages)
	name := GetChatName(client, messageStore, chat, chatJID, nil, sender, logger)

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
		// Descartar calado foi o que escondeu os menus por meses: a conversa ficava com
		// buracos e ninguém sabia que faltava algo. O tipo no log diz qual formato ainda
		// não sabemos ler (protocolMessage/reaction/poll update são esperados aqui).
		if t := messageTypeName(msg.Message); !silentDrop[t] {
			logger.Warnf("mensagem sem texto e sem midia, descartada: %s (%s)", t, msg.Info.ID)
		}
		return
	}

	quotedID, quotedSender, quotedText := extractQuoted(client, msg.Message)

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
		quotedID,
		quotedSender,
		quotedText,
	)

	if err != nil {
		logger.Warnf("Failed to store message: %v", err)
	} else {
		// Log message reception
		timestamp := msg.Info.Timestamp.Format("2006-01-02 15:04:05")
		direction := "←"
		if msg.Info.IsFromMe {
			direction = "→"
		}

		// Log based on message type
		if mediaType != "" {
			fmt.Printf("[%s] %s %s: [%s: %s] %s\n", timestamp, direction, sender, mediaType, filename, content)
		} else if content != "" {
			fmt.Printf("[%s] %s %s: %s\n", timestamp, direction, sender, content)
		}

		// Transcrição automática: só áudio recebido em conversa 1:1
		if autoTranscribe && mediaType == "audio" && !msg.Info.IsFromMe && chat.Server == types.DefaultUserServer {
			go transcribeAndReply(client, messageStore, msg, chat, logger)
		}

		// Arquivo REENCAMINHADO chega com a assinatura do upload original, que pode já ter
		// vencido — e o reupload só existe enquanto o aparelho de quem mandou ainda tem o
		// arquivo (horas depois o celular responde NOT_FOUND). Quando a URL já nasce
		// vencida, buscamos AGORA: é a única janela em que dá.
		if mediaType != "" && mediaURLExpired(url, time.Now()) {
			go func() {
				fmt.Printf("Media for %s arrived with an expired signature, fetching it now...\n", msg.Info.ID)
				if _, _, _, _, err := downloadMedia(client, messageStore, msg.Info.ID, chat.String()); err != nil {
					logger.Warnf("Failed to rescue expired media %s: %v", msg.Info.ID, err)
				}
			}()
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
	Success  bool   `json:"success"`
	Message  string `json:"message"`
	Filename string `json:"filename,omitempty"`
	Path     string `json:"path,omitempty"`
}

// Store additional media info in the database
func (store *MessageStore) StoreMediaInfo(id, chatJID, url string, mediaKey, fileSHA256, fileEncSHA256 []byte, fileLength uint64) error {
	_, err := store.db.Exec(
		"UPDATE messages SET url = ?, media_key = ?, file_sha256 = ?, file_enc_sha256 = ?, file_length = ? WHERE id = ? AND chat_jid = ?",
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
		"SELECT media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length FROM messages WHERE id = ? AND chat_jid = ?",
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
	chatDir := filepath.Join(messageStore.storeDir, strings.ReplaceAll(chatJID, ":", "_"))
	localPath := ""

	// Get media info from the database
	mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength, err = messageStore.GetMediaInfo(messageID, chatJID)

	if err != nil {
		// Try to get basic info if extended info isn't available
		err = messageStore.db.QueryRow(
			"SELECT media_type, filename FROM messages WHERE id = ? AND chat_jid = ?",
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
		return false, "", "", "", fmt.Errorf("incomplete media information for download")
	}

	fmt.Printf("Attempting to download media for message %s in chat %s...\n", messageID, chatJID)

	// Extract direct path from URL
	directPath := extractDirectPathFromURL(url)

	// Create a downloader that implements DownloadableMessage
	var waMediaType whatsmeow.MediaType
	switch mediaType {
	case "image", "sticker":
		waMediaType = whatsmeow.MediaImage
	case "video":
		waMediaType = whatsmeow.MediaVideo
	case "audio":
		waMediaType = whatsmeow.MediaAudio
	case "document":
		waMediaType = whatsmeow.MediaDocument
	default:
		return false, "", "", "", fmt.Errorf("unsupported media type: %s", mediaType)
	}

	downloader := &MediaDownloader{
		URL:           url,
		DirectPath:    directPath,
		MediaKey:      mediaKey,
		FileLength:    fileLength,
		FileSHA256:    fileSHA256,
		FileEncSHA256: fileEncSHA256,
		MediaType:     waMediaType,
	}

	// Download the media using whatsmeow client. Assinatura já vencida não vale a viagem: o
	// CDN só devolve 403 e, depois de algumas seguidas, devolve devagar (medido: minutos
	// pendurado). Nesse caso vamos direto para as vias que resolvem.
	var mediaData []byte
	if mediaURLExpired(url, time.Now()) {
		err = errMediaURLExpired
	} else {
		dlCtx, cancelDl := context.WithTimeout(context.Background(), mediaDownloadTimeout)
		mediaData, err = client.Download(dlCtx, downloader)
		cancelDl()
	}
	if err != nil && isExpiredMediaError(err) {
		// O CDN nega mídia antiga ou reencaminhada (a assinatura da URL vence em ~3 meses).
		// A via do protocolo é pedir ao aparelho de quem mandou que suba de novo — vale
		// enquanto ele ainda tem o arquivo. (O cache do WhatsApp Desktop deste Mac é a outra
		// via, e mora no Clauditor: tocar no container de outro app pendura ESTE processo,
		// que não tem Acesso Total ao Disco — medido em 25/08/2026.)
		fmt.Printf("Media for %s expired on the CDN (%v)\n", messageID, err)
		data, retryErr := retryExpiredMedia(client, messageStore, messageID, chatJID, downloader)
		if retryErr != nil {
			err = fmt.Errorf("%v (re-upload request also failed: %v)", err, retryErr)
		} else {
			mediaData, err = data, nil
		}
	}
	if err != nil {
		return false, "", "", "", fmt.Errorf("failed to download media: %v", err)
	}

	// Save the downloaded media to file
	if err := os.WriteFile(localPath, mediaData, 0644); err != nil {
		return false, "", "", "", fmt.Errorf("failed to save media file: %v", err)
	}

	fmt.Printf("Successfully downloaded %s media to %s (%d bytes)\n", mediaType, absPath, len(mediaData))
	return true, mediaType, filename, absPath, nil
}

// O WhatsApp assina a URL da mídia com tokens (oh/oe) que vencem em ~3 meses, e mensagem
// REENCAMINHADA carrega o upload ORIGINAL — por isso um arquivo antigo repassado hoje dá 403
// aqui enquanto o app no celular abre numa boa (ele pede reupload sozinho). Fazíamos o mesmo
// que nada: devolver "403" pro chamador, que virava "não consegui, é técnico" na cara do
// cliente. O remédio é o do próprio protocolo — media retry (25/08/2026).
const (
	mediaRetryTimeout    = 30 * time.Second
	mediaDownloadTimeout = 60 * time.Second
)

// errMediaURLExpired é o 403 que dá pra prever pelo próprio link, sem chamar o CDN.
var errMediaURLExpired = errors.New("media URL signature expired")

// mediaRetryCooldown: depois que o celular recusou (ou não respondeu) o reupload, não
// perguntamos de novo por este tempo. Cada pedido vira UMA notificação "sincronização com o
// outro dispositivo foi interrompida" no iPhone do dono — e consulta repetida (poll, dashboard
// re-renderizando a imagem) fazia 89 pedidos em 25 min para um status morto (02/09/2026).
const mediaRetryCooldown = time.Hour

var mediaRetries = struct {
	sync.Mutex
	waiting map[string]chan *events.MediaRetry
	failed  map[string]time.Time // messageID → quando o celular recusou/não respondeu
}{waiting: make(map[string]chan *events.MediaRetry), failed: make(map[string]time.Time)}

// mediaRetryDenied diz se este id ainda está no cooldown de um pedido que falhou.
func mediaRetryDenied(messageID string, now time.Time) bool {
	mediaRetries.Lock()
	defer mediaRetries.Unlock()
	t, ok := mediaRetries.failed[messageID]
	return ok && now.Sub(t) < mediaRetryCooldown
}

func noteMediaRetryFailed(messageID string, now time.Time) {
	mediaRetries.Lock()
	mediaRetries.failed[messageID] = now
	mediaRetries.Unlock()
}

// handleMediaRetry entrega a resposta do celular do remetente a quem está esperando por ela.
func handleMediaRetry(evt *events.MediaRetry) {
	mediaRetries.Lock()
	ch, ok := mediaRetries.waiting[evt.MessageID]
	mediaRetries.Unlock()
	if ok {
		select {
		case ch <- evt:
		default:
		}
	}
}

// mediaURLExpired lê o `oe` (unix em hex) que o WhatsApp põe na URL assinada e diz se ela
// já morreu. Sem o parâmetro, assume viva: chutar "vencida" custaria um pedido de reupload
// à toa em toda mídia normal.
func mediaURLExpired(url string, now time.Time) bool {
	idx := strings.Index(url, "oe=")
	if idx < 0 {
		return false
	}
	raw := url[idx+3:]
	if end := strings.IndexByte(raw, '&'); end >= 0 {
		raw = raw[:end]
	}
	exp, err := strconv.ParseInt(raw, 16, 64)
	if err != nil {
		return false
	}
	return now.After(time.Unix(exp, 0))
}

// isExpiredMediaError diz se vale pedir reupload: o CDN só responde 403/404/410 quando o
// arquivo saiu de lá. Erro de rede/hash é outra história e o whatsmeow já tenta de novo.
func isExpiredMediaError(err error) bool {
	return errors.Is(err, errMediaURLExpired) ||
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith403) ||
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith404) ||
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith410)
}

// retryExpiredMedia pede ao aparelho de quem mandou que suba o arquivo de novo e baixa pelo
// caminho novo. Depende do celular do remetente estar online — se não estiver, o erro diz isso.
func retryExpiredMedia(client *whatsmeow.Client, messageStore *MessageStore, messageID, chatJID string, d *MediaDownloader) ([]byte, error) {
	chat, err := types.ParseJID(chatJID)
	if err != nil {
		return nil, fmt.Errorf("invalid chat jid: %v", err)
	}
	if mediaRetryDenied(messageID, time.Now()) {
		fmt.Printf("Media for %s: the phone already refused the re-upload; not asking again\n", messageID)
		return nil, fmt.Errorf("the phone already refused (or ignored) the re-upload less than %s ago; not asking again", mediaRetryCooldown)
	}
	var sender string
	var isFromMe bool
	if err := messageStore.db.QueryRow(
		"SELECT IFNULL(sender, ''), is_from_me FROM messages WHERE id = ? AND chat_jid = ?",
		messageID, chatJID,
	).Scan(&sender, &isFromMe); err != nil {
		return nil, fmt.Errorf("failed to read message: %v", err)
	}
	senderJID := chat
	if sender != "" {
		senderJID = types.NewJID(sender, types.DefaultUserServer)
	}
	info := &types.MessageInfo{
		ID: messageID,
		MessageSource: types.MessageSource{
			Chat:     chat,
			Sender:   senderJID,
			IsFromMe: isFromMe,
			IsGroup:  chat.Server == types.GroupServer,
		},
	}

	ch := make(chan *events.MediaRetry, 1)
	mediaRetries.Lock()
	mediaRetries.waiting[messageID] = ch
	mediaRetries.Unlock()
	defer func() {
		mediaRetries.Lock()
		delete(mediaRetries.waiting, messageID)
		mediaRetries.Unlock()
	}()

	askCtx, cancelAsk := context.WithTimeout(context.Background(), mediaRetryTimeout)
	defer cancelAsk()
	fmt.Printf("Media for %s: asking the sender to re-upload...\n", messageID)
	if err := client.SendMediaRetryReceipt(askCtx, info, d.MediaKey); err != nil {
		return nil, fmt.Errorf("failed to ask for re-upload: %v", err)
	}

	select {
	case evt := <-ch:
		notif, err := whatsmeow.DecryptMediaRetryNotification(evt, d.MediaKey)
		if err != nil {
			noteMediaRetryFailed(messageID, time.Now()) // inclui "media no longer available on phone"
			return nil, fmt.Errorf("re-upload answer unreadable: %v", err)
		}
		if notif.GetResult() != waMmsRetry.MediaRetryNotification_SUCCESS {
			noteMediaRetryFailed(messageID, time.Now())
			return nil, fmt.Errorf("sender refused the re-upload: %s", notif.GetResult())
		}
		dlCtx, cancelDl := context.WithTimeout(context.Background(), mediaRetryTimeout)
		defer cancelDl()
		return client.DownloadMediaWithPath(dlCtx, notif.GetDirectPath(), d.FileEncSHA256, d.FileSHA256, d.MediaKey, d.MediaType, "", false)
	case <-askCtx.Done():
		noteMediaRetryFailed(messageID, time.Now())
		return nil, fmt.Errorf("no re-upload answer from the sender's phone within %s", mediaRetryTimeout)
	}
}

// Extract direct path from a WhatsApp media URL
func extractDirectPathFromURL(url string) string {
	// The direct path is typically in the URL, we need to extract it
	// Example URL: https://mmg.whatsapp.net/v/t62.7118-24/13812002_698058036224062_3424455886509161511_n.enc?ccb=11-4&oh=...

	// Find the path part after the domain
	parts := strings.SplitN(url, ".net/", 2)
	if len(parts) < 2 {
		return url // Return original URL if parsing fails
	}

	pathPart := parts[1]

	// Keep query parameters: the CDN auth tokens (ccb/oh/oe) live there and
	// stripping them makes WhatsApp return 403 on download
	return "/" + pathPart
}

// Start a REST API server to expose the WhatsApp client functionality
func startRESTServer(client *whatsmeow.Client, messageStore *MessageStore, bindAddress string, port int, accountName string) error {
	mux := http.NewServeMux()

	// Read-only status endpoint used to verify that each isolated account is healthy.
	mux.HandleFunc("/api/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		jid := ""
		if client.Store.ID != nil {
			jid = client.Store.ID.ToNonAD().String()
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":   true,
			"account":   accountName,
			"connected": client.IsConnected(),
			"logged_in": client.IsLoggedIn(),
			"jid":       jid,
		})
	})

	// Handler for sending messages
	mux.HandleFunc("/api/send", func(w http.ResponseWriter, r *http.Request) {
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
		success, message := sendWhatsAppMessage(client, messageStore, req.Recipient, req.Message, req.MediaPath, req.ReplyTo, req.Mentions)
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

	mux.HandleFunc("/api/react", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Recipient string `json:"recipient"`
			MessageID string `json:"message_id"`
			Emoji     string `json:"emoji"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}
		if req.Recipient == "" || req.MessageID == "" {
			http.Error(w, "Recipient and message_id are required", http.StatusBadRequest)
			return
		}
		success, message := sendWhatsAppReaction(client, messageStore, req.Recipient, req.MessageID, req.Emoji)
		w.Header().Set("Content-Type", "application/json")
		if !success {
			w.WriteHeader(http.StatusInternalServerError)
		}
		json.NewEncoder(w).Encode(SendMessageResponse{Success: success, Message: message})
	})

	// Apagar "para todos" uma mensagem nossa. Nasceu como superfície de teste do revoke
	// recebido (03/09/2026): sem ela não há como provar a marca "apagada" sem celular.
	mux.HandleFunc("/api/revoke", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Recipient string `json:"recipient"`
			MessageID string `json:"message_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}
		if req.Recipient == "" || req.MessageID == "" {
			http.Error(w, "Recipient and message_id are required", http.StatusBadRequest)
			return
		}
		success, message := sendWhatsAppRevoke(client, messageStore, req.Recipient, req.MessageID)
		w.Header().Set("Content-Type", "application/json")
		if !success {
			w.WriteHeader(http.StatusInternalServerError)
		}
		json.NewEncoder(w).Encode(SendMessageResponse{Success: success, Message: message})
	})

	mux.HandleFunc("/api/read", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Recipient  string   `json:"recipient"`
			MessageIDs []string `json:"message_ids"`
			Sender     string   `json:"sender"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}
		if req.Recipient == "" || len(req.MessageIDs) == 0 {
			http.Error(w, "Recipient and message_ids are required", http.StatusBadRequest)
			return
		}
		success, message := sendWhatsAppRead(client, req.Recipient, req.MessageIDs, req.Sender)
		w.Header().Set("Content-Type", "application/json")
		if !success {
			w.WriteHeader(http.StatusInternalServerError)
		}
		json.NewEncoder(w).Encode(SendMessageResponse{Success: success, Message: message})
	})

	// Presença por conversa ("digitando…"): cosmética, sem janela de horário — quem manda
	// mensagem de verdade é o /api/send, que já bloqueia fora do horário seguro.
	mux.HandleFunc("/api/presence", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Recipient string `json:"recipient"`
			State     string `json:"state"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}
		if req.Recipient == "" {
			http.Error(w, "Recipient is required", http.StatusBadRequest)
			return
		}
		success, message := sendWhatsAppChatPresence(client, req.Recipient, req.State)
		w.Header().Set("Content-Type", "application/json")
		if !success {
			w.WriteHeader(http.StatusInternalServerError)
		}
		json.NewEncoder(w).Encode(SendMessageResponse{Success: success, Message: message})
	})

	// Handler for downloading media
	mux.HandleFunc("/api/download", func(w http.ResponseWriter, r *http.Request) {
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

		// Send successful response
		json.NewEncoder(w).Encode(DownloadMediaResponse{
			Success:  true,
			Message:  fmt.Sprintf("Successfully downloaded %s media", mediaType),
			Filename: filename,
			Path:     path,
		})
	})

	// Handler to check whether phone numbers are registered on WhatsApp, WITHOUT sending
	// anything. GET /api/exists?phone=+5542998102118[,+5542...] — comma-separated.
	// Answers the question a failed send can't: "no LID found from server" means either
	// the number has no WhatsApp or the JID is a different variant (BR 8-vs-9 digits), and
	// the only safe way to tell them apart is asking the server instead of probing by sending
	// a message to a stranger. The response carries the canonical JID to send to.
	mux.HandleFunc("/api/exists", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		raw := r.URL.Query().Get("phone")
		if raw == "" {
			http.Error(w, "phone is required", http.StatusBadRequest)
			return
		}
		phones := []string{}
		for _, p := range strings.Split(raw, ",") {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			if !strings.HasPrefix(p, "+") { // whatsmeow wants international format with +
				p = "+" + p
			}
			phones = append(phones, p)
		}
		if len(phones) == 0 {
			http.Error(w, "phone is required", http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()

		resp, err := client.IsOnWhatsApp(ctx, phones)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			json.NewEncoder(w).Encode(map[string]any{"success": false, "message": err.Error()})
			return
		}
		results := make([]map[string]any, 0, len(resp))
		for _, item := range resp {
			results = append(results, map[string]any{
				"query": item.Query,
				"is_on_whatsapp": item.IsIn,
				"jid": item.JID.String(),
			})
		}
		json.NewEncoder(w).Encode(map[string]any{"success": true, "results": results})
	})

	// Handler for profile pictures (avatars). GET /api/avatar?jid=<jid>
	// 404 means "no picture available" (not set, or hidden by privacy) — callers fall back
	// to their own placeholder. Returns the preview (thumbnail) size, which is what avatars need.
	mux.HandleFunc("/api/avatar", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		jidStr := r.URL.Query().Get("jid")
		if jidStr == "" {
			http.Error(w, "jid is required", http.StatusBadRequest)
			return
		}
		jid, err := types.ParseJID(jidStr)
		if err != nil {
			http.Error(w, "Invalid JID", http.StatusBadRequest)
			return
		}

		// Bound the IQ query: for a contact with no picture the server may never answer,
		// and an unbounded wait would pin a caller (and a goroutine) indefinitely.
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		info, err := client.GetProfilePictureInfo(ctx, jid, &whatsmeow.GetProfilePictureParams{Preview: true})
		if err != nil || info == nil || info.URL == "" {
			// ErrProfilePictureNotSet / ErrProfilePictureUnauthorized land here too
			http.Error(w, "No profile picture", http.StatusNotFound)
			return
		}

		resp, err := http.Get(info.URL) // CDN URL, plain HTTP GET per whatsmeow docs
		if err != nil {
			http.Error(w, "Failed to fetch picture", http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			http.Error(w, "Failed to fetch picture", http.StatusBadGateway)
			return
		}

		ct := resp.Header.Get("Content-Type")
		if ct == "" {
			ct = "image/jpeg"
		}
		w.Header().Set("Content-Type", ct)
		io.Copy(w, resp.Body)
	})

	// Start the server
	serverAddr := fmt.Sprintf("%s:%d", bindAddress, port)
	fmt.Printf("Starting REST API server on %s...\n", serverAddr)

	listener, err := net.Listen("tcp", serverAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", serverAddr, err)
	}
	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Run server in a goroutine so it doesn't block
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			fmt.Printf("REST API server error: %v\n", err)
		}
	}()
	return nil
}

func main() {
	// Set up logger
	logger := waLog.Stdout("Client", "INFO", true)
	cfg, err := loadRuntimeConfig()
	if err != nil {
		logger.Errorf("Invalid runtime configuration: %v", err)
		return
	}
	logger.Infof("Starting WhatsApp client for %s (store=%s, bind=%s, port=%d)...",
		cfg.AccountName, cfg.StoreDir, cfg.BindAddress, cfg.Port)

	// Create database connection for storing session data
	dbLog := waLog.Stdout("Database", "INFO", true)

	// Keep a second process from opening the same WhatsApp session store.
	lockFile, err := acquireInstanceLock(cfg.StoreDir)
	if err != nil {
		logger.Errorf("Failed to lock bridge instance: %v", err)
		return
	}
	defer func() {
		syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
		lockFile.Close()
	}()

	container, err := sqlstore.New(
		context.Background(),
		"sqlite3",
		sqliteDSN(filepath.Join(cfg.StoreDir, "whatsapp.db")),
		dbLog,
	)
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
	messageStore, err := NewMessageStore(cfg.StoreDir)
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
			handleMessage(client, messageStore, v, logger, cfg.AutoTranscribe)

		case *events.Receipt:
			handleReceipt(client, messageStore, v, logger)

		case *events.MediaRetry:
			handleMediaRetry(v)

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
				if err := os.WriteFile(cfg.QRPath, []byte(evt.Code), 0600); err != nil {
					logger.Warnf("Failed to write QR code to %s: %v", cfg.QRPath, err)
				}
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

	fmt.Println("\n✓ Connected to WhatsApp! Type 'help' for commands.")

	// Start REST API server
	if err := startRESTServer(client, messageStore, cfg.BindAddress, cfg.Port, cfg.AccountName); err != nil {
		logger.Errorf("Failed to start REST API server: %v", err)
		return
	}

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
	err := messageStore.db.QueryRow("SELECT name FROM chats WHERE jid = ?", chatJID).Scan(&existingName)
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

// Handle history sync events
func handleHistorySync(client *whatsmeow.Client, messageStore *MessageStore, historySync *events.HistorySync, logger waLog.Logger) {
	fmt.Printf("Received history sync event with %d conversations\n", len(historySync.Data.Conversations))

	syncedCount := 0
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

		// Normalize LID -> phone-number JID (see resolveJID)
		jid = resolveJID(client, jid)
		chatJID = jid.String()

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

			// Store messages
			for _, msg := range messages {
				if msg == nil || msg.Message == nil {
					continue
				}

				// Extract text content
				content := extractTextContent(msg.Message.Message)

				// Extract media info
				var mediaType, filename, url string
				var mediaKey, fileSHA256, fileEncSHA256 []byte
				var fileLength uint64

				if msg.Message.Message != nil {
					mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength = extractMediaInfo(msg.Message.Message)
				}

				// Log the message content for debugging
				logger.Infof("Message content: %v, Media Type: %v", content, mediaType)

				// Determine sender
				var sender string
				isFromMe := false
				if msg.Message.Key != nil {
					if msg.Message.Key.FromMe != nil {
						isFromMe = *msg.Message.Key.FromMe
					}
					if !isFromMe && msg.Message.Key.Participant != nil && *msg.Message.Key.Participant != "" {
						sender = *msg.Message.Key.Participant
						// Participant may come as a LID JID string — normalize it
						if pJID, pErr := types.ParseJID(sender); pErr == nil {
							sender = resolveJID(client, pJID).User
						}
					} else if isFromMe {
						sender = client.Store.ID.User
					} else {
						sender = jid.User
					}
				} else {
					sender = jid.User
				}

				// Store message
				msgID := ""
				if msg.Message.Key != nil && msg.Message.Key.ID != nil {
					msgID = *msg.Message.Key.ID
				}

				// Get message timestamp
				timestamp := time.Time{}
				if ts := msg.Message.GetMessageTimestamp(); ts != 0 {
					timestamp = time.Unix(int64(ts), 0)
				} else {
					continue
				}

				// Reação no history sync: não é mensagem, não entra no store de texto.
				if content == "" && mediaType == "" {
					if targetID, emoji, ok := protoReaction(msg.Message.Message); ok {
						if sender == "" {
							sender = jid.User
						}
						if err := messageStore.StoreReaction(chatJID, targetID, sender, emoji, timestamp, isFromMe); err != nil {
							logger.Warnf("Failed to store history reaction: %v", err)
						}
					}
					continue
				}

				quotedID, quotedSender, quotedText := extractQuoted(client, msg.Message.Message)

				err = messageStore.StoreMessage(
					msgID,
					chatJID,
					sender,
					content,
					timestamp,
					isFromMe,
					mediaType,
					filename,
					url,
					mediaKey,
					fileSHA256,
					fileEncSHA256,
					fileLength,
					quotedID,
					quotedSender,
					quotedText,
				)
				if err != nil {
					logger.Warnf("Failed to store history message: %v", err)
				} else {
					syncedCount++
					// Log successful message storage
					if mediaType != "" {
						logger.Infof("Stored message: [%s] %s -> %s: [%s: %s] %s",
							timestamp.Format("2006-01-02 15:04:05"), sender, chatJID, mediaType, filename, content)
					} else {
						logger.Infof("Stored message: [%s] %s -> %s: %s",
							timestamp.Format("2006-01-02 15:04:05"), sender, chatJID, content)
					}
				}
			}
		}
	}

	fmt.Printf("History sync complete. Stored %d messages.\n", syncedCount)
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
