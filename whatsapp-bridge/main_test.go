package main

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
	"time"

	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

// Revogação ("apagar para todos") marca revoked_at na linha e a deixa lá; mensagem comum
// não é tratada como revogação; a segunda revogação do mesmo alvo não anda o horário.
func TestApplyRevoke(t *testing.T) {
	store, err := NewMessageStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	chat := "5527999999999@s.whatsapp.net"
	t0 := time.Date(2026, 9, 3, 15, 0, 0, 0, time.UTC)
	if err := store.StoreChat(chat, "Fulano", t0); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreMessage("m1", chat, "5527999999999", "foto comprometedora", t0, false,
		"image", "IMG.jpg", "https://x", nil, nil, nil, 0, "", "", ""); err != nil {
		t.Fatal(err)
	}

	revoke := func(id string, ts time.Time) *events.Message {
		return &events.Message{
			Info: types.MessageInfo{ID: "r-" + id, Timestamp: ts},
			Message: &waProto.Message{ProtocolMessage: &waProto.ProtocolMessage{
				Type: waProto.ProtocolMessage_REVOKE.Enum(),
				Key:  &waCommon.MessageKey{ID: proto.String(id), RemoteJID: proto.String(chat)},
			}},
		}
	}
	plain := &events.Message{Info: types.MessageInfo{ID: "m2"},
		Message: &waProto.Message{Conversation: proto.String("oi")}}

	if applyRevoke(store, chat, plain, waLog.Noop) {
		t.Fatal("mensagem comum tratada como revogação")
	}
	t1 := t0.Add(2 * time.Minute)
	if !applyRevoke(store, chat, revoke("m1", t1), waLog.Noop) {
		t.Fatal("REVOKE não reconhecido")
	}
	var content string
	var revokedAt time.Time
	if err := store.db.QueryRow("SELECT content, revoked_at FROM messages WHERE id='m1'").Scan(&content, &revokedAt); err != nil {
		t.Fatal(err)
	}
	if content != "foto comprometedora" || !revokedAt.Equal(t1) {
		t.Fatalf("linha depois do revoke: content=%q revoked_at=%v (esperado conteúdo intacto e %v)", content, revokedAt, t1)
	}
	// reentrega do mesmo revoke: horário da primeira fica
	applyRevoke(store, chat, revoke("m1", t1.Add(time.Hour)), waLog.Noop)
	store.db.QueryRow("SELECT revoked_at FROM messages WHERE id='m1'").Scan(&revokedAt)
	if !revokedAt.Equal(t1) {
		t.Fatalf("segunda revogação andou o horário: %v", revokedAt)
	}
	// alvo desconhecido: reconhece, não grava nada, não explode
	if !applyRevoke(store, chat, revoke("nunca-vi", t1), waLog.Noop) {
		t.Fatal("REVOKE de alvo desconhecido não reconhecido")
	}
	// reentrega da mensagem original (UPSERT) não apaga a marca
	store.StoreMessage("m1", chat, "5527999999999", "foto comprometedora", t0, false,
		"image", "IMG.jpg", "https://x", nil, nil, nil, 0, "", "", "")
	var n int
	store.db.QueryRow("SELECT COUNT(*) FROM messages WHERE id='m1' AND revoked_at IS NOT NULL").Scan(&n)
	if n != 1 {
		t.Fatal("reentrega da mensagem apagou revoked_at")
	}
}

// parseGroupJID é o freio de /api/group/leave e /api/group/participants: sair de grupo e
// tirar gente não desfazem, então jid que não é de grupo tem que morrer antes da chamada.
func TestParseGroupJID(t *testing.T) {
	if jid, err := parseGroupJID("120363426638900169@g.us"); err != nil {
		t.Fatalf("grupo válido recusado: %v", err)
	} else if jid.User != "120363426638900169" || jid.Server != "g.us" {
		t.Fatalf("grupo válido parseado errado: %s", jid)
	}
	// número de pessoa no lugar do grupo: o engano que custa caro
	for _, raw := range []string{
		"",                             // campo esquecido
		"5527998372363@s.whatsapp.net", // conversa de pessoa
		"5527998372363",                // número solto (parseRecipientJID aceitaria)
		"120363426638900169",           // id do grupo sem o @g.us
		"status@broadcast",             // não é grupo
	} {
		if _, err := parseGroupJID(raw); err == nil {
			t.Fatalf("parseGroupJID(%q) passou, devia recusar", raw)
		}
	}
}

// Duas mídias no mesmo segundo ganham o mesmo nome na chegada; a segunda não pode devolver
// o arquivo da primeira (19/09/2026).
func TestMediaLocalPathSameSecondCollision(t *testing.T) {
	dir := t.TempDir()
	first, second := []byte("print da entrada de 100"), []byte("print da entrada de 200")
	sum1, sum2 := sha256.Sum256(first), sha256.Sum256(second)
	name := "image_20260919_093613.jpg"
	base := filepath.Join(dir, name)

	if got := mediaLocalPath(dir, name, "AAA", sum1[:]); got != base {
		t.Fatalf("nada em disco: esperava o caminho padrão, veio %s", got)
	}
	if err := os.WriteFile(base, first, 0644); err != nil {
		t.Fatal(err)
	}
	if got := mediaLocalPath(dir, name, "AAA", sum1[:]); got != base {
		t.Fatalf("arquivo da própria mensagem: esperava o caminho padrão, veio %s", got)
	}
	want := filepath.Join(dir, "image_20260919_093613_BBB.jpg")
	if got := mediaLocalPath(dir, name, "BBB", sum2[:]); got != want {
		t.Fatalf("colisão: esperava %s, veio %s", want, got)
	}
	if got := mediaLocalPath(dir, name, "BBB", nil); got != base {
		t.Fatalf("sem sha256 no store: esperava o caminho padrão, veio %s", got)
	}
}
