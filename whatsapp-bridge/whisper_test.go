package main

import (
	"context"
	"os"
	"strings"
	"testing"
)

// Áudio em espanhol tem de sair em espanhol (com "-l pt" fixo o whisper traduzia: "o proveedor segue vazio"). Usa o
// whisper de verdade: sem binário ou modelo na máquina, pula.
func TestWhisperDetectKeepsTheSpokenLanguage(t *testing.T) {
	for _, p := range []string{"/opt/homebrew/bin/whisper-cli", whisperModelPath} {
		if _, err := os.Stat(p); err != nil {
			t.Skip("sem whisper local")
		}
	}
	out, err := whisperDetect(context.Background(), "testdata/whisper-es.wav")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.ToLower(string(out)), "sigue") {
		t.Fatalf("esperava espanhol, veio %q", out)
	}
}
