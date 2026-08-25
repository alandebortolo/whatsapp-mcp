package main

import (
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// URLs reais das duas pontas do caso de 25/08/2026: o .pfx reencaminhado pelo Marco (assinado
// em maio, morto quando chegou) e um PDF do mesmo dia que baixava normalmente.
const (
	expiredURL = "https://mmg.whatsapp.net/v/t62.7119-24/565146191_1469583247700410_7756839601637647216_n.enc?ccb=11-4&oh=01_Q5Aa4QFeYbz68NV&oe=6A181F5C&_nc_sid=5e03e0&mms3=true"
	liveURL    = "https://mmg.whatsapp.net/v/t62.7119-24/785535543_1368564345481454_784633239992218816_n.enc?ccb=11-4&oh=01_Q5Aa5QGW9wC4N8Pu&oe=6AB5028A&_nc_sid=5e03e0&mms3=true"
)

func TestMediaURLExpired(t *testing.T) {
	now := time.Date(2026, 8, 25, 11, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		url  string
		want bool
	}{
		{"assinatura vencida", expiredURL, true},
		{"assinatura viva", liveURL, false},
		{"sem oe nenhum", "https://mmg.whatsapp.net/v/x_n.enc?ccb=11-4", false},
		{"oe ilegível", "https://mmg.whatsapp.net/v/x_n.enc?oe=zzz&mms3=true", false},
		{"url vazia", "", false},
	}
	for _, c := range cases {
		if got := mediaURLExpired(c.url, now); got != c.want {
			t.Errorf("%s: mediaURLExpired = %v, queria %v", c.name, got, c.want)
		}
	}
	// O mesmo arquivo era baixável antes do vencimento (28/05/2026 07:56 UTC-3).
	if mediaURLExpired(expiredURL, time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)) {
		t.Error("assinatura de maio não podia estar vencida em 01/05")
	}
}

func TestHandleMediaRetryEntregaAQuemEspera(t *testing.T) {
	ch := make(chan *events.MediaRetry, 1)
	mediaRetries.Lock()
	mediaRetries.waiting["MSG-A"] = ch
	mediaRetries.Unlock()
	defer func() {
		mediaRetries.Lock()
		delete(mediaRetries.waiting, "MSG-A")
		mediaRetries.Unlock()
	}()

	// Resposta de OUTRA mensagem não pode acordar quem espera por esta.
	handleMediaRetry(&events.MediaRetry{MessageID: types.MessageID("MSG-B")})
	select {
	case evt := <-ch:
		t.Fatalf("recebeu resposta da mensagem errada: %s", evt.MessageID)
	default:
	}

	handleMediaRetry(&events.MediaRetry{MessageID: types.MessageID("MSG-A")})
	select {
	case evt := <-ch:
		if evt.MessageID != "MSG-A" {
			t.Fatalf("entregou %s", evt.MessageID)
		}
	default:
		t.Fatal("a resposta da própria mensagem não chegou em quem espera")
	}

	// Sem ninguém esperando (o download já desistiu), não pode travar nem entrar em pânico.
	handleMediaRetry(&events.MediaRetry{MessageID: types.MessageID("MSG-ORFA")})
}
