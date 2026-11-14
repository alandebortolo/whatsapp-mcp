package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

func TestRingTrackerOfferEndAndExpiry(t *testing.T) {
	rings = &ringTracker{calls: map[string]ringingCall{}}
	now := time.Now()
	rings.offer(ringingCall{CallID: "a", From: "5527@s.whatsapp.net", At: now})
	rings.offer(ringingCall{CallID: "old", At: now.Add(-2 * ringMaxAge)})
	got := rings.list(now)
	if len(got) != 1 || got[0].CallID != "a" {
		t.Fatalf("esperava só a chamada viva, veio %+v", got)
	}
	if _, ok := rings.get("old"); ok {
		t.Fatal("chamada vencida deveria sair na listagem")
	}
	rings.end("a")
	if len(rings.list(now)) != 0 {
		t.Fatal("fim da chamada deveria tirar da lista")
	}
}

func TestHandleCallEventIgnoresOtherEvents(t *testing.T) {
	rings = &ringTracker{calls: map[string]ringingCall{}}
	if handleCallEvent(nil, &events.Connected{}) {
		t.Fatal("evento que não é de chamada deve seguir pro switch do main")
	}
	// Terminate sem oferta: não quebra e consome o evento
	ev := &events.CallTerminate{BasicCallMeta: types.BasicCallMeta{CallID: "x"}}
	if !handleCallEvent(nil, ev) {
		t.Fatal("CallTerminate é evento de chamada")
	}
}

func TestRingingRouteAndRejectUnknown(t *testing.T) {
	rings = &ringTracker{calls: map[string]ringingCall{}}
	rings.offer(ringingCall{CallID: "c1", From: "1@s.whatsapp.net", Raw: "1@s.whatsapp.net", Video: true, At: time.Now()})
	mux := http.NewServeMux()
	registerCallRoutes(mux, nil)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/calls/ringing", nil))
	var body struct {
		Calls []ringingCall `json:"calls"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || len(body.Calls) != 1 || !body.Calls[0].Video {
		t.Fatalf("ringing: %d %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/calls/reject", strings.NewReader(`{"call_id":"nope"}`)))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("recusar chamada que não toca deveria dar 404, deu %d", rec.Code)
	}
}
