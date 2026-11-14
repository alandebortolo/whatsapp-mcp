package main

import (
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// Ligação recebida ao vivo (Clauditor, 14/11/2026): a ponte é aparelho vinculado e recebe o
// CallOffer enquanto o celular ainda toca. Ela NÃO atende (whatsmeow não tem mídia de chamada);
// só guarda quem está ligando para o Clauditor ler em GET /api/calls/ringing (poll de ~1,5 s,
// calls_live.py) e recusar por POST /api/calls/reject. Grupo (CallOfferNotice) fica fora.

type ringingCall struct {
	CallID string    `json:"call_id"`
	From   string    `json:"from"`  // telefone resolvido do LID quando o store sabe
	Raw    string    `json:"raw"`   // JID como chegou (o RejectCall precisa dele)
	Video  bool      `json:"video"`
	At     time.Time `json:"at"`
}

// ringMaxAge: chamada que não recebeu o fim (ponte caiu no meio) some sozinha.
const ringMaxAge = 90 * time.Second

type ringTracker struct {
	mu    sync.Mutex
	calls map[string]ringingCall
}

var rings = &ringTracker{calls: map[string]ringingCall{}}

func (t *ringTracker) offer(c ringingCall) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.calls) > 50 { // nunca cresce sem teto
		t.calls = map[string]ringingCall{}
	}
	t.calls[c.CallID] = c
}

func (t *ringTracker) end(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.calls, id)
}

func (t *ringTracker) get(id string) (ringingCall, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	c, ok := t.calls[id]
	return c, ok
}

func (t *ringTracker) list(now time.Time) []ringingCall {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := []ringingCall{}
	for id, c := range t.calls {
		if now.Sub(c.At) > ringMaxAge {
			delete(t.calls, id)
			continue
		}
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].At.Before(out[j].At) })
	return out
}

// handleCallEvent: true quando o evento era de chamada (o switch do main não precisa de mais nada).
func handleCallEvent(client *whatsmeow.Client, evt interface{}) bool {
	switch v := evt.(type) {
	case *events.CallOffer:
		video := false
		if v.Data != nil {
			_, video = v.Data.GetOptionalChildByTag("video")
		}
		at := v.Timestamp
		if at.IsZero() {
			at = time.Now()
		}
		rings.offer(ringingCall{CallID: v.CallID, From: resolveJID(client, v.From).ToNonAD().String(),
			Raw: v.From.String(), Video: video, At: at})
	case *events.CallAccept: // atendeu no celular: para de tocar aqui também
		rings.end(v.CallID)
	case *events.CallTerminate:
		rings.end(v.CallID)
	case *events.CallReject:
		rings.end(v.CallID)
	default:
		return false
	}
	return true
}

func registerCallRoutes(mux *http.ServeMux, client *whatsmeow.Client) {
	mux.HandleFunc("/api/calls/ringing", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "calls": rings.list(time.Now())})
	})
	mux.HandleFunc("/api/calls/reject", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			CallID string `json:"call_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.CallID == "" {
			http.Error(w, "call_id is required", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		c, ok := rings.get(req.CallID)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(SendMessageResponse{Success: false, Message: "call is not ringing"})
			return
		}
		from, err := types.ParseJID(c.Raw)
		if err == nil {
			err = client.RejectCall(r.Context(), from, c.CallID)
		}
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(SendMessageResponse{Success: false, Message: err.Error()})
			return
		}
		rings.end(c.CallID)
		json.NewEncoder(w).Encode(SendMessageResponse{Success: true, Message: "rejected"})
	})
}
