package main

import (
	"testing"

	waProto "go.mau.fi/whatsmeow/binary/proto"
	"google.golang.org/protobuf/proto"
)

// O bug: menu de bot chegava vazio e o handler descartava a mensagem inteira.
// Cada caso aqui é um formato de menu real que estava sumindo da conversa.
func TestExtractTextContentMenus(t *testing.T) {
	cases := []struct {
		name string
		msg  *waProto.Message
		want string
	}{
		{"buttons", &waProto.Message{ButtonsMessage: &waProto.ButtonsMessage{
			ContentText: proto.String("Você já é cliente?"),
			Buttons: []*waProto.ButtonsMessage_Button{
				{ButtonID: proto.String("1"), ButtonText: &waProto.ButtonsMessage_Button_ButtonText{DisplayText: proto.String("Já sou cliente")}},
				{ButtonID: proto.String("2"), ButtonText: &waProto.ButtonsMessage_Button_ButtonText{DisplayText: proto.String("Quero conhecer")}},
			},
		}}, "Você já é cliente?\n[menu] Já sou cliente | Quero conhecer"},

		{"list", &waProto.Message{ListMessage: &waProto.ListMessage{
			Description: proto.String("Escolha o assunto"),
			Sections: []*waProto.ListMessage_Section{{Rows: []*waProto.ListMessage_Row{
				{Title: proto.String("Financeiro"), Description: proto.String("boletos e notas")},
				{Title: proto.String("Suporte")},
			}}},
		}}, "Escolha o assunto\n[menu] Financeiro (boletos e notas) | Suporte"},

		{"native flow quick reply", &waProto.Message{InteractiveMessage: &waProto.InteractiveMessage{
			Body: &waProto.InteractiveMessage_Body{Text: proto.String("Posso ajudar?")},
			InteractiveMessage: &waProto.InteractiveMessage_NativeFlowMessage_{NativeFlowMessage: &waProto.InteractiveMessage_NativeFlowMessage{
				Buttons: []*waProto.InteractiveMessage_NativeFlowMessage_NativeFlowButton{
					{Name: proto.String("quick_reply"), ButtonParamsJSON: proto.String(`{"display_text":"Sim","id":"s"}`)},
					{Name: proto.String("cta_url"), ButtonParamsJSON: proto.String(`{"display_text":"Site","url":"https://x.com"}`)},
				},
			}},
		}}, "Posso ajudar?\n[menu] Sim | Site (https://x.com)"},

		{"native flow single select", &waProto.Message{InteractiveMessage: &waProto.InteractiveMessage{
			Body: &waProto.InteractiveMessage_Body{Text: proto.String("Menu")},
			InteractiveMessage: &waProto.InteractiveMessage_NativeFlowMessage_{NativeFlowMessage: &waProto.InteractiveMessage_NativeFlowMessage{
				Buttons: []*waProto.InteractiveMessage_NativeFlowMessage_NativeFlowButton{
					{Name: proto.String("single_select"), ButtonParamsJSON: proto.String(`{"title":"Ver","sections":[{"rows":[{"title":"A","id":"a"},{"title":"B","id":"b"}]}]}`)},
				},
			}},
		}}, "Menu\n[menu] A | B"},

		// A escolha de quem clica também é mensagem sem Conversation — sumia igual.
		{"resposta de botão", &waProto.Message{ButtonsResponseMessage: &waProto.ButtonsResponseMessage{
			Response:         &waProto.ButtonsResponseMessage_SelectedDisplayText{SelectedDisplayText: "Já sou cliente"},
			SelectedButtonID: proto.String("1"),
		}}, "Já sou cliente"},

		{"resposta de lista", &waProto.Message{ListResponseMessage: &waProto.ListResponseMessage{
			Title:             proto.String("Financeiro"),
			SingleSelectReply: &waProto.ListResponseMessage_SingleSelectReply{SelectedRowID: proto.String("fin")},
		}}, "Financeiro"},

		{"resposta native flow", &waProto.Message{InteractiveResponseMessage: &waProto.InteractiveResponseMessage{
			InteractiveResponseMessage: &waProto.InteractiveResponseMessage_NativeFlowResponseMessage_{
				NativeFlowResponseMessage: &waProto.InteractiveResponseMessage_NativeFlowResponseMessage{
					ParamsJSON: proto.String(`{"display_text":"Quero conhecer","id":"2"}`),
				},
			},
		}}, "Quero conhecer"},

		// Conversa efêmera embrulha a mensagem: sem desembrulhar, texto vazio.
		{"envelope efêmero", &waProto.Message{EphemeralMessage: &waProto.FutureProofMessage{
			Message: &waProto.Message{Conversation: proto.String("oi")},
		}}, "oi"},

		{"texto normal segue igual", &waProto.Message{Conversation: proto.String("bom dia")}, "bom dia"},
	}

	for _, c := range cases {
		if got := extractTextContent(c.msg); got != c.want {
			t.Errorf("%s:\n got %q\nwant %q", c.name, got, c.want)
		}
	}
}

// Resposta de menu cita a mensagem do menu — é o quote que o WhatsApp desenha.
func TestQuotedContextInfoOnButtonResponse(t *testing.T) {
	msg := &waProto.Message{ButtonsResponseMessage: &waProto.ButtonsResponseMessage{
		Response:    &waProto.ButtonsResponseMessage_SelectedDisplayText{SelectedDisplayText: "Já sou cliente"},
		ContextInfo: &waProto.ContextInfo{StanzaID: proto.String("ABC123")},
	}}
	if ci := quotedContextInfo(msg); ci.GetStanzaID() != "ABC123" {
		t.Fatalf("resposta de botão perdeu a citação: %v", ci)
	}
}
