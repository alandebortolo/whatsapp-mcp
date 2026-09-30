package main

import (
	"bytes"
	"strings"
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

// O bug: cartão de contato (vCard) chegava sem texto e sem mídia e o handler
// descartava — o número que a pessoa mandou sumia da conversa (26/08/2026:
// 8 cartões perdidos num dia; o do caso Alliance foi resgatado do ChatStorage
// do WhatsApp Desktop). O vCard do primeiro caso é o real, na íntegra.
func TestExtractTextContentContact(t *testing.T) {
	vcard := "BEGIN:VCARD\nVERSION:3.0\nN:Bonacho Site Web;Vinicius;;;\nFN:Vinicius Bonacho Site Web\nTEL;type=CELL;type=VOICE;waid=5527988230464:+55 27 98823-0464\nEMAIL;type=INTERNET;type=HOME:vinicius@unicocomunicacao.com\nEND:VCARD"
	cases := []struct {
		name string
		msg  *waProto.Message
		want string
	}{
		{"contact", &waProto.Message{ContactMessage: &waProto.ContactMessage{
			DisplayName: proto.String("Vinicius Bonacho Site Web"),
			Vcard:       proto.String(vcard),
		}}, "[contato] Vinicius Bonacho Site Web: +55 27 98823-0464 | vinicius@unicocomunicacao.com"},

		{"contacts array", &waProto.Message{ContactsArrayMessage: &waProto.ContactsArrayMessage{
			Contacts: []*waProto.ContactMessage{
				{DisplayName: proto.String("Fulano"), Vcard: proto.String("BEGIN:VCARD\nTEL:+55 11 1111-1111\nEND:VCARD")},
				{DisplayName: proto.String("Beltrana"), Vcard: proto.String("BEGIN:VCARD\nTEL;waid=5522922222222:+55 22 2222-2222\nEND:VCARD")},
			},
		}}, "[contato] Fulano: +55 11 1111-1111\n[contato] Beltrana: +55 22 2222-2222"},

		{"vcard do iPhone agrupado (item1.TEL)", &waProto.Message{ContactMessage: &waProto.ContactMessage{
			DisplayName: proto.String("Grasy"),
			Vcard:       proto.String("BEGIN:VCARD\nVERSION:3.0\nN:;Grasy;;;\nFN:Grasy\nitem1.TEL;waid=5527999999999:+55 27 99999-9999\nitem1.X-ABLabel:Celular\nEND:VCARD"),
		}}, "[contato] Grasy: +55 27 99999-9999"},

		{"vcard vazio cai no nome", &waProto.Message{ContactMessage: &waProto.ContactMessage{
			DisplayName: proto.String("Só Nome"),
		}}, "[contato] Só Nome"},
	}
	for _, tc := range cases {
		if got := extractTextContent(tc.msg); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}

// Localizações e convites também precisam de texto para não sumirem da conversa.
func TestExtractTextContentLocationAndInvite(t *testing.T) {
	cases := []struct {
		name string
		msg  *waProto.Message
		want string
	}{
		{"localização com nome e endereço", &waProto.Message{LocationMessage: &waProto.LocationMessage{
			Name:             proto.String(" Praça Central "),
			Address:          proto.String(" Rua das Flores, 10 "),
			DegreesLatitude:  proto.Float64(-23.55052),
			DegreesLongitude: proto.Float64(-46.633308),
		}}, "[localização] Praça Central — Rua das Flores, 10 https://maps.google.com/?q=-23.55052,-46.633308"},

		{"só coordenadas", &waProto.Message{LocationMessage: &waProto.LocationMessage{
			DegreesLatitude:  proto.Float64(-23.5),
			DegreesLongitude: proto.Float64(-46.6),
		}}, "[localização] https://maps.google.com/?q=-23.5,-46.6"},

		{"coordenadas zero", &waProto.Message{LocationMessage: &waProto.LocationMessage{
			DegreesLatitude:  proto.Float64(0),
			DegreesLongitude: proto.Float64(0),
		}}, "[localização]"},

		{"só nome e comentário", &waProto.Message{LocationMessage: &waProto.LocationMessage{
			Name:    proto.String("Portaria"),
			Comment: proto.String(" Entrada lateral "),
		}}, "[localização] Portaria\nEntrada lateral"},

		{"só endereço e latitude zero", &waProto.Message{LocationMessage: &waProto.LocationMessage{
			Address:          proto.String("Rua das Flores, 10"),
			DegreesLatitude:  proto.Float64(0),
			DegreesLongitude: proto.Float64(-46.6),
		}}, "[localização] Rua das Flores, 10 https://maps.google.com/?q=0,-46.6"},

		{"longitude zero e coordenada pequena", &waProto.Message{LocationMessage: &waProto.LocationMessage{
			DegreesLatitude:  proto.Float64(0.0000001),
			DegreesLongitude: proto.Float64(0),
		}}, "[localização] https://maps.google.com/?q=0.0000001,0"},

		{"ao vivo", &waProto.Message{LiveLocationMessage: &waProto.LiveLocationMessage{
			Caption:          proto.String(" Estou chegando "),
			DegreesLatitude:  proto.Float64(-23.5),
			DegreesLongitude: proto.Float64(-46.6),
		}}, "[localização ao vivo] Estou chegando https://maps.google.com/?q=-23.5,-46.6"},

		{"ao vivo só coordenadas", &waProto.Message{LiveLocationMessage: &waProto.LiveLocationMessage{
			DegreesLatitude:  proto.Float64(0),
			DegreesLongitude: proto.Float64(-46.6),
		}}, "[localização ao vivo] https://maps.google.com/?q=0,-46.6"},

		{"ao vivo coordenadas zero", &waProto.Message{LiveLocationMessage: &waProto.LiveLocationMessage{
			DegreesLatitude:  proto.Float64(0),
			DegreesLongitude: proto.Float64(0),
		}}, "[localização ao vivo]"},

		{"convite com código", &waProto.Message{GroupInviteMessage: &waProto.GroupInviteMessage{
			GroupName:  proto.String(" Grupo de teste "),
			InviteCode: proto.String(" CodigoTeste123 "),
			Caption:    proto.String(" Entre aqui "),
		}}, "[convite de grupo] Grupo de teste https://chat.whatsapp.com/CodigoTeste123\nEntre aqui"},

		{"convite sem código", &waProto.Message{GroupInviteMessage: &waProto.GroupInviteMessage{
			GroupName: proto.String("Grupo de teste"),
			Caption:   proto.String("Entre aqui"),
		}}, "[convite de grupo] Grupo de teste\nEntre aqui"},

		{"convite vazio", &waProto.Message{GroupInviteMessage: &waProto.GroupInviteMessage{}}, "[convite de grupo]"},
	}
	for _, tc := range cases {
		if got := extractTextContent(tc.msg); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
		wrapped := &waProto.Message{EphemeralMessage: &waProto.FutureProofMessage{Message: tc.msg}}
		if got := extractTextContent(wrapped); got != tc.want {
			t.Errorf("%s (envelope efêmero): got %q, want %q", tc.name, got, tc.want)
		}
	}
}

// Nota de vídeo usa os mesmos metadados de download do vídeo comum.
func TestExtractMediaInfoPtv(t *testing.T) {
	video := &waProto.VideoMessage{
		URL:           proto.String("https://example.com/video.enc"),
		MediaKey:      []byte{1, 2, 3},
		FileSHA256:    []byte{4, 5, 6},
		FileEncSHA256: []byte{7, 8, 9},
		FileLength:    proto.Uint64(1234),
	}
	ptv := &waProto.Message{PtvMessage: video}
	cases := []struct {
		name string
		msg  *waProto.Message
	}{
		{"ptv", ptv},
		{"ptv em envelope", &waProto.Message{EphemeralMessage: &waProto.FutureProofMessage{
			Message: &waProto.Message{ViewOnceMessageV2: &waProto.FutureProofMessage{Message: ptv}},
		}}},
		{"vídeo comum", &waProto.Message{VideoMessage: video}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength := extractMediaInfo(tc.msg)
			if mediaType != "video" {
				t.Fatalf("mediaType = %q, want %q", mediaType, "video")
			}
			if !strings.HasPrefix(filename, "video_") || !strings.HasSuffix(filename, ".mp4") {
				t.Errorf("filename = %q, want video_*.mp4", filename)
			}
			if url != video.GetURL() || !bytes.Equal(mediaKey, video.GetMediaKey()) ||
				!bytes.Equal(fileSHA256, video.GetFileSHA256()) || !bytes.Equal(fileEncSHA256, video.GetFileEncSHA256()) ||
				fileLength != video.GetFileLength() {
				t.Errorf("metadados alterados: url=%q, mediaKey=%v, sha256=%v, encSHA256=%v, length=%d",
					url, mediaKey, fileSHA256, fileEncSHA256, fileLength)
			}
		})
	}
}
