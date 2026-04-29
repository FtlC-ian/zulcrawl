package syncer

import "testing"

func TestParseMessageIndexablesExtractsMentionsAndSmallTextAttachments(t *testing.T) {
	html := `<p>Hello <span class="user-mention" data-user-id="42">@Alice Example</span></p>
<p><a class="message_inline_ref" href="/user_uploads/2/ab/todo.txt">todo.txt</a> remember the zeta task</p>`
	mentions, attachments := parseMessageIndexables(100, 1, 10, 20, "2026-04-29T17:00:00Z", html)
	if len(mentions) != 1 {
		t.Fatalf("mentions = %#v, want 1", mentions)
	}
	if mentions[0].UserID != 42 || mentions[0].Name != "Alice Example" || mentions[0].Kind != "user" {
		t.Fatalf("mention = %#v", mentions[0])
	}
	if len(attachments) != 1 {
		t.Fatalf("attachments = %#v, want 1", attachments)
	}
	if attachments[0].FileName != "todo.txt" || !attachments[0].Indexed || attachments[0].Text == "" {
		t.Fatalf("attachment = %#v", attachments[0])
	}
}

func TestParseMessageIndexablesDoesNotIndexBinaryUploadText(t *testing.T) {
	html := `<p><a class="message_inline_ref" href="/user_uploads/2/ab/photo.png">photo.png</a> binary-looking upload</p>`
	_, attachments := parseMessageIndexables(100, 1, 10, 20, "2026-04-29T17:00:00Z", html)
	if len(attachments) != 1 {
		t.Fatalf("attachments = %#v, want 1 metadata row", attachments)
	}
	if attachments[0].Indexed || attachments[0].Text != "" {
		t.Fatalf("binary attachment should not be text-indexed: %#v", attachments[0])
	}
}
