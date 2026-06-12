package updatepipe

import (
	"strings"
	"testing"

	mess "github.com/foxcpp/go-imap-mess"
)

func TestFormatParseRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		upd  mess.Update
	}{
		{
			name: "new message with uint64 key",
			upd: mess.Update{
				Type:   0,
				Key:    uint64(42),
				SeqSet: "1:5",
			},
		},
		{
			name: "flags update with string key",
			upd: mess.Update{
				Type:     1,
				Key:      uint64(100),
				SeqSet:   "3",
				NewFlags: []string{`\Seen`, `\Flagged`},
			},
		},
		{
			name: "removed update",
			upd: mess.Update{
				Type:   2,
				Key:    uint64(7),
				SeqSet: "10",
			},
		},
		{
			name: "empty seqset and flags",
			upd: mess.Update{
				Type: 3,
				Key:  uint64(999),
			},
		},
	}

	myID := "12345-0xc0000b4000"

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			formatted, err := formatUpdate(myID, tt.upd)
			if err != nil {
				t.Fatalf("formatUpdate: %v", err)
			}

			// formatUpdate appends \n, parseUpdate expects the line
			// without trailing newline.
			line := strings.TrimSuffix(formatted, "\n")

			gotID, gotUpd, err := parseUpdate(line)
			if err != nil {
				t.Fatalf("parseUpdate: %v", err)
			}

			if gotID != myID {
				t.Errorf("sender ID: got %q, want %q", gotID, myID)
			}
			if gotUpd.Type != tt.upd.Type {
				t.Errorf("Type: got %v, want %v", gotUpd.Type, tt.upd.Type)
			}

			// Key arrives as json.Number → uint64 after conversion.
			gotKey, ok := gotUpd.Key.(uint64)
			if !ok {
				t.Fatalf("Key type: got %T, want uint64", gotUpd.Key)
			}
			wantKey := tt.upd.Key.(uint64)
			if gotKey != wantKey {
				t.Errorf("Key: got %v, want %v", gotKey, wantKey)
			}

			if gotUpd.SeqSet != tt.upd.SeqSet {
				t.Errorf("SeqSet: got %q, want %q", gotUpd.SeqSet, tt.upd.SeqSet)
			}

			if len(gotUpd.NewFlags) != len(tt.upd.NewFlags) {
				t.Errorf("NewFlags length: got %d, want %d", len(gotUpd.NewFlags), len(tt.upd.NewFlags))
			} else {
				for i, f := range gotUpd.NewFlags {
					if f != tt.upd.NewFlags[i] {
						t.Errorf("NewFlags[%d]: got %q, want %q", i, f, tt.upd.NewFlags[i])
					}
				}
			}
		})
	}
}

func TestParseUpdate_MalformedNoSemicolon(t *testing.T) {
	_, _, err := parseUpdate("no-semicolon-here")
	if err == nil {
		t.Fatal("expected error for input without semicolon, got nil")
	}
	if !strings.Contains(err.Error(), "mismatched parts count") {
		t.Errorf("error message: got %q, want it to contain 'mismatched parts count'", err.Error())
	}
}

func TestParseUpdate_MalformedBadJSON(t *testing.T) {
	_, _, err := parseUpdate("some-id;{not valid json")
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
	if !strings.Contains(err.Error(), "parseUpdate") {
		t.Errorf("error message: got %q, want it to contain 'parseUpdate'", err.Error())
	}
}

func TestParseUpdate_EmptyString(t *testing.T) {
	_, _, err := parseUpdate("")
	if err == nil {
		t.Fatal("expected error for empty string, got nil")
	}
}

func TestParseUpdate_BadKeyNotUint64(t *testing.T) {
	// JSON with Key as a json.Number that is not a valid uint64
	// (negative number).
	_, _, err := parseUpdate("some-id;{\"Type\":0,\"Key\":-1}")
	if err == nil {
		t.Fatal("expected error for negative Key value, got nil")
	}
	if !strings.Contains(err.Error(), "invalid Key value") {
		t.Errorf("error message: got %q, want it to contain 'invalid Key value'", err.Error())
	}
}

func TestParseUpdate_BadKeyFloat(t *testing.T) {
	// JSON with Key as a json.Number that is a float (not a valid uint64).
	_, _, err := parseUpdate("some-id;{\"Type\":0,\"Key\":3.14}")
	if err == nil {
		t.Fatal("expected error for float Key value, got nil")
	}
	if !strings.Contains(err.Error(), "invalid Key value") {
		t.Errorf("error message: got %q, want it to contain 'invalid Key value'", err.Error())
	}
}

func TestParseUpdate_StringKeyPassthrough(t *testing.T) {
	// When Key is a string (not a json.Number), it should pass through
	// unchanged.
	_, upd, err := parseUpdate(`some-id;{"Type":0,"Key":"mailbox-abc"}`)
	if err != nil {
		t.Fatalf("parseUpdate: %v", err)
	}
	if upd.Key != "mailbox-abc" {
		t.Errorf("Key: got %v (%T), want 'mailbox-abc'", upd.Key, upd.Key)
	}
}

func TestParseUpdate_NoKey(t *testing.T) {
	// When Key is absent, it should be nil.
	_, upd, err := parseUpdate(`some-id;{"Type":0}`)
	if err != nil {
		t.Fatalf("parseUpdate: %v", err)
	}
	if upd.Key != nil {
		t.Errorf("Key: got %v (%T), want nil", upd.Key, upd.Key)
	}
}

func TestSemicolonEscaping(t *testing.T) {
	// Verify that semicolons in JSON values are properly escaped and
	// unescaped during round-trip.
	upd := mess.Update{
		Type:   0,
		Key:    uint64(1),
		SeqSet: "uid;with;semicolons",
	}

	formatted, err := formatUpdate("test-id", upd)
	if err != nil {
		t.Fatalf("formatUpdate: %v", err)
	}

	// The formatted string should not contain unescaped semicolons
	// in the JSON part (only the delimiter semicolon).
	parts := strings.SplitN(strings.TrimSuffix(formatted, "\n"), ";", 2)
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts after split, got %d", len(parts))
	}
	if strings.Contains(parts[1], ";") {
		t.Error("JSON part should not contain unescaped semicolons")
	}

	// Parse it back and verify the semicolons are restored.
	line := strings.TrimSuffix(formatted, "\n")
	_, gotUpd, err := parseUpdate(line)
	if err != nil {
		t.Fatalf("parseUpdate: %v", err)
	}
	if gotUpd.SeqSet != "uid;with;semicolons" {
		t.Errorf("SeqSet: got %q, want %q", gotUpd.SeqSet, "uid;with;semicolons")
	}
}

func TestEscapeUnescapeName(t *testing.T) {
	original := `hello;world;test`
	escaped := escapeName(original)
	if strings.Contains(escaped, ";") {
		t.Errorf("escapeName should remove all semicolons, got %q", escaped)
	}
	unescaped := unescapeName(escaped)
	if unescaped != original {
		t.Errorf("unescapeName(escapeName(%q)): got %q, want %q", original, unescaped, original)
	}
}
