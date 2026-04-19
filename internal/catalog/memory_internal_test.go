package catalog

import "testing"

func TestMemoryCreateSchemaClonesDescriptor(t *testing.T) {
	t.Parallel()

	cat := NewMemory()
	input := &SchemaDescriptor{
		Name:             "public",
		DefaultCollation: "unicode_ci",
	}

	if err := cat.CreateSchema(input); err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}

	input.Name = "mutated"
	input.DefaultCollation = "changed"

	cat.mu.RLock()
	defer cat.mu.RUnlock()

	entry, ok := cat.schemas["public"]
	if !ok {
		t.Fatalf("schema entry missing for %q", "public")
	}
	if entry.descriptor.Name != "public" {
		t.Fatalf("stored schema name = %q, want %q", entry.descriptor.Name, "public")
	}
	if entry.descriptor.DefaultCollation != "unicode_ci" {
		t.Fatalf("stored collation = %q, want %q", entry.descriptor.DefaultCollation, "unicode_ci")
	}
}
