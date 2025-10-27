package grafana

import (
	"encoding/json"
	"testing"
)

func TestResolveTemplatesDir(t *testing.T) {
	dir, err := resolveTemplatesDir()
	if err != nil {
		t.Fatalf("resolveTemplatesDir returned error: %v", err)
	}
	if dir == "" {
		t.Fatalf("resolveTemplatesDir returned empty directory")
	}
}

func TestLoadTemplateSummaries(t *testing.T) {
	dir, err := resolveTemplatesDir()
	if err != nil {
		t.Fatalf("resolveTemplatesDir returned error: %v", err)
	}

	summaries, err := loadTemplateSummaries(dir)
	if err != nil {
		t.Fatalf("loadTemplateSummaries returned error: %v", err)
	}
	if len(summaries) == 0 {
		t.Fatalf("expected at least one template summary")
	}
	for _, item := range summaries {
		if item.Name == "" {
			t.Fatalf("template summary missing name: %+v", item)
		}
		if item.Title == "" {
			t.Fatalf("template summary missing title for %s", item.Name)
		}
	}
}

func TestLoadTemplateDetail(t *testing.T) {
	dir, err := resolveTemplatesDir()
	if err != nil {
		t.Fatalf("resolveTemplatesDir returned error: %v", err)
	}

	summaries, err := loadTemplateSummaries(dir)
	if err != nil {
		t.Fatalf("loadTemplateSummaries returned error: %v", err)
	}
	if len(summaries) == 0 {
		t.Fatalf("no templates discovered")
	}

	name := summaries[0].Name
	detail, err := loadTemplateDetail(dir, name)
	if err != nil {
		t.Fatalf("loadTemplateDetail returned error: %v", err)
	}
	if len(detail.Content) == 0 {
		t.Fatalf("template detail content is empty for %s", name)
	}

	var payload map[string]any
	if err := json.Unmarshal(detail.Content, &payload); err != nil {
		t.Fatalf("template content is not valid JSON: %v", err)
	}
	if payload["title"] == "" {
		t.Fatalf("template content missing title field: %s", name)
	}
}
