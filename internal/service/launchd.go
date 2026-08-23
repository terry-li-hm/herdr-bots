package service

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"path/filepath"
)

const Label = "com.terry.herdr-bots"

type LaunchdConfig struct {
	Binary     string
	ConfigPath string
	StatePath  string
	LogPath    string
	Home       string
	Path       string
}

func RenderLaunchd(cfg LaunchdConfig) ([]byte, error) {
	for name, value := range map[string]string{"binary": cfg.Binary, "config": cfg.ConfigPath, "state": cfg.StatePath, "log": cfg.LogPath, "home": cfg.Home, "path": cfg.Path} {
		if value == "" {
			return nil, fmt.Errorf("%s is required", name)
		}
	}
	for name, value := range map[string]string{"binary": cfg.Binary, "config": cfg.ConfigPath, "state": cfg.StatePath, "log": cfg.LogPath, "home": cfg.Home} {
		if !filepath.IsAbs(value) {
			return nil, fmt.Errorf("%s must be an absolute path", name)
		}
	}
	var b bytes.Buffer
	b.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
	b.WriteString("<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n")
	b.WriteString("<plist version=\"1.0\">\n<dict>\n")
	keyString(&b, "Label", Label)
	b.WriteString("  <key>ProgramArguments</key>\n  <array>\n")
	for _, arg := range []string{cfg.Binary, "daemon", "--config", cfg.ConfigPath, "--state", cfg.StatePath} {
		b.WriteString("    <string>" + escape(arg) + "</string>\n")
	}
	b.WriteString("  </array>\n")
	b.WriteString("  <key>EnvironmentVariables</key>\n  <dict>\n")
	keyStringIndented(&b, "HOME", cfg.Home, 4)
	keyStringIndented(&b, "PATH", cfg.Path, 4)
	b.WriteString("  </dict>\n")
	b.WriteString("  <key>RunAtLoad</key>\n  <true/>\n")
	b.WriteString("  <key>KeepAlive</key>\n  <dict>\n    <key>SuccessfulExit</key>\n    <false/>\n  </dict>\n")
	b.WriteString("  <key>ThrottleInterval</key>\n  <integer>5</integer>\n")
	keyString(&b, "StandardOutPath", cfg.LogPath)
	keyString(&b, "StandardErrorPath", cfg.LogPath)
	b.WriteString("</dict>\n</plist>\n")
	return b.Bytes(), nil
}

func keyString(b *bytes.Buffer, key, value string) { keyStringIndented(b, key, value, 2) }
func keyStringIndented(b *bytes.Buffer, key, value string, spaces int) {
	indent := bytes.Repeat([]byte(" "), spaces)
	b.Write(indent)
	b.WriteString("<key>" + escape(key) + "</key>\n")
	b.Write(indent)
	b.WriteString("<string>" + escape(value) + "</string>\n")
}
func escape(value string) string {
	var b bytes.Buffer
	_ = xml.EscapeText(&b, []byte(value))
	return b.String()
}
