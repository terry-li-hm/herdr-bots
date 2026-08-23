package pane

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/terry-li-hm/herdr-bots/internal/herdr"
	"github.com/terry-li-hm/herdr-bots/internal/store"
)

var (
	titleStyle    = lipgloss.NewStyle().Bold(true)
	dimStyle      = lipgloss.NewStyle().Faint(true)
	selectedStyle = lipgloss.NewStyle().Reverse(true)
	failureStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
	successStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
)

type model struct {
	state  *store.Store
	herdr  *herdr.CLI
	runs   []store.Run
	cursor int
	width  int
	err    error
	notice string
}

type refreshMsg struct{}
type focusMsg struct{ err error }

func Run(state *store.Store, client *herdr.CLI) error {
	_, err := tea.NewProgram(load(state, client), tea.WithAltScreen()).Run()
	return err
}

func load(state *store.Store, client *herdr.CLI) model {
	runs, err := state.ListRuns(context.Background(), "", 100)
	return model{state: state, herdr: client, runs: runs, err: err}
}

func (m model) Init() tea.Cmd { return tick() }
func tick() tea.Cmd           { return tea.Tick(2*time.Second, func(time.Time) tea.Msg { return refreshMsg{} }) }

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
	case refreshMsg:
		next := load(m.state, m.herdr)
		next.cursor = m.cursor
		next.width = m.width
		next.notice = m.notice
		if next.cursor >= len(next.runs) {
			next.cursor = max(0, len(next.runs)-1)
		}
		return next, tick()
	case focusMsg:
		if msg.err != nil {
			m.notice = msg.err.Error()
			return m, nil
		}
		return m, tea.Quit
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "esc", "ctrl+c":
			return m, tea.Quit
		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
			}
		case "down", "j":
			if m.cursor < len(m.runs)-1 {
				m.cursor++
			}
		case "enter":
			if m.cursor >= len(m.runs) {
				return m, nil
			}
			run := m.runs[m.cursor]
			if run.WorkspaceID == "" {
				m.notice = "run has no Herdr workspace"
				return m, nil
			}
			return m, func() tea.Msg {
				err := m.herdr.Focus(context.Background(), run.WorkspaceID, run.PaneID)
				if err == nil {
					err = m.state.MarkRead(context.Background(), run.ID)
				}
				return focusMsg{err: err}
			}
		}
	}
	return m, nil
}

func (m model) View() string {
	help := dimStyle.Render("  enter: open run  j/k: move  q: quit")
	out := titleStyle.Render("Bot inbox") + help + "\n\n"
	if m.err != nil {
		return out + failureStyle.Render(m.err.Error()) + "\n"
	}
	if len(m.runs) == 0 {
		return out + dimStyle.Render("No bot runs yet.") + "\n"
	}
	for i, run := range m.runs {
		mark := " "
		if run.Unread {
			mark = "*"
		}
		plain := fmt.Sprintf("%s %-22s %-13s %-10s %s", mark, truncate(run.JobID, 22), truncate(run.State, 13), truncate(run.TaskVerdict, 10), run.UpdatedAt.Local().Format("02 Jan 15:04"))
		if i == m.cursor {
			out += selectedStyle.Render(" "+plain+" ") + "\n"
			continue
		}
		style := dimStyle
		if run.State == store.StateFailed || run.State == store.StateBlocked || run.State == store.StateInterrupted {
			style = failureStyle
		}
		if run.State == store.StateSucceeded {
			style = successStyle
		}
		out += style.Render(" "+plain) + "\n"
	}
	if m.notice != "" {
		width := m.width
		if width < 20 {
			width = 80
		}
		out += "\n" + failureStyle.Render(truncate(strings.Join(strings.Fields(m.notice), " "), width-2)) + "\n"
	}
	return out
}

func truncate(value string, limit int) string {
	runes := []rune(value)
	if limit <= 0 || len(runes) <= limit {
		return value
	}
	return string(runes[:limit-1]) + "…"
}
