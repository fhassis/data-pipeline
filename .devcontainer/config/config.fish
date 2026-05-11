# Disable default greeting
set -g fish_greeting ''

# Initialize starship prompt only for interactive sessions
# This guard is required for JetBrains IDEs (WebStorm, etc.) which may
# launch fish without a proper TTY before opening the terminal.
if status is-interactive
    starship init fish | source
end

# Aliases
alias ls='eza -l --icons'
alias ll='eza -la --icons'
