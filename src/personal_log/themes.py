"""Presentation tokens only; must not influence journal storage or processing."""

THEMES = {
    "modern": {"background": "#171A20", "foreground": "#F1F3F5", "recording_accent": "#5BA7E8"},
    "starship": {"background": "#101017", "foreground": "#F5EDE8", "recording_accent": "#FF9C52"},
    "classic": {"background": "#F4EBDD", "foreground": "#29241F", "recording_accent": "#873E4B"},
}

def get_theme(name: str) -> dict[str, str]:
    return dict(THEMES[name])
