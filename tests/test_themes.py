from personal_log.themes import get_theme

def test_theme_is_copy():
    a = get_theme("modern")
    a["recording_accent"] = "modified"
    assert get_theme("modern")["recording_accent"] != "modified"

def test_all_themes_have_required_tokens():
    for theme in ("modern", "starship", "classic"):
        assert {"background", "foreground", "recording_accent"} <= get_theme(theme).keys()
