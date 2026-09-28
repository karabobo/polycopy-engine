//! Parser for the non-secret `KEY=VALUE` runtime env file
//! (`/etc/polycopy-engine/persistent-public.env`).
//!
//! systemd's `EnvironmentFile=` is the real consumer; this parser only has to
//! agree with it for the simple form the project writes: one assignment per
//! line, `#` comments, blank lines, and optional matching single or double
//! quotes around the value. The secret credential file is never parsed here:
//! the panel only checks that it exists.

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EnvFile {
    entries: Vec<(String, String)>,
}

impl EnvFile {
    pub fn parse(text: &str) -> Self {
        let mut entries: Vec<(String, String)> = Vec::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let line = line.strip_prefix("export ").unwrap_or(line);
            let Some((key, value)) = line.split_once('=') else {
                continue;
            };
            let key = key.trim().to_owned();
            if key.is_empty() {
                continue;
            }
            let value = unquote(value.trim()).to_owned();
            // Later assignments win, exactly like systemd and a shell.
            if let Some(existing) = entries.iter_mut().find(|(k, _)| *k == key) {
                existing.1 = value;
            } else {
                entries.push((key, value));
            }
        }
        Self { entries }
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.entries
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    pub fn entries(&self) -> &[(String, String)] {
        &self.entries
    }
}

fn unquote(value: &str) -> &str {
    for quote in ['"', '\''] {
        if value.len() >= 2 && value.starts_with(quote) && value.ends_with(quote) {
            return &value[1..value.len() - 1];
        }
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_assignments_comments_quotes_and_last_assignment_wins() {
        let env = EnvFile::parse(
            "# comment\n\nA=1\nexport B = \"two words\"\nC='x'\nA=3\nnot an assignment\n=empty\n",
        );
        assert_eq!(env.get("A"), Some("3"));
        assert_eq!(env.get("B"), Some("two words"));
        assert_eq!(env.get("C"), Some("x"));
        assert_eq!(env.get("missing"), None);
        assert_eq!(env.entries().len(), 3);
    }
}
