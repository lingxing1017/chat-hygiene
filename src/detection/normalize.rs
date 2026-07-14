use std::collections::BTreeSet;
use std::sync::LazyLock;

use idna::domain_to_ascii;
use regex::Regex;
use sha2::{Digest, Sha256};
use unicode_normalization::UnicodeNormalization;
use unicode_segmentation::UnicodeSegmentation;
use url::Url;

use super::{MessageContent, MessageEntityKind};

static URL_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?:https?://|www\.|t\.me/)[^\s<>()]+").expect("static URL regex must compile")
});
static MENTION_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?:^|\s)@[a-z0-9_]{3,}").expect("static mention regex must compile")
});
static EMAIL_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b")
        .expect("static email regex must compile")
});
static PHONE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?:^|\s)\+?[0-9][0-9 ()-]{6,}[0-9](?:$|\s)")
        .expect("static phone regex must compile")
});
static SPACED_WORD_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?:^|\s)(?:[a-z]\s+){3,}[a-z](?:$|\s)")
        .expect("static spaced-word regex must compile")
});

#[derive(Debug)]
pub(crate) struct NormalizedContent {
    pub text: String,
    pub normalized_hash: String,
    pub links: Vec<NormalizedLink>,
    pub evasion: bool,
    pub contact: bool,
    pub mention_count: usize,
    pub emoji_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct NormalizedLink {
    pub canonical: String,
    pub domain: String,
    pub telegram_invite: bool,
}

pub(crate) fn normalize(content: &MessageContent) -> NormalizedContent {
    let source = [content.text.as_deref(), content.caption.as_deref()]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join("\n");
    let had_zero_width = source.chars().any(is_zero_width);
    let nfkc: String = source.nfkc().collect();
    let without_zero_width: String = nfkc
        .chars()
        .filter(|character| !is_zero_width(*character))
        .collect();
    let text = without_zero_width
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase();

    let mut raw_links = URL_RE
        .find_iter(&text)
        .map(|matched| matched.as_str().to_owned())
        .collect::<Vec<_>>();
    let mut mention_count = MENTION_RE.find_iter(&text).count();
    let mut entity_contact = false;
    for entity in &content.entities {
        match entity.kind {
            MessageEntityKind::Url | MessageEntityKind::TextLink => {
                raw_links.push(entity.value.clone());
            }
            MessageEntityKind::Mention => {
                mention_count += 1;
                entity_contact = true;
            }
            MessageEntityKind::PhoneNumber => entity_contact = true,
            MessageEntityKind::Other => {}
        }
    }

    let links = raw_links
        .into_iter()
        .filter_map(|raw| normalize_link(&raw))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mixed_script = text.split_whitespace().any(has_mixed_latin_cyrillic);
    let evasion = had_zero_width || mixed_script || SPACED_WORD_RE.is_match(&text);
    let contact = entity_contact
        || EMAIL_RE.is_match(&text)
        || PHONE_RE.is_match(&format!(" {text} "))
        || MENTION_RE.is_match(&format!(" {text}"))
        || text.contains("contact me")
        || text.contains("联系我")
        || text.contains("私聊我");
    let emoji_count = text
        .graphemes(true)
        .filter(|grapheme| grapheme.chars().any(is_emoji))
        .count();
    let normalized_hash = hash_content(&text, &links);

    NormalizedContent {
        text,
        normalized_hash,
        links,
        evasion,
        contact,
        mention_count,
        emoji_count,
    }
}

fn normalize_link(raw: &str) -> Option<NormalizedLink> {
    let trimmed = raw.trim_end_matches(['.', ',', ';', ':', '!', '?', ')', ']', '}']);
    let candidate = if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_owned()
    } else {
        format!("https://{trimmed}")
    };
    let parsed = Url::parse(&candidate).ok()?;
    let domain = domain_to_ascii(parsed.host_str()?).ok()?.to_lowercase();
    let domain = domain.strip_prefix("www.").unwrap_or(&domain).to_owned();
    let path = parsed.path().to_lowercase();
    let telegram_invite =
        domain == "t.me" && (path.starts_with("/+") || path.starts_with("/joinchat/"));
    let canonical = format!("{}{}", domain, parsed.path());
    Some(NormalizedLink {
        canonical,
        domain,
        telegram_invite,
    })
}

fn hash_content(text: &str, links: &[NormalizedLink]) -> String {
    let mut digest = Sha256::new();
    digest.update(text.as_bytes());
    for link in links {
        digest.update(b"\n");
        digest.update(link.canonical.as_bytes());
    }
    hex::encode(digest.finalize())
}

fn has_mixed_latin_cyrillic(token: &str) -> bool {
    let has_latin = token.chars().any(is_latin);
    let has_cyrillic = token.chars().any(is_cyrillic);
    has_latin && has_cyrillic
}

const fn is_zero_width(character: char) -> bool {
    matches!(
        character,
        '\u{200b}' | '\u{200c}' | '\u{200d}' | '\u{2060}' | '\u{feff}'
    )
}

fn is_latin(character: char) -> bool {
    character.is_ascii_alphabetic() || matches!(character as u32, 0x00c0..=0x024f | 0x1e00..=0x1eff)
}

fn is_cyrillic(character: char) -> bool {
    matches!(character as u32, 0x0400..=0x052f | 0x2de0..=0x2dff | 0xa640..=0xa69f)
}

fn is_emoji(character: char) -> bool {
    matches!(character as u32, 0x1f300..=0x1faff | 0x2600..=0x26ff | 0x2700..=0x27bf)
}
