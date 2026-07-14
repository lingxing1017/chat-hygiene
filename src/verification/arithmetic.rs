use chrono::{DateTime, Duration, Utc};
use hmac::{Hmac, Mac};
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};
use secrecy::{ExposeSecret, SecretString};
use sha2::Sha256;
use unicode_normalization::UnicodeNormalization;

const MAX_RANDOM_CANDIDATES: usize = 100;
const FALLBACK_EXPRESSION: &str = "7 + 5 - 3";
const FALLBACK_ANSWER: i32 = 9;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnswerKind {
    Correct,
    Incorrect,
    NonNumeric,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedChallenge {
    pub expression: String,
    pub answer_hmac: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub max_attempts: u8,
}

impl GeneratedChallenge {
    #[must_use]
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        now >= self.expires_at
    }
}

pub trait ChallengeVerifier: Send {
    fn generate(&mut self, now: DateTime<Utc>) -> GeneratedChallenge;
    fn evaluate(&self, raw: &str, expected_hmac: &str) -> AnswerKind;
}

pub struct ArithmeticVerifier<R> {
    rng: R,
    key: SecretString,
}

impl<R> ArithmeticVerifier<R> {
    pub fn new(rng: R, key: SecretString) -> Self {
        Self { rng, key }
    }
}

impl ArithmeticVerifier<StdRng> {
    #[must_use]
    pub fn from_os_rng(key: SecretString) -> Self {
        Self::new(StdRng::from_os_rng(), key)
    }
}

impl<R: RngCore + Send> ChallengeVerifier for ArithmeticVerifier<R> {
    fn generate(&mut self, now: DateTime<Utc>) -> GeneratedChallenge {
        let (expression, answer) = (0..MAX_RANDOM_CANDIDATES)
            .find_map(|_| random_candidate(&mut self.rng))
            .unwrap_or_else(|| (FALLBACK_EXPRESSION.to_owned(), FALLBACK_ANSWER));

        GeneratedChallenge {
            expression,
            answer_hmac: self.answer_hmac(&answer.to_string()),
            created_at: now,
            expires_at: now + Duration::minutes(2),
            max_attempts: 3,
        }
    }

    fn evaluate(&self, raw: &str, expected_hmac: &str) -> AnswerKind {
        let Some(answer) = normalize_answer(raw) else {
            return AnswerKind::NonNumeric;
        };
        let Ok(expected_tag) = hex::decode(expected_hmac) else {
            return AnswerKind::Incorrect;
        };
        let mut mac = self.mac();
        mac.update(answer.as_bytes());
        if mac.verify_slice(&expected_tag).is_ok() {
            AnswerKind::Correct
        } else {
            AnswerKind::Incorrect
        }
    }
}

impl<R> ArithmeticVerifier<R> {
    fn answer_hmac(&self, answer: &str) -> String {
        let mut mac = self.mac();
        mac.update(answer.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    fn mac(&self) -> HmacSha256 {
        HmacSha256::new_from_slice(self.key.expose_secret().as_bytes())
            .expect("HMAC accepts keys of any size")
    }
}

#[derive(Clone, Copy)]
enum Operator {
    Add,
    Subtract,
    Multiply,
}

impl Operator {
    const fn symbol(self) -> &'static str {
        match self {
            Self::Add => "+",
            Self::Subtract => "-",
            Self::Multiply => "×",
        }
    }

    const fn apply(self, left: i32, right: i32) -> i32 {
        match self {
            Self::Add => left + right,
            Self::Subtract => left - right,
            Self::Multiply => left * right,
        }
    }
}

fn random_candidate<R: RngCore>(rng: &mut R) -> Option<(String, i32)> {
    let first = rng.random_range(0..=99_i32);
    let second = rng.random_range(0..=99_i32);
    let third = rng.random_range(0..=99_i32);
    let first_operator = random_operator(rng);
    let second_operator = random_operator(rng);
    let intermediate = first_operator.apply(first, second);
    if !(0..=99).contains(&intermediate) {
        return None;
    }
    let answer = second_operator.apply(intermediate, third);
    if !(0..=99).contains(&answer) {
        return None;
    }

    Some((
        format!(
            "{first} {} {second} {} {third}",
            first_operator.symbol(),
            second_operator.symbol()
        ),
        answer,
    ))
}

fn random_operator<R: RngCore>(rng: &mut R) -> Operator {
    match rng.random_range(0..3_u8) {
        0 => Operator::Add,
        1 => Operator::Subtract,
        _ => Operator::Multiply,
    }
}

fn normalize_answer(raw: &str) -> Option<String> {
    let normalized: String = raw.nfkc().collect();
    let mut value = normalized.trim();
    if let Some(unsigned) = value.strip_prefix('+') {
        value = unsigned;
    }
    if value.is_empty() || !value.chars().all(|character| character.is_ascii_digit()) {
        return None;
    }
    let canonical = value.trim_start_matches('0');
    Some(if canonical.is_empty() {
        "0".to_owned()
    } else {
        canonical.to_owned()
    })
}
