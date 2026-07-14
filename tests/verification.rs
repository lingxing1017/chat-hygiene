use chathygiene::verification::{
    AnswerKind, ArithmeticVerifier, ChallengeVerifier, GeneratedChallenge,
};
use chrono::{DateTime, Duration, Utc};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use secrecy::SecretString;

fn at(value: &str) -> DateTime<Utc> {
    value.parse().expect("valid timestamp")
}

fn verifier(seed: u64) -> ArithmeticVerifier<StdRng> {
    ArithmeticVerifier::new(StdRng::seed_from_u64(seed), SecretString::from("test-key"))
}

fn solve(challenge: &GeneratedChallenge) -> i32 {
    let parts: Vec<_> = challenge.expression.split_whitespace().collect();
    assert_eq!(parts.len(), 5);
    let first = parts[0].parse::<i32>().expect("first operand");
    let second = parts[2].parse::<i32>().expect("second operand");
    let third = parts[4].parse::<i32>().expect("third operand");
    apply(apply(first, parts[1], second), parts[3], third)
}

fn apply(left: i32, operator: &str, right: i32) -> i32 {
    match operator {
        "+" => left + right,
        "-" => left - right,
        "×" => left * right,
        other => panic!("unexpected operator {other}"),
    }
}

fn full_width(value: i32) -> String {
    value
        .to_string()
        .chars()
        .map(|character| match character {
            '0'..='9' => char::from_u32(u32::from(character) + 0xfee0).unwrap(),
            other => other,
        })
        .collect()
}

#[test]
fn accepts_normalized_integer_answers_only() {
    let mut verifier = verifier(7);
    let challenge = verifier.generate(at("2026-07-14T00:00:00Z"));
    let answer = solve(&challenge);

    assert_eq!(
        verifier.evaluate(&answer.to_string(), &challenge.answer_hmac),
        AnswerKind::Correct
    );
    assert_eq!(
        verifier.evaluate(
            &format!("  +{}  ", full_width(answer)),
            &challenge.answer_hmac
        ),
        AnswerKind::Correct
    );
    assert_eq!(
        verifier.evaluate(&(answer + 1).to_string(), &challenge.answer_hmac),
        AnswerKind::Incorrect
    );
    for invalid in ["1.0", "7 = 7", "seven", "", "++7", "-1", "1 2"] {
        assert_eq!(
            verifier.evaluate(invalid, &challenge.answer_hmac),
            AnswerKind::NonNumeric,
            "{invalid:?} must not consume an attempt"
        );
    }
}

#[test]
fn challenge_has_two_minute_deadline_and_three_attempts() {
    let now = at("2026-07-14T00:00:00Z");
    let challenge = verifier(11).generate(now);

    assert_eq!(challenge.created_at, now);
    assert_eq!(challenge.expires_at, now + Duration::minutes(2));
    assert_eq!(challenge.max_attempts, 3);
    assert!(!challenge.is_expired(challenge.expires_at - Duration::nanoseconds(1)));
    assert!(challenge.is_expired(challenge.expires_at));
    assert_ne!(challenge.answer_hmac, solve(&challenge).to_string());
}

#[test]
fn generates_one_thousand_bounded_challenges() {
    let mut verifier = verifier(42);
    let now = at("2026-07-14T00:00:00Z");

    for _ in 0..1_000 {
        let challenge = verifier.generate(now);
        let parts: Vec<_> = challenge.expression.split_whitespace().collect();
        assert_eq!(parts.len(), 5);
        assert!(["+", "-", "×"].contains(&parts[1]));
        assert!(["+", "-", "×"].contains(&parts[3]));

        let first = parts[0].parse::<i32>().unwrap();
        let second = parts[2].parse::<i32>().unwrap();
        let third = parts[4].parse::<i32>().unwrap();
        let intermediate = apply(first, parts[1], second);
        let result = apply(intermediate, parts[3], third);
        assert!((0..=99).contains(&intermediate));
        assert!((0..=99).contains(&result));
        assert_eq!(
            verifier.evaluate(&result.to_string(), &challenge.answer_hmac),
            AnswerKind::Correct
        );
    }
}

struct MaxRng;

impl RngCore for MaxRng {
    fn next_u32(&mut self) -> u32 {
        u32::MAX
    }

    fn next_u64(&mut self) -> u64 {
        u64::MAX
    }

    fn fill_bytes(&mut self, destination: &mut [u8]) {
        destination.fill(u8::MAX);
    }
}

#[test]
fn falls_back_after_invalid_random_candidates() {
    let mut verifier = ArithmeticVerifier::new(MaxRng, SecretString::from("test-key"));
    let challenge = verifier.generate(at("2026-07-14T00:00:00Z"));

    assert_eq!(challenge.expression, "7 + 5 - 3");
    assert_eq!(
        verifier.evaluate("9", &challenge.answer_hmac),
        AnswerKind::Correct
    );
}
