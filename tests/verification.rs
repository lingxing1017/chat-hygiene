use chathygiene::verification::{
    AnswerKind, ArithmeticVerifier, ChallengeVerifier, GeneratedChallenge,
};
use chrono::{DateTime, Duration, Utc};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use secrecy::{SecretSlice, SecretString};

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
    calculate(first, parts[1], second, parts[3], third).1
}

fn calculate(
    first: i32,
    first_operator: &str,
    second: i32,
    second_operator: &str,
    third: i32,
) -> (i32, i32) {
    if second_operator == "×" && first_operator != "×" {
        let intermediate = apply(second, second_operator, third);
        (intermediate, apply(first, first_operator, intermediate))
    } else {
        let intermediate = apply(first, first_operator, second);
        (intermediate, apply(intermediate, second_operator, third))
    }
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
    assert_eq!(challenge.hmac_key_version, 0);
    assert!(!challenge.is_expired(challenge.expires_at - Duration::nanoseconds(1)));
    assert!(challenge.is_expired(challenge.expires_at));
    assert_ne!(challenge.answer_hmac, solve(&challenge).to_string());
}

#[test]
fn versioned_verifier_marks_generated_challenges() {
    let mut verifier = ArithmeticVerifier::new_with_key_version(
        StdRng::seed_from_u64(13),
        SecretSlice::from(b"version-one-key".to_vec()),
        1,
    );
    let challenge = verifier.generate(at("2026-07-14T00:00:00Z"));

    assert_eq!(verifier.key_version(), 1);
    assert_eq!(challenge.hmac_key_version, 1);
}

#[test]
fn persisted_expression_signing_matches_frozen_hmac() {
    let verifier = ArithmeticVerifier::new_with_key_version(
        StdRng::seed_from_u64(17),
        SecretSlice::from(
            hex::decode("bd442488308239145955b5d27edefe6f3b3c12e7996f099def53e114477253cb")
                .unwrap(),
        ),
        1,
    );
    let hmac = verifier.answer_hmac_for_expression("7 + 5 - 3").unwrap();
    assert!(
        hmac == "bd1ceaa24bdf20a8f2311171aa2bce5155ce12f48928a838b90fe837c28061ba",
        "persisted expression HMAC no longer matches the frozen vector"
    );
}

#[test]
fn persisted_expression_signing_rejects_noncanonical_or_out_of_range_input() {
    let verifier = ArithmeticVerifier::new_with_key_version(
        StdRng::seed_from_u64(19),
        SecretSlice::from(b"version-one-key".to_vec()),
        1,
    );
    for invalid in [
        "7+5-3",
        "7  + 5 - 3",
        "7\t+\t5 - 3",
        "7\n+ 5 - 3",
        "7 / 1 + 2",
        "-1 + 2 + 3",
        "100 + 0 + 0",
        "7 + 5",
        "7 + 5 - 3 extra",
        "99 × 99 - 0",
    ] {
        let error = verifier.answer_hmac_for_expression(invalid).unwrap_err();
        assert_eq!(
            error.to_string(),
            "persisted challenge expression is invalid"
        );
    }
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
        let (intermediate, result) = calculate(first, parts[1], second, parts[3], third);
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
