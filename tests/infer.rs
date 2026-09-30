use csv::{
    InferenceReport, Inferencer, InferencerBuilder, InferredType,
    ReaderBuilder,
};

fn infer(data: &[u8]) -> InferenceReport {
    let mut rdr = ReaderBuilder::new().from_reader(data);
    Inferencer::new().infer(&mut rdr).unwrap()
}

fn infer_no_headers(data: &[u8]) -> InferenceReport {
    let mut rdr = ReaderBuilder::new().has_headers(false).from_reader(data);
    Inferencer::new().infer(&mut rdr).unwrap()
}

/// Infers the type of a single one-column field.
fn field_type(s: &str) -> InferredType {
    let data = format!("{}\n", s);
    let report = infer_no_headers(data.as_bytes());
    assert_eq!(report.columns().len(), 1, "field: {:?}", s);
    report.columns()[0].inferred_type()
}

#[test]
fn basic_types_nulls_and_promotions() {
    // Covers the acceptance fixture: true, 0, 001, 1.20 and empty fields.
    let data = "flag,num,code,ratio,note,mixed,boolnum
true,0,001,1.20,,1,true
false,5,002,3.50,,2.5,1
";
    let report = infer(data.as_bytes());
    assert!(report.complete());
    assert_eq!(report.records_scanned(), 2);
    assert_eq!(
        report.headers().unwrap(),
        &["flag", "num", "code", "ratio", "note", "mixed", "boolnum"]
    );
    let cols = report.columns();
    assert_eq!(cols.len(), 7);

    assert_eq!(cols[0].inferred_type(), InferredType::Boolean);
    assert_eq!(cols[0].null_count(), 0);
    assert_eq!(cols[0].first_promotion(), None);

    assert_eq!(cols[1].inferred_type(), InferredType::Integer);

    // 001 has a leading zero, so the column stays text.
    assert_eq!(cols[2].inferred_type(), InferredType::Text);

    // 1.20 is an exact decimal, never an f64.
    assert_eq!(cols[3].inferred_type(), InferredType::Decimal);

    // All-empty column stays null and counts its empty fields.
    assert_eq!(cols[4].inferred_type(), InferredType::Null);
    assert_eq!(cols[4].null_count(), 2);
    assert_eq!(cols[4].first_promotion(), None);

    // 1 then 2.5: integer promoted to decimal by record 1 (line 3).
    assert_eq!(cols[5].inferred_type(), InferredType::Decimal);
    let p = cols[5].first_promotion().unwrap();
    assert_eq!(p.from(), InferredType::Integer);
    assert_eq!(p.to(), InferredType::Decimal);
    assert_eq!(p.record(), 1);
    assert_eq!(p.line(), 3);

    // true then 1: boolean mixed with a number promotes to text.
    assert_eq!(cols[6].inferred_type(), InferredType::Text);
    let p = cols[6].first_promotion().unwrap();
    assert_eq!(p.from(), InferredType::Boolean);
    assert_eq!(p.to(), InferredType::Text);
    assert_eq!(p.record(), 1);
    assert_eq!(p.line(), 3);
}

#[test]
fn null_does_not_promote() {
    // A quoted empty field is an empty (null) field, not an empty line.
    let report = infer_no_headers(b"1\n\"\"\n2\n");
    let col = &report.columns()[0];
    assert_eq!(col.inferred_type(), InferredType::Integer);
    assert_eq!(col.null_count(), 1);
    // The first non-null field sets the type; this is not a promotion.
    assert_eq!(col.first_promotion(), None);
}

#[test]
fn number_grammar() {
    // Booleans: only the exact lowercase strings.
    assert_eq!(field_type("true"), InferredType::Boolean);
    assert_eq!(field_type("false"), InferredType::Boolean);
    assert_eq!(field_type("True"), InferredType::Text);
    assert_eq!(field_type("TRUE"), InferredType::Text);

    // Integers, including negatives and zero.
    assert_eq!(field_type("0"), InferredType::Integer);
    assert_eq!(field_type("-0"), InferredType::Integer);
    assert_eq!(field_type("42"), InferredType::Integer);
    assert_eq!(field_type("-42"), InferredType::Integer);
    assert_eq!(field_type("9223372036854775807"), InferredType::Integer);
    assert_eq!(field_type("-9223372036854775808"), InferredType::Integer);

    // Leading zeros are kept as text.
    assert_eq!(field_type("001"), InferredType::Text);
    assert_eq!(field_type("00"), InferredType::Text);
    assert_eq!(field_type("-01"), InferredType::Text);

    // No trimming: surrounding whitespace means text.
    assert_eq!(field_type(" 1"), InferredType::Text);
    assert_eq!(field_type("1 "), InferredType::Text);
    assert_eq!(field_type(" true"), InferredType::Text);

    // Decimals: fractions and scientific notation, never via f64.
    assert_eq!(field_type("1.20"), InferredType::Decimal);
    assert_eq!(field_type("-1.5"), InferredType::Decimal);
    assert_eq!(field_type("0.5"), InferredType::Decimal);
    assert_eq!(field_type("1e3"), InferredType::Decimal);
    assert_eq!(field_type("1E3"), InferredType::Decimal);
    assert_eq!(field_type("1e+3"), InferredType::Decimal);
    assert_eq!(field_type("2e-3"), InferredType::Decimal);
    assert_eq!(field_type("1.5e10"), InferredType::Decimal);

    // Integers overflowing an i64 are exact decimals, not text.
    assert_eq!(field_type("9223372036854775808"), InferredType::Decimal);
    assert_eq!(field_type("-9223372036854775809"), InferredType::Decimal);
    assert_eq!(
        field_type("123456789012345678901234567890"),
        InferredType::Decimal
    );

    // Grammar violations are text.
    assert_eq!(field_type("1e"), InferredType::Text);
    assert_eq!(field_type("1e+"), InferredType::Text);
    assert_eq!(field_type("1."), InferredType::Text);
    assert_eq!(field_type(".5"), InferredType::Text);
    assert_eq!(field_type("+1"), InferredType::Text);
    assert_eq!(field_type("--1"), InferredType::Text);
    assert_eq!(field_type("-"), InferredType::Text);
    assert_eq!(field_type("1.2.3"), InferredType::Text);
    assert_eq!(field_type("1 000"), InferredType::Text);
    assert_eq!(field_type("abc"), InferredType::Text);
}

#[test]
fn integer_then_decimal_merges_to_decimal() {
    let report = infer_no_headers(b"1\n2\n3.0\n4\n");
    let col = &report.columns()[0];
    assert_eq!(col.inferred_type(), InferredType::Decimal);
    let p = col.first_promotion().unwrap();
    assert_eq!(p.from(), InferredType::Integer);
    assert_eq!(p.to(), InferredType::Decimal);
    assert_eq!(p.record(), 2);
    assert_eq!(p.line(), 3);
}

#[test]
fn non_utf8_field_promotes_to_text() {
    let data = b"a\n1\n\xff\xfe\n";
    let report = infer(data);
    let col = &report.columns()[0];
    assert_eq!(col.inferred_type(), InferredType::Text);
    let p = col.first_promotion().unwrap();
    assert_eq!(p.from(), InferredType::Integer);
    assert_eq!(p.to(), InferredType::Text);
    assert_eq!(p.record(), 1);
}

#[test]
fn duplicate_headers_are_preserved() {
    let report = infer(b"a,a\n1,2\n");
    assert_eq!(report.headers().unwrap(), &["a", "a"]);
    assert_eq!(report.columns().len(), 2);
    assert_eq!(report.columns()[0].inferred_type(), InferredType::Integer);
    assert_eq!(report.columns()[1].inferred_type(), InferredType::Integer);
}

#[test]
fn multiline_fields() {
    // The quoted field spans lines 3-4, so the record holding 2.5 starts
    // on line 5. The multiline text record itself starts on line 3.
    let data = "num\n1\n\"a\nb\"\n2.5\n";
    let report = infer(data.as_bytes());
    assert_eq!(report.records_scanned(), 3);
    let col = &report.columns()[0];
    assert_eq!(col.inferred_type(), InferredType::Text);
    let p = col.first_promotion().unwrap();
    assert_eq!(p.from(), InferredType::Integer);
    assert_eq!(p.to(), InferredType::Text);
    assert_eq!(p.record(), 1);
    assert_eq!(p.line(), 3);
}

#[test]
fn sample_limit_marks_prefix_only_report() {
    let data = b"a\n1\n2\n3\n4\n5\n";

    // No limit: the whole input is scanned.
    let report = infer(data);
    assert!(report.complete());
    assert_eq!(report.records_scanned(), 5);

    // Limit below the record count: prefix-only conclusion.
    let mut rdr = ReaderBuilder::new().from_reader(&data[..]);
    let report = Inferencer::with_sample_limit(2).infer(&mut rdr).unwrap();
    assert!(!report.complete());
    assert_eq!(report.records_scanned(), 2);
    assert_eq!(report.columns()[0].inferred_type(), InferredType::Integer);

    // A limit of zero scans nothing but still reports incompleteness.
    let mut rdr = ReaderBuilder::new().from_reader(&data[..]);
    let report = Inferencer::with_sample_limit(0).infer(&mut rdr).unwrap();
    assert!(!report.complete());
    assert_eq!(report.records_scanned(), 0);

    // A limit exactly at the record count is still a complete scan.
    let mut rdr = ReaderBuilder::new().from_reader(&data[..]);
    let report = Inferencer::with_sample_limit(5).infer(&mut rdr).unwrap();
    assert!(report.complete());
    assert_eq!(report.records_scanned(), 5);

    // A limit above the record count is complete too.
    let mut rdr = ReaderBuilder::new().from_reader(&data[..]);
    let report = Inferencer::with_sample_limit(100).infer(&mut rdr).unwrap();
    assert!(report.complete());
    assert_eq!(report.records_scanned(), 5);

    // The builder exposes the same knob.
    let mut rdr = ReaderBuilder::new().from_reader(&data[..]);
    let report = InferencerBuilder::new()
        .sample_limit(Some(3))
        .build()
        .infer(&mut rdr)
        .unwrap();
    assert!(!report.complete());
    assert_eq!(report.records_scanned(), 3);
}

#[test]
fn invalid_csv_is_an_error() {
    // Inconsistent record lengths are rejected by the default reader.
    let mut rdr = ReaderBuilder::new().from_reader(&b"a,b\n1,2,3\n"[..]);
    let result = Inferencer::new().infer(&mut rdr);
    assert!(result.is_err());
}

#[test]
fn headers_only_input() {
    let report = infer(b"a,b\n");
    assert!(report.complete());
    assert_eq!(report.records_scanned(), 0);
    assert_eq!(report.columns().len(), 2);
    assert_eq!(report.columns()[0].inferred_type(), InferredType::Null);
    assert_eq!(report.columns()[1].null_count(), 0);
}

#[test]
fn no_headers_mode() {
    let report = infer_no_headers(b"true,1\nfalse,2\n");
    assert_eq!(report.headers(), None);
    assert_eq!(report.columns()[0].inferred_type(), InferredType::Boolean);
    assert_eq!(report.columns()[1].inferred_type(), InferredType::Integer);
}
