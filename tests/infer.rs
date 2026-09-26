use csv::infer::{Inferencer, Report, Type};
use csv::{ErrorKind, StringRecord};

fn infer(data: &[u8]) -> Report {
    Inferencer::new().infer_reader(data).unwrap()
}

fn infer_with_limit(data: &[u8], limit: Option<u64>) -> Report {
    let mut inferencer = Inferencer::new();
    inferencer.sample_limit(limit);
    inferencer.infer_reader(data).unwrap()
}

fn records(data: &str) -> Vec<StringRecord> {
    let mut rdr = csv::Reader::from_reader(data.as_bytes());
    rdr.records().map(|r| r.unwrap()).collect()
}

#[test]
fn basic_types_and_promotion_positions() {
    let data = "\
bool_col,int_col,num_col,mix_col,code_col,empty_col
true,0,1,true,001,
false,-7,2,0,002,
true,42,1.20,,003,
,0,4,,004,
";
    let report = infer(data.as_bytes());
    assert!(report.is_complete());
    assert_eq!(report.records_scanned(), 4);
    let cols = report.columns();
    assert_eq!(cols.len(), 6);

    // true/false plus one empty field: stays Boolean.
    assert_eq!(cols[0].ty(), Type::Boolean);
    assert_eq!(cols[0].empty_count(), 1);
    assert_eq!(cols[0].first_promotion(), None);

    // 0, -7, 42, 0: signed 64-bit integers.
    assert_eq!(cols[1].ty(), Type::Integer);
    assert_eq!(cols[1].first_promotion(), None);

    // 1, 2, 1.20, 4: Integer promoted to Decimal by record 2.
    assert_eq!(cols[2].ty(), Type::Decimal);
    let p = cols[2].first_promotion().unwrap();
    assert_eq!(p.record(), 2);
    assert_eq!(p.from(), Type::Integer);
    assert_eq!(p.to(), Type::Decimal);
    assert_eq!(p.position().line(), 4);

    // true, 0: Boolean mixed with a number promotes to Text at record 1.
    assert_eq!(cols[3].ty(), Type::Text);
    let p = cols[3].first_promotion().unwrap();
    assert_eq!(p.record(), 1);
    assert_eq!(p.from(), Type::Boolean);
    assert_eq!(p.to(), Type::Text);
    assert_eq!(p.position().line(), 3);

    // 001, 002, ...: leading zeroes are kept as Text from the start, so
    // no promotion is ever recorded.
    assert_eq!(cols[4].ty(), Type::Text);
    assert_eq!(cols[4].first_promotion(), None);

    // Only empty fields: stays Empty, and empties are counted.
    assert_eq!(cols[5].ty(), Type::Empty);
    assert_eq!(cols[5].empty_count(), 4);

    // The promotion positions must point at the responsible fields in
    // the original records.
    let rows = records(data);
    let p = cols[2].first_promotion().unwrap();
    assert_eq!(&rows[p.record() as usize][2], "1.20");
    let p = cols[3].first_promotion().unwrap();
    assert_eq!(&rows[p.record() as usize][3], "0");
}

#[test]
fn scientific_notation_and_invalid_exponent() {
    let data = "sci,bad\n1e3,1\n2E+4,1e\n-3e-2,3\n";
    let report = infer(data.as_bytes());
    let cols = report.columns();

    // 1e3, 2E+4, -3e-2: valid exponent syntax is an exact decimal.
    assert_eq!(cols[0].ty(), Type::Decimal);
    assert_eq!(cols[0].first_promotion(), None);

    // 1, then invalid 1e: promotes Integer to Text at record 1.
    assert_eq!(cols[1].ty(), Type::Text);
    let p = cols[1].first_promotion().unwrap();
    assert_eq!(p.record(), 1);
    assert_eq!(p.from(), Type::Integer);
    assert_eq!(p.to(), Type::Text);
    let rows = records(data);
    assert_eq!(&rows[p.record() as usize][1], "1e");
}

#[test]
fn negative_and_huge_integers() {
    let data = "\
n,m,d
-42,-9223372036854775808,-1.5
9223372036854775807,-9223372036854775809,0.0
-0,1,2
9223372036854775808,3,4
";
    let report = infer(data.as_bytes());
    let cols = report.columns();

    // i64::MIN, i64::MAX and -0 are integers; 2^63 overflows to Decimal.
    assert_eq!(cols[0].ty(), Type::Decimal);
    let p = cols[0].first_promotion().unwrap();
    assert_eq!(p.record(), 3);
    assert_eq!(p.from(), Type::Integer);
    assert_eq!(p.to(), Type::Decimal);
    let rows = records(data);
    assert_eq!(&rows[p.record() as usize][0], "9223372036854775808");

    // i64::MIN is an integer; -(2^63) - 1 overflows to Decimal.
    assert_eq!(cols[1].ty(), Type::Decimal);
    let p = cols[1].first_promotion().unwrap();
    assert_eq!(p.record(), 1);
    assert_eq!(&rows[p.record() as usize][1], "-9223372036854775809");

    // Negative decimals stay Decimal.
    assert_eq!(cols[2].ty(), Type::Decimal);
    assert_eq!(cols[2].first_promotion(), None);
}

#[test]
fn non_utf8_field_promotes_to_text() {
    let mut data = b"col\n1\n".to_vec();
    data.push(0xFF);
    data.extend_from_slice(b"\n2\n");
    let report = infer(&data);
    let cols = report.columns();
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0].ty(), Type::Text);
    let p = cols[0].first_promotion().unwrap();
    assert_eq!(p.record(), 1);
    assert_eq!(p.from(), Type::Integer);
    assert_eq!(p.to(), Type::Text);
}

#[test]
fn duplicate_headers_are_preserved() {
    let data = "a,a,a\n1,2,x\n3,4,y\n";
    let report = infer(data.as_bytes());
    let headers: Vec<&[u8]> = report.headers().iter().collect();
    assert_eq!(headers, vec![b"a", b"a", b"a"]);
    let cols = report.columns();
    assert_eq!(cols.len(), 3);
    assert_eq!(cols[0].ty(), Type::Integer);
    assert_eq!(cols[1].ty(), Type::Integer);
    assert_eq!(cols[2].ty(), Type::Text);
}

#[test]
fn multiline_fields() {
    let data =
        "name,num,quoted_empty\n\"hello\nworld\",1,\"\"\nplain,2,\"\"\n";
    let report = infer(data.as_bytes());
    assert!(report.is_complete());
    assert_eq!(report.records_scanned(), 2);
    let cols = report.columns();
    assert_eq!(cols[0].ty(), Type::Text);
    assert_eq!(cols[1].ty(), Type::Integer);
    assert_eq!(cols[2].ty(), Type::Empty);
    assert_eq!(cols[2].empty_count(), 2);
}

#[test]
fn whitespace_is_not_trimmed() {
    let data = "a,b,c,d\n 1,1 ,true ,0\n";
    let report = infer(data.as_bytes());
    let cols = report.columns();
    assert_eq!(cols[0].ty(), Type::Text);
    assert_eq!(cols[1].ty(), Type::Text);
    assert_eq!(cols[2].ty(), Type::Text);
    assert_eq!(cols[3].ty(), Type::Integer);
}

#[test]
fn sample_limit_marks_report_partial() {
    let data = "n\n1\n2\n3\n4\n5\n";

    let report = infer_with_limit(data.as_bytes(), Some(2));
    assert!(!report.is_complete());
    assert_eq!(report.records_scanned(), 2);
    assert_eq!(report.columns()[0].ty(), Type::Integer);

    // A limit of zero scans nothing and is still partial.
    let report = infer_with_limit(data.as_bytes(), Some(0));
    assert!(!report.is_complete());
    assert_eq!(report.records_scanned(), 0);
    assert_eq!(report.columns()[0].ty(), Type::Empty);

    // A limit that exactly covers the input is a complete scan.
    let report = infer_with_limit(data.as_bytes(), Some(5));
    assert!(report.is_complete());
    assert_eq!(report.records_scanned(), 5);

    // No limit scans everything.
    let report = infer(data.as_bytes());
    assert!(report.is_complete());
    assert_eq!(report.records_scanned(), 5);
}

#[test]
fn invalid_csv_is_a_parse_error() {
    let data = "a,b\n1,2,3\n";
    let err = Inferencer::new().infer_reader(data.as_bytes()).unwrap_err();
    match err.kind() {
        ErrorKind::UnequalLengths { .. } => {}
        kind => panic!("expected UnequalLengths, got {:?}", kind),
    }
}

#[test]
fn no_headers_participates_in_inference() {
    let data = "1,2\n3,4\n";
    let mut inferencer = Inferencer::new();
    inferencer.has_headers(false);
    let report = inferencer.infer_reader(data.as_bytes()).unwrap();
    assert_eq!(report.headers().len(), 0);
    assert_eq!(report.records_scanned(), 2);
    assert_eq!(report.columns()[0].ty(), Type::Integer);
    assert_eq!(report.columns()[1].ty(), Type::Integer);
}
