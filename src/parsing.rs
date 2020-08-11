use anyhow::{anyhow, Result};
use std::iter::FromIterator;

pub type StringPair = (String, String);

pub fn parse_key_value_data<T: FromIterator<StringPair>>(data: &str) -> Result<T> {
    parse_key_value_pair_lines(data.lines(), "=")
}

pub fn parse_key_value_pair_lines<'a, T, U>(lines: T, seperator: &str) -> Result<U>
where
    T: Iterator<Item = &'a str>,
    U: FromIterator<StringPair>,
{
    lines.map(|s| parse_key_value_pair_line(s, seperator)).collect::<Result<U>>()
}

fn parse_key_value_pair_line(line: &str, seperator: &str) -> Result<StringPair> {
    let parts: Vec<&str> = line.splitn(2, seperator).collect();
    match parts.len() {
        2 => Ok((parts[0].trim().to_string(), parts[1].trim().to_string())),
        _ => Err(anyhow!("Invalid line in key value pair data.")),
    }
}
