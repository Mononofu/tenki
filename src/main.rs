extern crate chrono;
extern crate clap;
extern crate flate2;

use std::path;
use std::fs;
use std::io;
use std::io::BufRead;

use chrono::prelude::*;

#[derive(Debug)]
struct WeatherMeasurement {
  datetime: DateTime<UTC>,
}

fn parse(filename: &str, reader: &mut BufRead) -> Result<Vec<WeatherMeasurement>, io::Error> {
  let parts = path::Path::new(filename).file_stem().unwrap().to_str().unwrap()
                                       .split("-").collect::<Vec<_>>();

  let mut measurements = Vec::new();
  for maybe_line in reader.lines() {
    let line = maybe_line.unwrap();

    // Some sanity checking.
    let usaf = &line[4..10];
    if parts[0] != usaf {
      return Err(io::Error::new(io::ErrorKind::InvalidData,
        format!("unexpected usaf code: {} <> {}", parts[0], usaf)));
    }

    let wban = &line[10..15];
    if parts[1] != wban {
      return Err(io::Error::new(io::ErrorKind::InvalidData,
        format!("unexpected wban code: {} <> {}", parts[1], wban)));
    }

    let date = &line[15..23];
    let year = date[0..4].parse::<i32>().unwrap();
    let month = date[4..6].parse::<u32>().unwrap();
    let day = date[6..8].parse::<u32>().unwrap();

    let utc_day = UTC.ymd(year, month, day);

    let time = &line[23..27];
    let hour = time[0..2].parse::<u32>().unwrap();
    let minute = time[2..4].parse::<u32>().unwrap();

    let datetime = utc_day.and_hms(hour, minute, 0);


    let measurement = WeatherMeasurement{
      datetime: datetime,
    };
    println!("{:?}", measurement);
    measurements.push(measurement);

  }

  return Ok(measurements);
}

fn parse_file(filename: &str) {
  println!("parsing {:?}", filename);

  let f = fs::File::open(filename).unwrap();
  let mut reader = io::BufReader::new(f);

  if filename.ends_with(".gz") {
    println!("unzip");
    parse(filename, &mut io::BufReader::new(
      flate2::bufread::GzDecoder::new(reader).unwrap())).unwrap();
  } else {
    parse(filename, &mut reader).unwrap();
  }
}

fn main() {
  let args = clap::App::new("parser")
    .arg(clap::Arg::with_name("file").long("file").takes_value(true))
    .arg(clap::Arg::with_name("directory").long("directory").takes_value(true))
    .get_matches();

  args.value_of("directory").map(|directory| {
    for path in fs::read_dir(directory).unwrap() {
      parse_file(path.unwrap().path().to_str().unwrap());
    }
  });

  args.value_of("file").map(parse_file);
}
