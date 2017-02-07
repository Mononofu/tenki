extern crate chrono;
extern crate clap;
extern crate flate2;
extern crate num;

use std::collections;
use std::path;
use std::fs;
use std::io;
use std::io::BufRead;

use chrono::prelude::*;

macro_rules! ret_check_ge {
    ($a:expr, $b:expr) => (
      if !($a >= $b) {
        return Err(io::Error::new(io::ErrorKind::InvalidData,
                   format!("ret_check_ge({}, {}) failed for {} >= {}",
                    stringify!($a), stringify!($b), $a, $b)));
      }
    )
}

macro_rules! ret_check_le {
    ($a:expr, $b:expr) => (
      if !($a <= $b) {
        return Err(io::Error::new(io::ErrorKind::InvalidData,
                   format!("ret_check_le({}, {}) failed for {} <= {}",
                    stringify!($a), stringify!($b), $a, $b)));
      }
    )
}

#[derive(Debug)]
enum WindMeasurement {
  Calm,
  Variable,
  Normal{speed: num::rational::Ratio<i32>, direction: i32},
}

#[derive(Debug)]
struct WeatherMeasurement {
  datetime: DateTime<UTC>,
  latitude: num::rational::Ratio<i32>,
  longitude: num::rational::Ratio<i32>,
  elevation: Option<i32>,
  wind: Option<WindMeasurement>,
  air_temperature: Option<num::rational::Ratio<i32>>,
  air_pressure: Option<num::rational::Ratio<i32>>,
}

fn parse(filename: &str, reader: &mut BufRead) ->
    Result<Vec<WeatherMeasurement>, io::Error> {
  let parts = path::Path::new(filename).file_stem().unwrap().to_str().unwrap()
                                       .split("-").collect::<Vec<_>>();

  let mut measurements = Vec::new();
  let mut missing = collections::HashMap::<&str, i32>::new();
  for maybe_line in reader.lines() {
    let line = maybe_line.unwrap();

    // Data from https://www1.ncdc.noaa.gov/pub/data/noaa/
    // File format documentation:
    // https://www1.ncdc.noaa.gov/pub/data/noaa/ish-format-document.pdf

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

    // Date and time.
    let date = &line[15..23];
    let year = date[0..4].parse::<i32>().unwrap();
    let month = date[4..6].parse::<u32>().unwrap();
    let day = date[6..8].parse::<u32>().unwrap();

    let utc_day = UTC.ymd(year, month, day);

    let time = &line[23..27];
    let hour = time[0..2].parse::<u32>().unwrap();
    let minute = time[2..4].parse::<u32>().unwrap();

    let datetime = utc_day.and_hms(hour, minute, 0);

    // Location.
    let latitude = line[28..34].parse::<i32>().unwrap();
    if latitude < -90000 || latitude > 90000 {
      return Err(io::Error::new(io::ErrorKind::InvalidData,
                 format!("latitude out of range: {}", latitude)));
    }

    let longitude = line[34..41].parse::<i32>().unwrap();
    if longitude < -180000 || longitude > 180000 {
      return Err(io::Error::new(io::ErrorKind::InvalidData,
                 format!("longitude out of range: {}", longitude)));
    }


    let elevation = line[46..51].parse::<i32>().unwrap();
    let maybe_elevation = if elevation >= -400 && elevation <= 9000 {
      Some(elevation)
    } else {
      *missing.entry("elevation").or_insert(0) += 1;
      None
    };

    let wind_direction = line[60..63].parse::<i32>().unwrap();
    let wind_speed = line[65..69].parse::<i32>().unwrap();
    let wind_type = &line[64..65];

    let wind_observation = if wind_direction >= 0 && wind_direction <= 360 &&
       wind_speed >= 0 && wind_speed <= 900 {
      Some(WindMeasurement::Normal{
        speed: num::rational::Ratio::<i32>::new(wind_speed, 10),
        direction: wind_direction
      })
    } else if wind_type == "C" || (wind_type == "9" && wind_speed == 0) {
      Some(WindMeasurement::Calm)
    } else if wind_type == "V" {
      Some(WindMeasurement::Variable)
    } else {
      *missing.entry("wind").or_insert(0) += 1;
      None
    };


    let temp = line[87..92].parse::<i32>().unwrap();
    let maybe_air_temperature = if temp >= -1000 && temp <= 1000 {
      Some(num::rational::Ratio::<i32>::new(temp, 10))
    } else {
      *missing.entry("air_temperature").or_insert(0) += 1;
      None
    };


    let air_pressure = line[99..104].parse::<i32>().unwrap();
    let maybe_air_pressure = if air_pressure >= 0 && air_pressure <= 20000 {
      Some(num::rational::Ratio::<i32>::new(air_pressure, 10))
    } else {
      *missing.entry("air_pressure").or_insert(0) += 1;
      None
    };

    let measurement = WeatherMeasurement{
      datetime: datetime,
      latitude: num::rational::Ratio::<i32>::new(latitude, 1000),
      longitude: num::rational::Ratio::<i32>::new(longitude, 1000),
      elevation: maybe_elevation,
      wind: wind_observation,
      air_temperature: maybe_air_temperature,
      air_pressure: maybe_air_pressure,
    };
    // println!("{:?}", measurement);
    measurements.push(measurement);
  }

  if !missing.is_empty() {
    for (key, count) in &missing {
      println!("missing {}: {} ({} %)", key, count,
              (*count as f32) / (measurements.len() as f32));
    }
  }

  return Ok(measurements);
}

fn parse_file(filename: &str) {
  println!("parsing {:?}", filename);

  let f = fs::File::open(filename).unwrap();
  let mut reader = io::BufReader::new(f);

  match if filename.ends_with(".gz") {
    parse(filename, &mut io::BufReader::new(
      flate2::bufread::GzDecoder::new(reader).unwrap()))
  } else {
    parse(filename, &mut reader)
  } {
    Ok(measurements) => {},
    Err(error) => {
      panic!("parsing {:?} failed: {}", filename, error);
    }
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
