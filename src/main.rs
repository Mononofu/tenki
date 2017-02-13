#![feature(plugin)]
#![plugin(rocket_codegen)]

extern crate chrono;
extern crate clap;
extern crate cpuprofiler;
extern crate flate2;
extern crate image;
extern crate rocket;
extern crate rocket_contrib;
extern crate threadpool;
extern crate time;

use std::collections;
use std::path;
use std::fs;
use std::io;
use std::io::BufRead;
use std::sync;

use chrono::prelude::*;

use std::f64::consts;

macro_rules! ret_check_eq {
    ($a:expr, $b:expr) => { ret_check_impl!($a, $b, ==) }
}

macro_rules! ret_check_ge {
    ($a:expr, $b:expr) => { ret_check_impl!($a, $b, >=) }
}

macro_rules! ret_check_le {
    ($a:expr, $b:expr) => { ret_check_impl!($a, $b, <=) }
}

macro_rules! ret_check_impl {
    ($a:expr, $b:expr, $op:tt) => (
      if !($a $op $b) {
        return Err(io::Error::new(io::ErrorKind::InvalidData,
                   format!("check {} {} {}; failed for {} {} {}",
                    stringify!($a), stringify!($op), stringify!($b),
                     $a, stringify!($op), $b)));
      }
    )
}

macro_rules! check_eq {
    ($a:expr, $b:expr) => { check_impl!($a, $b, ==) }
}

macro_rules! check_ge {
    ($a:expr, $b:expr) => { check_impl!($a, $b, >=) }
}

macro_rules! check_le {
    ($a:expr, $b:expr) => { check_impl!($a, $b, <=) }
}

macro_rules! check_lt {
    ($a:expr, $b:expr) => { check_impl!($a, $b, <) }
}

macro_rules! check_impl {
    ($a:expr, $b:expr, $op:tt) => (
      if !($a $op $b) {
        panic!("check {} {} {}; failed for {} {} {}",
               stringify!($a), stringify!($op), stringify!($b),
               $a, stringify!($op), $b);
      }
    )
}

macro_rules! ret_check_approx_eq {
    ($a:expr, $b:expr, $t:expr) => (
      if ($a - $b).abs() > $t {
        return Err(io::Error::new(io::ErrorKind::InvalidData,
                   format!("check {} ~ {}; failed for {} ~ {} (difference: {})",
                    stringify!($a), stringify!($b),
                     $a, $b, ($a - $b).abs())));
      }
    )
}

#[derive(Debug)]
enum WindMeasurement {
  Calm,
  Variable,
  Normal { speed: f32, direction: i32 },
}

#[derive(Debug)]
struct WeatherMeasurement {
  datetime: DateTime<UTC>,

  wind: Option<WindMeasurement>,
  air_temperature: Option<f32>,
  air_pressure: Option<f32>,
}

struct WeatherStation {
  usaf: String,
  wban: String,

  latitude: f32,
  longitude: f32,
  elevation: Option<i32>,

  measurements: Vec<WeatherMeasurement>,
}

fn parse(filename: &str,
         reader: &mut BufRead,
         max_measurements: usize)
         -> Result<WeatherStation, io::Error> {
  let parts = path::Path::new(filename)
    .file_stem()
    .unwrap()
    .to_str()
    .unwrap()
    .split("-")
    .collect::<Vec<_>>();

  let mut station = WeatherStation {
    usaf: String::from(parts[0]),
    wban: String::from(parts[1]),

    latitude: -1000.0,
    longitude: -1000.0,
    elevation: None,

    measurements: vec![],
  };

  let mut missing = collections::HashMap::<&str, i32>::new();
  for maybe_line in reader.lines() {
    let line = maybe_line.unwrap();

    // Data from https://www1.ncdc.noaa.gov/pub/data/noaa/
    // File format documentation:
    // https://www1.ncdc.noaa.gov/pub/data/noaa/ish-format-document.pdf

    // Some sanity checking.
    let usaf = &line[4..10];
    ret_check_eq!(station.usaf, usaf);

    let wban = &line[10..15];
    ret_check_eq!(station.wban, wban);

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
    let latitude = line[28..34].parse::<f32>().unwrap() / 1000.0;
    ret_check_ge!(latitude, -90.0);
    ret_check_le!(latitude, 90.0);
    if station.measurements.is_empty() {
      station.latitude = latitude;
    }

    let longitude = line[34..41].parse::<f32>().unwrap() / 1000.0;
    ret_check_ge!(longitude, -180.0);
    ret_check_le!(longitude, 180.0);
    if station.measurements.is_empty() {
      station.longitude = longitude;
    }

    let elevation = line[46..51].parse::<i32>().unwrap();
    if elevation >= -400 && elevation <= 9000 {
      if station.elevation.is_none() {
        station.elevation = Some(elevation);
      }
    } else {
      *missing.entry("elevation").or_insert(0) += 1;
    }

    let wind_direction = line[60..63].parse::<i32>().unwrap();
    let wind_speed = line[65..69].parse::<i32>().unwrap();
    let wind_type = &line[64..65];

    let wind_observation =
      if wind_direction >= 0 && wind_direction <= 360 && wind_speed >= 0 &&
         wind_speed <= 900 {
        Some(WindMeasurement::Normal {
          speed: wind_speed as f32 / 10.0,
          direction: wind_direction,
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
      Some(temp as f32 / 10.0)
    } else {
      *missing.entry("air_temperature").or_insert(0) += 1;
      None
    };


    let air_pressure = line[99..104].parse::<i32>().unwrap();
    let maybe_air_pressure = if air_pressure >= 0 && air_pressure <= 20000 {
      Some(air_pressure as f32 / 10.0)
    } else {
      *missing.entry("air_pressure").or_insert(0) += 1;
      None
    };

    if wind_observation.is_none() && maybe_air_temperature.is_none() &&
       maybe_air_pressure.is_none() {
      continue;
    }

    station.measurements.push(WeatherMeasurement {
      datetime: datetime,
      wind: wind_observation,
      air_temperature: maybe_air_temperature,
      air_pressure: maybe_air_pressure,
    });

    if station.measurements.len() > max_measurements {
      break;
    }
  }

  // if !missing.is_empty() {
  //   for (key, count) in &missing {
  //     let missing_perc = (*count as f32) /
  // (station.measurements.len() as f32) *
  //                        100.0;
  //     if missing_perc > 1.0 {
  //       println!("missing {}: {} ({} %)", key, count, missing_perc);
  //     }
  //   }
  // }

  return Ok(station);
}

fn parse_file(filename: &str,
              max_measurements: usize)
              -> Result<WeatherStation, io::Error> {
  let f = fs::File::open(filename).unwrap();
  let mut reader = io::BufReader::new(f);

  match if filename.ends_with(".gz") {
    parse(filename,
          &mut io::BufReader::new(flate2::bufread::GzDecoder::new(reader)
            .unwrap()),
          max_measurements)
  } else {
    parse(filename, &mut reader, max_measurements)
  } {
    Ok(measurements) => Ok(measurements),
    Err(error) => {
      return Err(io::Error::new(io::ErrorKind::InvalidData,
                                format!("parsing {} failed: {}",
                                        filename,
                                        error)))
    }
  }
}

// Applies the web-mercator projection to a latitude in degrees.
// Following https://en.wikipedia.org/wiki/Web_Mercator#Formulas
fn mercator(latitude: f32) -> f32 {
  ((consts::PI as f32 / 4.0) + (latitude.to_radians() / 2.0)).tan().ln()
}

fn draw_stations(stations: &Vec<WeatherStation>,
                 longitude_min: f32,
                 longitude_max: f32,
                 latitude_min: f32,
                 latitude_max: f32,
                 width: u32,
                 height: u32,
                 dot_radius: u32,
                 start_time: DateTime<UTC>,
                 end_time: DateTime<UTC>)
                 -> image::RgbImage {
  println!("requesting stations for longitude {} to {}, latitude {} to {}",
           longitude_min,
           longitude_max,
           latitude_min,
           latitude_max);

  let mut img = image::ImageBuffer::new(width, height);

  for station in stations {
    if station.longitude < longitude_min || station.longitude > longitude_max ||
       station.latitude < latitude_min ||
       station.latitude > latitude_max {
      continue;
    }

    let x = ((station.longitude - longitude_min) /
             (longitude_max - longitude_min) *
             (width - 1) as f32) as i32;
    check_ge!(x, 0);
    check_lt!(x, width as i32);

    let y = ((1.0 -
              (mercator(station.latitude) - mercator(latitude_min)) /
              (mercator(latitude_max) - mercator(latitude_min))) *
             (height - 1) as f32) as i32;
    check_ge!(y, 0);
    check_lt!(y, height as i32);

    let pixel = if station.measurements.is_empty() {
      image::Rgb([0u8, 0u8, 0u8])
    } else {
      let start = match station.measurements
        .binary_search_by(|m| m.datetime.cmp(&start_time)) {
        Ok(index) => index,
        Err(index) => index,
      };
      let (_, after) = station.measurements.split_at(start);

      let end = match after.binary_search_by(|m| m.datetime.cmp(&end_time)) {
        Ok(index) => index,
        Err(index) => index,
      };
      let (measurements, _) = after.split_at(end);


      match measurements.iter()
        .filter(|m| m.air_temperature.is_some())
        .next() {
        Some(m) => {
          let t = m.air_temperature.unwrap();
          let t_min = -30.0f32;
          let t_max = 40.0f32;
          let scaled = (t_max.min(t_min.max(t)) - t_min) / (t_max - t_min);
          image::Rgb([(255.0 * scaled) as u8,
                      127u8,
                      (255.0 * (1.0 - scaled)) as u8])
        }
        None => image::Rgb([0u8, 0u8, 0u8]),
      }
    };

    for dx in 0..dot_radius {
      for dy in 0..dot_radius {
        let (px, py) = (x + (dx as i32 - dot_radius as i32 / 2),
                        y + (dy as i32 - dot_radius as i32 / 2));
        if px >= 0 && px < width as i32 && py >= 0 && py < height as i32 {
          img.put_pixel(px as u32, py as u32, pixel);
        }
      }
    }
  }

  return img;
}

fn draw_stations_to_file(stations: &Vec<WeatherStation>,
                         start_time: DateTime<UTC>,
                         end_time: DateTime<UTC>,
                         image_path: &path::Path) {
  let img = draw_stations(stations,
                          -180.0,
                          180.0,
                          -90.0,
                          90.0,
                          1024,
                          512,
                          1,
                          start_time,
                          end_time);
  let _ = img.save(image_path);
}

fn coordinates_to_degrees(zoom: u32, x: u32, y: u32) -> (f32, f32) {
  let n = 2f32.powi(zoom as i32);
  let longitude = (x as f32) / n * 360.0 - 180.0;
  let latitude = (consts::PI as f32 * (1.0 - 2.0 * (y as f32) / n))
    .sinh()
    .atan()
    .to_degrees();
  (longitude, latitude)
}

#[get("/")]
fn index() -> rocket_contrib::Template {
  let context = collections::HashMap::<&str, &str>::new();
  rocket_contrib::Template::render("index", &context)
}

#[get("/static/<filename>")]
fn static_file(filename: &str)
               -> Result<rocket::response::NamedFile, io::Error> {
  rocket::response::NamedFile::open(path::Path::new("static").join(filename))
}

#[get("/api/map/<zoom>/<x>/<y>/tile.png")]
fn map_tile<'a>(zoom: u32,
                x: u32,
                y: u32,
                stations: rocket::State<Vec<WeatherStation>>)
                -> Result<rocket::Response<'a>, io::Error> {
  let (long_min, lat_top) = coordinates_to_degrees(zoom, x, y);
  let (long_max, lat_bot) = coordinates_to_degrees(zoom, x + 1, y + 1);

  let dot_radius = if zoom < 5 { 1 } else { zoom - 3 };

  let size = 256;
  let mut img = draw_stations(stations.inner(),
                              long_min,
                              long_max,
                              lat_bot,
                              lat_top,
                              size,
                              size,
                              dot_radius,
                              UTC.ymd(1900, 1, 1).and_hms(0, 0, 0),
                              UTC.ymd(2100, 1, 1).and_hms(0, 0, 0));

  // Debug borders:
  // for i in 0..size {
  //   img.put_pixel(i, 0, image::Rgb([0u8, 255u8, 0u8]));
  //   img.put_pixel(i, size - 1, image::Rgb([0u8, 255u8, 0u8]));
  //   img.put_pixel(0, i, image::Rgb([0u8, 255u8, 0u8]));
  //   img.put_pixel(size - 1, i, image::Rgb([0u8, 255u8, 0u8]));
  // }
  // img.put_pixel(size / 2, size / 2, image::Rgb([255u8, 0u8, 0u8]));

  let mut buf = Vec::<u8>::new();
  {
    let encoder = image::png::PNGEncoder::new(&mut buf);
    try!(encoder.encode(&img.into_raw(), size, size, image::ColorType::RGB(8)));
  }

  rocket::Response::build().sized_body(io::Cursor::new(buf)).ok()
}

fn main() {
  let args = clap::App::new("parser")
    .arg(clap::Arg::with_name("file").long("file").takes_value(true))
    .arg(clap::Arg::with_name("directory").long("directory").takes_value(true))
    .arg(clap::Arg::with_name("render_dir")
      .long("render_dir")
      .takes_value(true))
    .arg(clap::Arg::with_name("max_stations")
      .long("max_stations")
      .takes_value(true))
    .arg(clap::Arg::with_name("max_measurements")
      .long("max_measurements")
      .takes_value(true))
    .arg(clap::Arg::with_name("threads")
      .long("threads")
      .takes_value(true)
      .default_value("8"))
    .get_matches();

  let max_measurements = args.value_of("max_measurements")
    .and_then(|n| n.parse::<usize>().ok())
    .unwrap_or(usize::max_value());

  let mut stations = Vec::new();

  cpuprofiler::PROFILER.lock().unwrap().start("prof.profile").unwrap();

  args.value_of("directory").map(|directory| {
    let max_stations = args.value_of("max_stations")
      .and_then(|n| n.parse::<usize>().ok())
      .unwrap_or(usize::max_value());
    let n_threads = args.value_of("threads").unwrap().parse::<usize>().unwrap();
    let pool = threadpool::ThreadPool::new(n_threads);
    let (tx, rx) = sync::mpsc::channel();

    let mut num_files = 0;
    for path in fs::read_dir(directory).unwrap().take(max_stations) {
      let tx = tx.clone();
      pool.execute(move || {
        tx.send(parse_file(path.unwrap().path().to_str().unwrap(),
                           max_measurements))
          .unwrap();
      });
      num_files += 1;
    }

    let start = time::now();
    let mut last_update = time::now();
    let mut num_processed = 0;
    for result in rx.iter().take(num_files) {
      match result {
        Ok(station) => {
          stations.push(station);

          num_processed += 1;
          if time::now() - last_update > time::Duration::seconds(1) {
            last_update = time::now();
            let elapsed = time::now() - start;
            let elapsed_secs = elapsed.num_milliseconds() as f64 / 1.0e3;
            println!("processed {} files in {} - {} files / second",
                     num_processed,
                     elapsed_secs,
                     num_processed as f64 / elapsed_secs);
          }
        }
        Err(error) => {
          println!("{}", error);
        }
      }
    }
  });

  args.value_of("file")
    .map(|f| parse_file(f, max_measurements))
    .map(|result| { stations.push(result.unwrap()); });

  cpuprofiler::PROFILER.lock().unwrap().stop().unwrap();

  args.value_of("render_dir").map(|directory| {
    let start = UTC.ymd(2016, 1, 1).and_hms(0, 0, 0);
    for i in 0..(52) {
      draw_stations_to_file(&stations,
                            start + time::Duration::weeks(i),
                            start + time::Duration::weeks(i + 1),
                            &path::Path::new(directory)
                              .join(format!("weather-{:04}.png", i)));
    }
  });


  rocket::ignite()
    .mount("/", routes![index, static_file, map_tile])
    .manage(stations)
    .launch();
}
