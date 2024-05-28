use crate::configuration::{Configuration, API_URL};
use serde::{Deserialize, Serialize};

pub async fn get_weather_data(
    config: &Configuration,
) -> Result<WeatherResponse, Box<dyn std::error::Error>> {
    let url = format!(
        "{API_URL}?lat={lat}&lon={lon}&appid={api_key}&units={units}",
        lat = config.lat,
        lon = config.lon,
        api_key = config.api_key,
        units = config.units
    );
    let response = reqwest::Client::new().get(url).send().await?;
    let response = response.json::<WeatherResponse>().await?;
    Ok(response)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WeatherResponse {
    coord: Coord,
    weather: Vec<Weather>,
    base: String,
    main: Main,
    visibility: i32,
    wind: Wind,
    rain: Option<Rain>,
    clouds: Clouds,
    dt: i32,
    sys: Sys,
    timezone: i32,
    id: i32,
    name: String,
    cod: i32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Coord {
    lon: f32,
    lat: f32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Weather {
    id: i32,
    main: String,
    description: String,
    icon: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Wind {
    speed: f32,
    deg: i32,
    gust: Option<f32>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Main {
    temp: f32,
    feels_like: f32,
    temp_min: f32,
    temp_max: f32,
    pressure: i32,
    humidity: i32,
    sea_level: Option<i32>,
    grnd_level: Option<i32>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Rain {
    #[serde(rename = "1h")]
    last_hour: f32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Clouds {
    all: i32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Sys {
    #[serde(rename = "type")]
    ty: Option<i32>,
    id: Option<i32>,
    country: String,
    sunrise: i32,
    sunset: i32,
}
