use dotenv::dotenv;
use thiserror::Error;

pub const API_URL: &str = "https://api.openweathermap.org/data/2.5/weather";

pub struct Configuration {
    pub api_key: String,
    pub lat: f32,
    pub lon: f32,
    pub units: String,
}

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Key `{0}` not found in .env")]
    KeyNotFound(String),
    #[error("Key `{0}` is not a float")]
    BadFormat(String),
}

pub fn read_configuration() -> Result<Configuration, ConfigError> {
    dotenv().ok();
    let api_key = std::env::var("OPENWEATHERMAP_API_KEY")
        .map_err(|_| ConfigError::KeyNotFound("OPENWEATHERMAP_API_KEY".to_owned()))?;
    let lat = std::env::var("LAT")
        .map_err(|_| ConfigError::KeyNotFound("LAT".to_owned()))?
        .parse::<f32>()
        .map_err(|_| ConfigError::BadFormat("LAT".to_owned()))?;
    let lon = std::env::var("LON")
        .map_err(|_| ConfigError::KeyNotFound("LON".to_owned()))?
        .parse::<f32>()
        .map_err(|_| ConfigError::BadFormat("LAT".to_owned()))?;
    let units = std::env::var("UNITS").unwrap_or("standard".to_owned());
    Ok(Configuration {
        api_key,
        lat,
        lon,
        units,
    })
}
