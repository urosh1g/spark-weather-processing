mod configuration;
mod weather;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = configuration::read_configuration()?;
    let weather_data = weather::get_weather_data(&config).await?;
    println!("{weather_data:?}");
    Ok(())
}
