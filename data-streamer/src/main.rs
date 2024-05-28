use std::{
    io::Write,
    net::{TcpListener, TcpStream},
    time::Duration,
};
use tokio::sync::broadcast::{self, Receiver};

mod configuration;
mod weather;

const CHANNEL_CAPACITY: usize = 32;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:9999")?;
    let (tx, _) = broadcast::channel::<weather::WeatherResponse>(CHANNEL_CAPACITY);

    let api_tx = tx.clone();

    tokio::spawn(async move {
        let config =
            configuration::read_configuration().expect("Should be able to read configuration");
        loop {
            match weather::get_weather_data(&config).await {
                Ok(response) => {
                    let _ = api_tx.send(response);
                }
                Err(err) => eprintln!("API call failed: {err:?}"),
            };
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    });

    loop {
        let (socket, _) = listener.accept()?;
        let receiver = tx.subscribe();
        tokio::spawn(handle_client(socket, receiver));
    }
}

async fn handle_client(mut socket: TcpStream, mut rx: Receiver<weather::WeatherResponse>) {
    while let Ok(weather) = rx.recv().await {
        let bytes = bincode::serialize(&weather).expect("Should be able to serialize struct");
        if let Err(e) = socket.write_all(&bytes) {
            eprintln!("Writing to socket failed: {e:?}");
            drop(rx);
            break;
        }
    }
}
