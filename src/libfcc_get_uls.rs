use futures_util::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use std::cmp::min;
use std::fs::File;
use std::io::{Seek, Write};
use std::path::Path;

pub async fn download_ham_db() -> Result<(), Box<dyn std::error::Error>> {
    let target = "https://data.fcc.gov/download/pub/uls/complete/l_amat.zip";
    let path = Path::new("./data/l_amat.zip");
    let client = reqwest::Client::new();
    let res = client
        .get(target)
        .send()
        .await
        .or(Err(format!("Failed to GET from '{}'", &target)))?;
    let total_size = res
        .content_length()
        .ok_or(format!("Failed to get content length from '{}'", &target))?;

    let pb = ProgressBar::new(total_size);
    pb.set_style(ProgressStyle::default_bar()
		 .template("{msg}\n{spinner:.green} [{elapsed_precise}] [{wide_bar:.white/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
		 .expect("Could not create template")
		 .progress_chars("█  "));
    pb.set_message(format!("Downloading {}", target));

    let mut file;
    let mut downloaded: u64 = 0;
    let mut stream = res.bytes_stream();

    println!("Seeking in file.");
    if std::path::Path::new(path).exists() {
        println!("File exists. Resuming.");
        file = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .unwrap();
        let file_size = std::fs::metadata(path).unwrap().len();
        file.seek(std::io::SeekFrom::Start(file_size)).unwrap();
        downloaded = file_size;
    } else {
        println!("Fresh file..");
        file = File::create(path).or(Err(format!("Failed to create file '{}'", path.display())))?;
    }

    println!("Commencing transfer");
    while let Some(item) = stream.next().await {
        let chunk = item.or(Err(format!("Error while downloading file")))?;
        file.write(&chunk)
            .or(Err(format!("Error while writing to file")))?;
        let new = min(downloaded + (chunk.len() as u64), total_size);
        downloaded = new;
        pb.set_position(new);
    }

    pb.finish_with_message(format!("Downloaded {} to {}", target, path.display()));
    return Ok(());

    // Original Code
    /*
    let path = Path::new("./data/l_amat.zip");
    let content = match reqwest::blocking::get(target)?.bytes() {
    Err(why) => panic!("Couldn't download {}", why),
    Ok(resp) => resp,
    };

    match file.write_all(&content) {
    Err(why) => panic!("Couldn't write content {}", why),
    Ok(the_write) => the_write,
    };
    Ok(())
    */
}
