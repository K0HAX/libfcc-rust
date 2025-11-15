use std::fs;
use std::io;

pub fn unzip_uls() {
    let src_path = "data/l_amat.zip";
    let fname = std::path::Path::new(&src_path);
    let file = fs::File::open(&fname).unwrap();

    let mut archive = zip::ZipArchive::new(file).unwrap();

    for i in 0..archive.len() {
        let mut file = archive.by_index(i).unwrap();
        let outdir = "data/";
        let outpath = match file.enclosed_name() {
            Some(path) => path.to_owned(),
            None => continue,
        };
        let real_outpath = format!(
            "{}{}",
            outdir,
            outpath.into_os_string().into_string().unwrap()
        );
        let real_outpath_obj = std::path::Path::new(&real_outpath);

        {
            let comment = file.comment();
            if !comment.is_empty() {
                println!("File {} comment: {}", i, comment);
            }
        }

        if (*file.name()).ends_with('/') {
            println!("File {} extracted to \"{}\"", i, real_outpath);
            fs::create_dir_all(&real_outpath_obj).unwrap();
        } else {
            println!(
                "File {} extracted to \"{}\" ({} bytes)",
                i,
                real_outpath,
                file.size()
            );
            if let Some(p) = real_outpath_obj.parent() {
                if !p.exists() {
                    fs::create_dir_all(&p).unwrap();
                }
            }
            let mut outfile = fs::File::create(&real_outpath_obj).unwrap();
            io::copy(&mut file, &mut outfile).unwrap();
        }

        // Get and Set permissions
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            if let Some(mode) = file.unix_mode() {
                fs::set_permissions(&real_outpath_obj, fs::Permissions::from_mode(mode)).unwrap();
            }
        }
    }
}
