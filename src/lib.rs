use std::io;

fn read_file_header(r: &mut impl io::Read) {
    let mut buf: [u8; 4] = [0, 0, 0, 0];
    r.read(&mut buf).expect("Couldn't read file header");
    assert_eq!(&buf, b"SPAT");

    r.read(&mut buf).expect("Couldn't read file version header");
    assert_eq!(&buf, b"\0\0\0\0");
}

fn read_block(r: &mut impl io::Read) -> Option<Vec<u8>> {
    let mut bodylen_b: [u8; 4] = [0; 4];
    r.read(&mut bodylen_b).expect("Couldn't read body length");
    let bodylen = u32::from_le_bytes(bodylen_b);

    if bodylen == 0 {
        return None;
    }

    println!("{:?}", bodylen);

    let mut flags_b: [u8; 2] = [0; 2];
    r.read(&mut flags_b).expect("Couldn't read flags");
    assert_eq!(&flags_b, b"\0\0");

    let mut compression_b: [u8; 1] = [0; 1];
    r.read(&mut compression_b)
        .expect("Couldn't get compression flags");
    assert_eq!(&compression_b, b"\0");

    let mut messagetype_b: [u8; 1] = [0; 1];
    r.read(&mut messagetype_b)
        .expect("Couldn't get message type");
    assert_eq!(&messagetype_b, b"\0");

    let mut body = vec![0; bodylen as usize];
    r.read(&mut body).expect("Body reading failed");

    return Some(body);
}

#[cfg(test)]
mod tests {
    #[test]
    fn file_header_test() {
        use crate::read_file_header;
        use std::io::Cursor;

        let mut file = Cursor::new(b"SPAT\0\0\0\0");
        read_file_header(&mut file);
    }

    #[test]
    fn file_read_test() {
        use crate::read_block;
        use crate::read_file_header;
        use std::fs::File;

        let mut file = File::open("nrw-motorway.spaten");

        let mut file = match file {
            Ok(f) => f,
            Err(error) => panic!("Couldn't open file: {:?}", error),
        };
        read_file_header(&mut file);

        loop {
            match read_block(&mut file) {
                Some(x) => println!("block"),
                None => {
                    println!("end");
                    return;
                }
            }
        }
    }
}
