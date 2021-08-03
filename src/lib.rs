use std::io;

fn read_file_header(r: &mut impl io::Read) {
    let mut buf: [u8; 4] = [0, 0, 0, 0];
    r.read(&mut buf).expect("Couldn't read file header");
    assert_eq!(&buf, b"SPAT");

    r.read(&mut buf).expect("Couldn't read file version header");
    assert_eq!(&buf, b"\0\0\0\0");
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
}
