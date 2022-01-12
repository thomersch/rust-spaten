mod fileformat;
use protobuf::Message;
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::io::Cursor;
use wkb::*;

pub enum Value {
    String(String),
    Integer(i64),
    Float(f64),
}

impl Value {
    fn from_bytes(src: Vec<u8>, field_type: fileformat::Tag_ValueType) -> Value {
        match field_type {
            fileformat::Tag_ValueType::STRING => {
                Value::String(String::from_utf8_lossy(&src).to_string())
            }
            fileformat::Tag_ValueType::INT => {
                let mut sf: [u8; 8] = [0; 8];
                sf.copy_from_slice(&src[0..8]);
                Value::Integer(i64::from_le_bytes(sf))
            }
            fileformat::Tag_ValueType::DOUBLE => {
                let mut sf: [u8; 8] = [0; 8];
                sf.copy_from_slice(&src[0..8]);
                Value::Float(f64::from_le_bytes(sf))
            }
        }
    }
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::String(v) => write!(f, "\"{}\"", v),
            Value::Integer(v) => write!(f, "{}", v),
            Value::Float(v) => write!(f, "{}", v),
        }
    }
}

pub struct Feature {
    pub geometry: geo_types::Geometry<f64>,
    pub tags: HashMap<String, Value>,
}

pub struct FeatureIterator<'a> {
    stream: &'a mut dyn io::Read,
    queue: Vec<Feature>,
}

impl FeatureIterator<'_> {
    /// Initializes a streaming reader that can be used to iterate over the features.
    /// ```
    /// use spaten::FeatureIterator;
    /// use std::fs::File;
    ///
    /// let mut file = File::open("nrw-motorway.spaten").unwrap();
    /// for ft in FeatureIterator::new(&mut file) {
    ///     println!("{:?}", ft.tags)
    /// }
    /// ```
    pub fn new(r: &mut impl io::Read) -> FeatureIterator {
        read_file_header(r);
        FeatureIterator {
            stream: r,
            queue: Vec::new(),
        }
    }
}

impl Iterator for FeatureIterator<'_> {
    type Item = Feature;

    fn next(&mut self) -> Option<Self::Item> {
        if self.queue.len() == 0 {
            match read_block(&mut self.stream) {
                Ok(x) => match x {
                    Some(s) => self.queue = read_body(s),
                    None => return None,
                },
                Err(e) => panic!("iterating failed: {:?}", e),
            }
        }
        Some(self.queue.remove(0))
    }
}

pub fn read_file_header(r: &mut impl io::Read) {
    let mut buf: [u8; 4] = [0, 0, 0, 0];
    r.read(&mut buf).expect("Couldn't read file header");
    assert_eq!(&buf, b"SPAT");

    r.read(&mut buf).expect("Couldn't read file version header");
    assert_eq!(&buf, b"\0\0\0\0");
}

pub fn read_block(r: &mut impl io::Read) -> Result<Option<Vec<u8>>, &'static str> {
    let mut bodylen_b: [u8; 4] = [0; 4];
    if let Err(_) = r.read(&mut bodylen_b) {
        return Err("Couldn't read body length");
    }
    let bodylen = u32::from_le_bytes(bodylen_b);

    if bodylen == 0 {
        return Ok(None);
    }

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

    return Ok(Some(body));
}

pub fn read_body(v: Vec<u8>) -> Vec<Feature> {
    let body = fileformat::Body::parse_from_bytes(&v).unwrap();
    let mut features = Vec::with_capacity(body.feature.len() as usize);

    for ft in body.feature {
        let mut bytes_cur = Cursor::new(ft.geom);
        let g = bytes_cur.read_wkb().unwrap();

        let mut tags = HashMap::with_capacity(ft.tags.len());
        for tag in ft.tags {
            tags.insert(tag.key, Value::from_bytes(tag.value, tag.field_type));
        }

        let ft = Feature { geometry: g, tags };
        features.push(ft);
    }
    features
}

#[cfg(test)]
mod tests {
    use crate::FeatureIterator;

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
        use crate::read_body;
        use crate::read_file_header;
        use std::fs::File;

        let mut file = File::open("nrw-motorway.spaten").unwrap();
        read_file_header(&mut file);

        loop {
            match read_block(&mut file) {
                Ok(x) => {
                    match x {
                        Some(block) => {
                            println!("block");
                            let fts = read_body(block);
                            for _ft in fts {
                                // println!("{:?}", ft.tags);
                            }
                        }
                        None => {
                            println!("end");
                            return;
                        }
                    }
                }
                Err(err) => {
                    panic!("error while reading: {:?}", err)
                }
            }
        }
    }

    #[test]
    fn stream_iterator() {
        use std::fs::File;

        let mut file = File::open("nrw-motorway.spaten").unwrap();
        for ft in FeatureIterator::new(&mut file) {
            println!("{:?}", ft.tags)
        }
    }
}
