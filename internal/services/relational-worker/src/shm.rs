use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::arrow::record_batch::RecordBatch;
use std::fs::File;
use std::io::{Cursor, Write};

pub struct ShmManager {
    base_path: String,
}

impl ShmManager {
    pub fn new(base_path: &str) -> Self {
        Self {
            base_path: base_path.to_string(),
        }
    }

    pub fn get(&self, id: &str) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let path = format!("{}/{}", self.base_path, id);
        let file = File::open(path)?;
        let mmap = unsafe { memmap2::Mmap::map(&file)? };

        let reader = FileReader::try_new(Cursor::new(&mmap[..]), None)?;

        let mut batches = Vec::new();
        for batch in reader {
            batches.push(batch?);
        }

        if batches.is_empty() {
            return Err("no batches found in shm".into());
        }

        Ok(batches[0].clone())
    }

    pub fn put(&self, id: &str, batch: &RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
        let path = format!("{}/{}", self.base_path, id);

        let mut buf = Vec::new();
        {
            let mut writer = FileWriter::try_new(&mut buf, &batch.schema())?;
            writer.write(batch)?;
            writer.finish()?;
        }

        let mut file = File::create(path)?;
        file.set_len(buf.len() as u64)?;
        file.write_all(&buf)?;

        Ok(())
    }
}
