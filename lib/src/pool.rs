use std::{
    cmp,
    io::{self, Read, Write},
    ops,
    ptr,
    sync::atomic::{AtomicUsize, Ordering},
    fs::OpenOptions,
    mem,
};

use memmap2::{MmapMut, MmapOptions};

static BUFFER_COUNT: AtomicUsize = AtomicUsize::new(0);

pub struct Pool {
    pub mmap: MmapMut,
    pub buffer_size: usize,
}

impl Pool {
    pub fn with_capacity(file_path: &str, buffer_size: usize) -> io::Result<Pool> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path)?;

        // Set the file size to the desired buffer size
        file.set_len(buffer_size as u64)?;

        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };

        Ok(Pool { mmap, buffer_size })
    }

    pub fn checkout(&mut self) -> Option<Checkout> {
        let old_buffer_count = BUFFER_COUNT.fetch_add(1, Ordering::SeqCst);
        gauge!("buffer.number", old_buffer_count + 1);
        Some(Checkout { pool: self })
    }
}

impl ops::Deref for Pool {
    type Target = MmapMut;

    fn deref(&self) -> &Self::Target {
        &self.mmap
    }
}

pub struct Checkout<'a> {
    pool: &'a mut Pool,
}

impl Drop for Checkout<'_> {
    fn drop(&mut self) {
        let old_buffer_count = BUFFER_COUNT.fetch_sub(1, Ordering::SeqCst);
        gauge!("buffer.number", old_buffer_count - 1);
    }
}

impl Checkout<'_> {
    pub fn available_data(&self) -> usize {
        self.pool.mmap.len() // Adjust as needed for your logic
    }

    pub fn fill(&mut self, count: usize) -> usize {
        let available_space = self.pool.mmap.len() - self.available_data();
        let cnt = cmp::min(count, available_space);
        // Fill logic here
        cnt
    }

    pub fn data(&self) -> &[u8] {
        &self.pool.mmap
    }

    pub fn space(&mut self) -> &mut [u8] {
        &mut self.pool.mmap
    }
}

impl Write for Checkout<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = cmp::min(self.available_data(), buf.len());
        unsafe {
            ptr::copy_nonoverlapping(buf.as_ptr(), self.pool.mmap.as_mut_ptr(), len);
        }
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Read for Checkout<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = cmp::min(self.available_data(), buf.len());
        unsafe {
            ptr::copy_nonoverlapping(self.pool.mmap.as_ptr(), buf.as_mut_ptr(), len);
        }
        Ok(len)
    }
}
