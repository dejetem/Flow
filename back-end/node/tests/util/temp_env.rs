use std::env;

pub struct TempEnv {
    keys: Vec<String>,
}

impl TempEnv {
    pub fn new() -> Self {
        Self { keys: Vec::new() }
    }

    pub fn set(&mut self, key: &str, value: &str) {
        unsafe {
            env::set_var(key, value);
        }
        self.keys.push(key.to_string());
    }

    pub fn remove(&mut self, key: &str) {
        unsafe {
            env::remove_var(key);
        }
        self.keys.push(key.to_string());
    }
}

impl Drop for TempEnv {
    fn drop(&mut self) {
        // Clean up all set variables
        for key in &self.keys {
            unsafe {
                env::remove_var(key);
            }
        }
    }
}
