use std::fs;

pub fn get_rss_memory() -> u64 {
    if let Ok(statm) = fs::read_to_string("/proc/self/statm") {
        if let Some(rss_str) = statm.split_whitespace().nth(1) {
            if let Ok(rss) = rss_str.parse::<u64>() {
                return rss * 4096 / 1024; // Convertendo páginas para KB
            }
        }
    }
    0
}
