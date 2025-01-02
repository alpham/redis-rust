use crate::internal::server::ServerMetadata;
use crate::Error;

pub fn get_server_info(
    server_metadata: &ServerMetadata,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    let mut response = vec![
        format!("master_replid:{}", server_metadata.master_replid),
        format!("master_repl_offset:{}", server_metadata.master_repl_offset),
    ];
    if server_metadata.role == 0 {
        response.push("role:master".to_string());
    } else if server_metadata.role == 1 {
        response.push("role:slave".to_string());
    }
    let rtn = response.join(" ");
    Ok(format!("${}\r\n{}\r\n", rtn.len(), rtn))
}
